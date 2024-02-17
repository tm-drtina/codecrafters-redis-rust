use std::collections::VecDeque;
use std::future::Future;
use std::io::ErrorKind;
use std::pin::Pin;

use anyhow::{bail, Context};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt, BufReader};
use tokio::net::tcp::ReadHalf;

use crate::resp::RespType;

pub struct RespReader<'stream> {
    reader: BufReader<ReadHalf<'stream>>,
    buf: Vec<u8>,
}

impl<'stream> RespReader<'stream> {
    pub(crate) fn new(reader: ReadHalf<'stream>) -> Self {
        Self {
            reader: BufReader::new(reader),
            buf: Vec::new(),
        }
    }

    async fn read_until_crlf(&mut self) -> std::io::Result<usize> {
        let mut read = 0;
        loop {
            read += self.reader.read_until(b'\n', &mut self.buf).await?;
            if read > 1
                && self.buf[self.buf.len() - 1] == b'\n'
                && self.buf[self.buf.len() - 2] == b'\r'
            {
                // Remove the ending CRLF
                self.buf.pop();
                self.buf.pop();
                read -= 2;
                break;
            }
        }

        Ok(read)
    }

    async fn read_string(&mut self) -> anyhow::Result<String> {
        self.buf.clear();
        self.read_until_crlf().await?;
        Ok(std::str::from_utf8(&self.buf)
            .context("Non-UTF-8 valid string provided")?
            .to_string())
    }

    async fn read_i64(&mut self) -> anyhow::Result<i64> {
        self.buf.clear();
        self.read_until_crlf().await?;
        Ok(std::str::from_utf8(&self.buf)
            .context("Non-UTF-8 valid string provided")?
            .parse()
            .context("Failed to parse string to int")?)
    }

    async fn read_usize(&mut self) -> anyhow::Result<usize> {
        self.buf.clear();
        self.read_until_crlf().await?;
        Ok(std::str::from_utf8(&self.buf)
            .context("Non-UTF-8 valid string provided")?
            .parse()
            .context("Failed to parse string to int")?)
    }

    pub fn read_item<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<RespType>>> + 'a + Send>> {
        Box::pin(async move {
            match self.reader.read_u8().await {
                Ok(b'+') => Ok(Some(RespType::SimpleString(self.read_string().await?))),
                Ok(b'-') => Ok(Some(RespType::SimpleError(self.read_string().await?))),
                Ok(b':') => Ok(Some(RespType::Integer(self.read_i64().await?))),
                Ok(b'$') => {
                    let len = self.read_usize().await?;
                    let mut buf = Vec::with_capacity(len + 2);
                    buf.resize(len + 2, 0);
                    self.reader.read_exact(&mut buf).await?;
                    assert_eq!(Some(b'\n'), buf.pop());
                    assert_eq!(Some(b'\r'), buf.pop());
                    Ok(Some(RespType::BulkString(buf.into_boxed_slice())))
                }
                Ok(b'*') => {
                    let count = self.read_usize().await?;
                    let mut arr = VecDeque::with_capacity(count);
                    for _ in 0..count {
                        arr.push_back(
                            self.read_item()
                                .await?
                                .ok_or(anyhow::anyhow!("Missing array item"))?,
                        );
                    }
                    Ok(Some(RespType::Array(arr)))
                }
                Ok(b) => bail!("Unrecognized first byte {}", b),
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(None),
                Err(err) => Err(err).context("Error while reading data"),
            }
        })
    }
}

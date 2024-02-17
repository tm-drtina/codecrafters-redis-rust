use std::future::Future;
use std::pin::Pin;

use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::WriteHalf;

use crate::resp::RespType;

pub struct RespWriter<'stream> {
    writer: BufWriter<WriteHalf<'stream>>,
}

impl<'stream> RespWriter<'stream> {
    pub(crate) fn new(writer: WriteHalf<'stream>) -> Self {
        Self {
            writer: BufWriter::new(writer),
        }
    }

    async fn write_crlf(&mut self) -> anyhow::Result<()> {
        self.writer.write_u8(b'\r').await?;
        self.writer.write_u8(b'\n').await?;
        Ok(())
    }

    pub fn write_item<'a>(
        &'a mut self,
        item: RespType,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + 'a + Send>> {
        Box::pin(async move {
            self.writer.write_u8(item.first_byte()).await?;
            match item {
                RespType::SimpleString(s) | RespType::SimpleError(s) => {
                    self.writer.write_all(s.as_bytes()).await?;
                    self.write_crlf().await?;
                }
                RespType::Integer(i) => {
                    self.writer.write_all(format!("{i}").as_bytes()).await?;
                    self.write_crlf().await?;
                }
                RespType::BulkString(data) => {
                    self.writer
                        .write_all(format!("{}", data.len()).as_bytes())
                        .await?;
                    self.write_crlf().await?;
                    self.writer.write_all(&data).await?;
                    self.write_crlf().await?;
                }
                RespType::NullBulkString => {
                    self.writer.write_u8(b'-').await?;
                    self.writer.write_u8(b'1').await?;
                    self.write_crlf().await?;
                }
                RespType::Array(items) => {
                    self.writer
                        .write_all(format!("{}", items.len()).as_bytes())
                        .await?;
                    self.write_crlf().await?;
                    for item in items {
                        self.write_item(item).await?;
                    }
                }
            }
            self.writer.flush().await?;
            Ok(())
        })
    }
}

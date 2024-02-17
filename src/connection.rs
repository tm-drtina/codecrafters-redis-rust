use std::collections::VecDeque;
use std::net::SocketAddr;

use anyhow::{anyhow, bail, ensure};
use tokio::net::TcpStream;

use crate::resp::{RespReader, RespType, RespWriter};
use crate::Server;

pub struct Connection<'a> {
    reader: RespReader<'a>,
    writer: RespWriter<'a>,
    addr: SocketAddr,
    server: Server,
}

impl<'a> Connection<'a> {
    pub fn new(stream: &'a mut TcpStream, addr: SocketAddr, server: Server) -> Self {
        let (reader, writer) = stream.split();
        Self {
            reader: RespReader::new(reader),
            writer: RespWriter::new(writer),
            addr,
            server,
        }
    }

    /// `command` must be lowercase!
    async fn command(
        &mut self,
        command: &[u8],
        mut args: VecDeque<RespType>,
    ) -> anyhow::Result<RespType> {
        eprintln!(
            "Processing command {} with args {args:?}",
            String::from_utf8_lossy(command)
        );
        Ok(match command {
            b"ping" => {
                ensure!(args.is_empty(), "PING accepts no args!");
                RespType::SimpleString(String::from("PONG"))
            }
            b"echo" => {
                ensure!(args.len() == 1, "ECHO accepts exactly one arg!");
                match args.pop_front().unwrap() {
                    arg @ RespType::BulkString(_) => arg,
                    _ => bail!("Invalid argument for `ECHO` command"),
                }
            }
            b"get" => {
                ensure!(args.len() == 1, "GET accepts exactly one arg!");
                let key = match args.pop_front().unwrap() {
                    RespType::BulkString(s) => s,
                    _ => bail!("Invalid value for `key` argument"),
                };
                let value = self.server.get(&key).await;
                match value {
                    Some(value) => RespType::BulkString(value),
                    None => RespType::NullBulkString,
                }
            }
            b"set" => {
                ensure!(args.len() >= 2, "SET requires at least two args!");
                let key = match args.pop_front().unwrap() {
                    RespType::BulkString(s) => s,
                    _ => bail!("Invalid value for `key` argument"),
                };
                let value = match args.pop_front().unwrap() {
                    RespType::BulkString(s) => s,
                    _ => bail!("Invalid value for `value` argument"),
                };
                let _old_value = self.server.set(key, value).await;
                RespType::SimpleString(String::from("OK"))
            }
            b"command" => {
                eprintln!("Ignoring `COMMAND` command. Sending back empty array");
                RespType::Array(VecDeque::new())
            }
            _ => bail!("Unknown command `{}`", String::from_utf8_lossy(command)),
        })
    }

    pub async fn run_processing_loop(mut self) -> anyhow::Result<()> {
        eprintln!("Starting processing loop for client: {:?}", self.addr);
        loop {
            let Some(item) = self.reader.read_item().await? else {
                eprintln!("Terminating processing loop for client: {:?}", self.addr);
                break;
            };
            let response = match item {
                RespType::SimpleString(mut s) => {
                    s.make_ascii_lowercase();
                    self.command(s.as_bytes(), VecDeque::new()).await?
                }
                RespType::SimpleError(err) => bail!("{err}"),
                RespType::Integer(_) => bail!("Unexpected integer received"),
                RespType::BulkString(mut s) => {
                    s.make_ascii_lowercase();
                    self.command(&mut s, VecDeque::new()).await?
                }
                RespType::Array(mut items) => {
                    let command = items
                        .pop_front()
                        .ok_or(anyhow!("Expected command, got empty array"))?;
                    match command {
                        RespType::SimpleString(mut s) => {
                            s.make_ascii_lowercase();
                            self.command(s.as_bytes(), items).await?
                        }
                        RespType::SimpleError(err) => bail!("{err}"),
                        RespType::Integer(_) => bail!("Unexpected integer received"),
                        RespType::BulkString(mut s) => {
                            s.make_ascii_lowercase();
                            self.command(&s, items).await?
                        }
                        RespType::Array(_) => bail!("Unexpected nested array"),
                        RespType::NullBulkString => unreachable!("Reader doesn't parse this value"),
                    }
                }
                RespType::NullBulkString => unreachable!("Reader doesn't parse this value"),
            };
            self.writer.write_item(response).await?;
        }
        Ok(())
    }
}

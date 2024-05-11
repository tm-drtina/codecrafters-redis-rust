use std::collections::VecDeque;
use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context};
use tokio::net::TcpStream;

use crate::resp::{RespReader, RespType, RespWriter};
use crate::Server;

const EMPTY_RDB_FILE: &[u8] = &[
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
    0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
    0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
    0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
];

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
    ) -> anyhow::Result<()> {
        eprintln!(
            "Processing command {} with args {args:?}",
            String::from_utf8_lossy(command)
        );
        match command {
            b"ping" => {
                ensure!(args.is_empty(), "PING accepts no args!");
                let response = RespType::SimpleString(String::from("PONG"));
                self.writer.write_item(response).await?;
            }
            b"echo" => {
                ensure!(args.len() == 1, "ECHO accepts exactly one arg!");
                let response = match args.pop_front().unwrap() {
                    arg @ RespType::BulkString(_) => arg,
                    _ => bail!("Invalid argument for `ECHO` command"),
                };
                self.writer.write_item(response).await?;
            }
            b"get" => {
                ensure!(args.len() == 1, "GET accepts exactly one arg!");
                let key = match args.pop_front().unwrap() {
                    RespType::BulkString(s) => s,
                    _ => bail!("Invalid value for `key` argument"),
                };
                let value = self.server.get(key).await;
                let response = match value {
                    Some(value) => RespType::BulkString(value),
                    None => RespType::NullBulkString,
                };
                self.writer.write_item(response).await?;
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
                let mut expiry = None;
                while let Some(mut arg) = args.pop_front() {
                    let argname = arg.make_str_bytes_lowercase()?;
                    match argname {
                        b"px" => {
                            let duration_mili = args.pop_front().ok_or(anyhow!("Missing value for `px` arg"))?.as_int().context("Value of `px` arg must be an integer")?;
                            ensure!(duration_mili >= 0, "Expiration cannot be negative");
                            expiry = Some(Duration::from_millis(duration_mili as u64));
                        }
                        _ => bail!("Unknown parameter `{}` for `SET` command", String::from_utf8_lossy(argname))
                    }
                }
                let _old_value = self.server.set(key, value, expiry).await;
                let response = RespType::SimpleString(String::from("OK"));
                self.writer.write_item(response).await?;
            }
            b"info" => {
                let mut buf = Vec::new();
                buf.extend_from_slice(b"# Replication");
                match self.server.replication {
                    crate::ReplicationMode::Master => buf.extend_from_slice(b"\nrole:master"),
                    crate::ReplicationMode::Slave { .. } => buf.extend_from_slice(b"\nrole:slave"),
                }
                write!(&mut buf, "\nmaster_replid:{}", self.server.master_replid).context("Falied to write info data")?;
                write!(&mut buf, "\nmaster_repl_offset:{}", self.server.master_repl_offset).context("Falied to write info data")?;
                let response = RespType::BulkString(buf.into_boxed_slice());
                self.writer.write_item(response).await?;
            }
            b"command" => {
                eprintln!("Ignoring `COMMAND` command. Sending back empty array");
                let response = RespType::Array(VecDeque::new());
                self.writer.write_item(response).await?;
            }
            b"replconf" => {
                eprintln!("Ignoring `REPLCONF` command.");
                let response = RespType::SimpleString(String::from("OK"));
                self.writer.write_item(response).await?;
            }
            b"psync" => {
                ensure!(args.len() == 2, "PSYNC requires exactly two args!");
                let master_id = match args.pop_front().unwrap() {
                    RespType::BulkString(s) => s,
                    _ => bail!("Invalid value for `key` argument"),
                };
                let offset = match args.pop_front().unwrap() {
                    RespType::BulkString(s) => s,
                    _ => bail!("Invalid value for `value` argument"),
                };
                ensure!(*master_id == *b"?", "Expected unknown master ID");
                ensure!(*offset == *b"-1", "Expected unknown master ID");

                let response = RespType::SimpleString(format!("FULLRESYNC {} {}", self.server.master_replid, self.server.master_repl_offset));
                self.writer.write_item(response).await?;

                self.writer.write_rdb_file(EMPTY_RDB_FILE).await?;
            }
            _ => bail!("Unknown command `{}`", String::from_utf8_lossy(command)),
        }
        Ok(())
    }

    pub async fn run_processing_loop(mut self) -> anyhow::Result<()> {
        eprintln!("Starting processing loop for client: {:?}", self.addr);
        loop {
            let Some(item) = self.reader.read_item().await? else {
                eprintln!("Terminating processing loop for client: {:?}", self.addr);
                break;
            };
            match item {
                RespType::SimpleString(mut s) => {
                    s.make_ascii_lowercase();
                    self.command(s.as_bytes(), VecDeque::new()).await?
                }
                RespType::SimpleError(err) => bail!("{err}"),
                RespType::Integer(_) => bail!("Unexpected integer received"),
                RespType::BulkString(mut s) => {
                    s.make_ascii_lowercase();
                    self.command(&s, VecDeque::new()).await?
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
        }
        Ok(())
    }
}

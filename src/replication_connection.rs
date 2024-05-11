use std::iter::once;

use anyhow::{bail, ensure, Context};
use tokio::net::TcpStream;

use crate::resp::{RespReader, RespType, RespWriter};
use crate::Server;

pub struct ReplicationConnection<'a> {
    reader: RespReader<'a>,
    writer: RespWriter<'a>,
    #[allow(dead_code)]
    server: Server,
}

impl<'a> ReplicationConnection<'a> {
    pub fn new(stream: &'a mut TcpStream, server: Server) -> Self {
        let (reader, writer) = stream.split();
        Self {
            reader: RespReader::new(reader),
            writer: RespWriter::new(writer),
            server,
        }
    }

    async fn ensure_pong(&mut self) -> anyhow::Result<()> {
        match self.reader.read_item().await? {
            Some(RespType::SimpleString(s)) if s == "PONG" => Ok(()),
            Some(_) => bail!("Received invalid reponse for PING"),
            None => bail!("Failed to receive PONG response to our PING"),
        }
    }
    async fn ensure_ok(&mut self) -> anyhow::Result<()> {
        match self.reader.read_item().await? {
            Some(RespType::SimpleString(s)) if s == "OK" => Ok(()),
            Some(_) => bail!("Received invalid reponse for PING"),
            None => bail!("Failed to receive PONG response to our PING"),
        }
    }
    async fn ensure_full_resync(&mut self) -> anyhow::Result<String> {
        match self.reader.read_item().await? {
            Some(RespType::SimpleString(s)) if s.starts_with("FULLRESYNC ") => {
                let (id, offset) = s
                    .strip_prefix("FULLRESYNC ")
                    .unwrap()
                    .split_once(' ')
                    .context("Invalid format of FULLRESYNC response")?;
                ensure!(offset == "0", "Expected received offset to be zero");
                Ok(id.to_string())
            }
            Some(_) => bail!("Received invalid reponse for PSYNC"),
            None => bail!("Failed to receive FULLRESYNC response to our PSYNC"),
        }
    }

    async fn handshake(&mut self) -> anyhow::Result<()> {
        eprintln!("Starting replication handshake");
        self.writer.write_item(RespType::Array(once(RespType::BulkString(b"ping".to_vec().into_boxed_slice())).collect())).await?;
        self.ensure_pong().await?;
        self.writer.write_item(RespType::Array(
            // REPLCONF listening-port <PORT>
            once(RespType::bulk_string_from_bytes(b"REPLCONF"))
            .chain(once(RespType::bulk_string_from_bytes(b"listening-port")))
            .chain(once(RespType::bulk_string_from_string(format!("{}", self.server.addr.port()))))
            .collect()
        )).await?;
        self.ensure_ok().await?;
        self.writer.write_item(RespType::Array(
            // REPLCONF capa psync2
            once(RespType::bulk_string_from_bytes(b"REPLCONF"))
            .chain(once(RespType::bulk_string_from_bytes(b"capa")))
            .chain(once(RespType::bulk_string_from_bytes(b"psync2")))
            .collect()
        )).await?;
        self.ensure_ok().await?;
        self.writer.write_item(RespType::Array(
            // PSYNC ? -1
            once(RespType::bulk_string_from_bytes(b"PSYNC"))
            .chain(once(RespType::bulk_string_from_bytes(b"?")))
            .chain(once(RespType::bulk_string_from_bytes(b"-1")))
            .collect()
        )).await?;
        // TODO: store it
        // FULLRESYNC <ID> <offset>
        let _master_id = self.ensure_full_resync().await?;
        // handle RDB file...
        eprintln!("Replication handshake done");
        Ok(())
    }

    pub async fn run_replication_loop(mut self) -> anyhow::Result<()> {
        self.handshake().await?;
        eprintln!("Starting replication loop");
        /*loop {
            
        }*/
        eprintln!("Stopping replication loop");
        Ok(())
    }
}

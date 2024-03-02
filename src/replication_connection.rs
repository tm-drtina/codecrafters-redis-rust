use std::iter::once;

use anyhow::bail;
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

    async fn handshake(&mut self) -> anyhow::Result<()> {
        eprintln!("Starting replication handshake");
        self.writer.write_item(RespType::Array(once(RespType::BulkString(b"ping".to_vec().into_boxed_slice())).collect())).await?;
        self.ensure_pong().await?;
        self.writer.write_item(RespType::Array(
            //REPLCONF listening-port <PORT>
            once(RespType::bulk_string_from_bytes(b"REPLCONF"))
            .chain(once(RespType::bulk_string_from_bytes(b"listening-port")))
            .chain(once(RespType::bulk_string_from_string(format!("{}", self.server.addr.port()))))
            .collect()
        )).await?;
        self.ensure_ok().await?;
        self.writer.write_item(RespType::Array(
            //REPLCONF capa psync2
            once(RespType::bulk_string_from_bytes(b"REPLCONF"))
            .chain(once(RespType::bulk_string_from_bytes(b"capa")))
            .chain(once(RespType::bulk_string_from_bytes(b"psync2")))
            .collect()
        )).await?;
        self.ensure_ok().await?;
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

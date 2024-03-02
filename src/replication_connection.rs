use std::iter::once;

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

    pub async fn run_replication_loop(mut self) -> anyhow::Result<()> {
        eprintln!("Starting replication loop");
        self.writer.write_item(RespType::Array(once(RespType::BulkString(b"ping".to_vec().into_boxed_slice())).collect())).await?;
        eprintln!("Send ping to master node");
        let _ = self.reader.read_item().await?;
        eprintln!("Stopping replication loop");
        Ok(())
    }
}

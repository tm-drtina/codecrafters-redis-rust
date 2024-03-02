use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::data::Data;

#[derive(Debug)]
pub enum ReplicationMode {
    Master,
    Slave { host: String, port: u16 },
}

#[derive(Debug)]
pub struct Inner {
    pub(crate) replication: ReplicationMode,
    pub(crate) master_replid: String,
    pub(crate) master_repl_offset: usize,
    data: Mutex<Data>,
}

#[derive(Debug)]
pub struct Server(pub(crate) Arc<Inner>);

impl Deref for Server {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for Server {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Server {
    pub fn new(replication: ReplicationMode) -> Self {
        let master_replid = String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        let master_repl_offset = 0;

        Self(Arc::new(Inner {
            replication,
            master_replid,
            master_repl_offset,
            data: Default::default(),
        }))
    }

    pub async fn get(&self, key: Box<[u8]>) -> Option<Box<[u8]>> {
        self.0.data.lock().await.get(key).await
    }
    pub async fn set(
        &self,
        key: Box<[u8]>,
        value: Box<[u8]>,
        expiry: Option<Duration>,
    ) -> Option<Box<[u8]>> {
        self.0.data.lock().await.set(key, value, expiry).await
    }
}

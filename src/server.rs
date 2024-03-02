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
        Self(Arc::new(Inner {
            replication,
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

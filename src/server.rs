use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::data::Data;

#[derive(Debug, Default)]
pub struct Server(Arc<Mutex<Data>>);

impl Clone for Server {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Server {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self, key: Box<[u8]>) -> Option<Box<[u8]>> {
        self.0.lock().await.get(key).await
    }
    pub async fn set(&self, key: Box<[u8]>, value: Box<[u8]>, expiry: Option<Duration>) -> Option<Box<[u8]>> {
        self.0.lock().await.set(key, value, expiry).await
    }
}

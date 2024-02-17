use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

struct Inner {
    data: HashMap<Box<[u8]>, Box<[u8]>>,
}

pub struct Server(Arc<Mutex<Inner>>);

impl Clone for Server {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Server {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner {
            data: HashMap::new(),
        })))
    }

    pub async fn get(&self, key: &Box<[u8]>) -> Option<Box<[u8]>> {
        self.0.lock().await.data.get(key).cloned()
    }
    pub async fn set(&self, key: Box<[u8]>, value: Box<[u8]>) -> Option<Box<[u8]>> {
        self.0.lock().await.data.insert(key, value)
    }
}

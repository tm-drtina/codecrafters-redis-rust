use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct ValueWithMeta {
    value: Box<[u8]>,
    expiry: Option<Instant>,
}

#[derive(Debug, Default)]
pub struct Data {
    data: HashMap<Box<[u8]>, ValueWithMeta>,
}

impl ValueWithMeta {
    fn new(value: Box<[u8]>, expiry: Option<Duration>) -> Self {
        Self {
            value,
            expiry: expiry.map(|d| Instant::now().checked_add(d).expect("Invalid duration for expire")),
        }
    }
}

impl Data {
    pub async fn get(&mut self, key: Box<[u8]>) -> Option<Box<[u8]>> {
        let entry = match self.data.entry(key) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => return None,
        };
        let val_meta = entry.get();
        if let Some(exp) = val_meta.expiry {
            if exp <= Instant::now() {
                entry.remove();
                return None
            }
        };
        Some(val_meta.value.clone())
    }
    pub async fn set(&mut self, key: Box<[u8]>, value: Box<[u8]>, expiry: Option<Duration>) -> Option<Box<[u8]>> {
        let value_with_meta = ValueWithMeta::new(value, expiry);
        self.data.insert(key, value_with_meta).map(|v| v.value)
    }
}

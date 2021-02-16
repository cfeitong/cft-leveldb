//! memory table

use std::collections::BTreeMap;

use bytes::Bytes;
use tokio::sync::Mutex;

/// an ordered table in memory
pub struct MemTable {
    inner: Mutex<BTreeMap<Bytes, Bytes>>,
}

impl MemTable {
    /// create an empty [`MemTable`]
    pub fn new() -> Self {
        MemTable {
            inner: Mutex::default(),
        }
    }

    /// get value from memtable
    pub async fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.inner.lock().await.get(key).cloned()
    }

    /// set key value pair, return possible old value
    pub async fn set(&self, key: Bytes, value: Bytes) -> Option<Bytes> {
        self.inner.lock().await.insert(key, value)
    }

    /// check if key is in memtable
    pub async fn contains(&self, key: &Bytes) -> bool {
        self.inner.lock().await.contains_key(key)
    }

    /// remove corresponding value
    pub async fn remove(&self, key: &Bytes) -> Option<Bytes> {
        self.inner.lock().await.remove(key)
    }
}

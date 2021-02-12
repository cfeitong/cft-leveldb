//! db interface

use crate::mem_table::MemTable;
use crate::vfs::VFS;
use std::path::Path;
use bytes::Bytes;

/// db interface object
pub struct Db {
    vfs: VFS,
    mem: MemTable,
}

impl Db {
    /// create a new db
    pub async fn create(path: impl AsRef<Path>) -> Self {
        Db {
            vfs: VFS::new(path.as_ref().to_owned()),
            mem: MemTable::new(),
        }
    }

    /// get value from db
    pub async fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.mem.get(key).await
    }

    /// set key value pair in db
    pub async fn set(&self, key: Bytes, value: Bytes) -> Option<Bytes> {
        self.mem.set(key, value).await
    }
}

//! db interface

use std::path::Path;

use bytes::Bytes;
use thiserror::Error;

use crate::{
    mem_table::MemTable,
    vfs::{
        Vfs,
        VfsError,
    },
    wal::{
        Wal,
        WalError,
    },
};

#[derive(Debug, Error)]
pub enum DbError {
    #[error(transparent)]
    VfsError(#[from] VfsError),
    #[error(transparent)]
    WalError(#[from] WalError),
}

type Result<T> = std::result::Result<T, DbError>;

/// db interface object
pub struct Db {
    vfs: Vfs,
    mem: MemTable,
    wal: Wal,
}

impl Db {
    /// create a new db
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let vfs = Vfs::new(path.to_owned()).await?;
        let wal = Wal::open(vfs.clone()).await?;
        Ok(Db {
            vfs,
            mem: MemTable::new(),
            wal,
        })
    }

    /// get value from db
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let key = key.as_ref();
        Ok(self.mem.get(key).await)
    }

    /// set key value pair in db
    pub async fn set(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.wal.set(&key, &value).await?;
        self.mem.set(key, value).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_db_basic() {
        let db = Db::create("test-db").await.unwrap();
        assert!(db.get("non-exists key").await.unwrap().is_none());
        assert!(db.set("key1".into(), "val1".into()).await.is_ok());
        assert_eq!(db.get("key1").await.unwrap(), Some("val1".into()));
        assert!(db.set("key1".into(), "val2".into()).await.is_ok());
    }
}

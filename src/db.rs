//! db interface

use std::path::Path;

use bytes::Bytes;

use crate::error::Result;
use crate::mem_table::MemTable;
use crate::vfs::VFS;

/// db interface object
pub struct Db {
    vfs: VFS,
    mem: MemTable,
}

impl Db {
    /// create a new db
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Db {
            vfs: VFS::new(path.as_ref().to_owned()),
            mem: MemTable::new(),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_db_basic() {
        let db = Db::create("test-db").await.unwrap();
        assert!(db.get(&"non-exists key".to_string().into()).await.is_none());
        assert!(db
            .set("key1".to_string().into(), "val1".to_string().into())
            .await
            .is_none());
        assert_eq!(
            db.get(&"key1".to_string().into()).await,
            Some("val1".to_string().into())
        );
        assert_eq!(
            db.set("key1".into(), "val2".into()).await,
            Some("val1".into())
        );
    }
}

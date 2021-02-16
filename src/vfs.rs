//! virtual file system

use std::io::SeekFrom;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use tokio::fs::{
    File,
    OpenOptions,
};
use tokio::io::{
    AsyncReadExt,
    AsyncSeekExt,
    AsyncWriteExt,
};
use tokio::sync::Mutex;

use crate::error::Result;

/// virtual file system object, which encapsulate all states
pub struct VFS {
    inner: Arc<VFSInner>,
}

impl VFS {
    /// create new VFS object
    pub fn new(base: PathBuf) -> Self {
        VFS {
            inner: Arc::new(VFSInner { base }),
        }
    }

    /// open sstable file by level
    pub async fn open_sstable(&self, level: usize) -> Result<VFile> {
        let path = self.base().join(level.to_string());
        let reader = OpenOptions::new().read(true).open(&path).await?;
        let writer = OpenOptions::new().append(true).open(&path).await?;
        Ok(VFile {
            inner: Mutex::new(VFileInner { reader, writer }),
        })
    }

    fn base(&self) -> PathBuf {
        self.inner.base.clone()
    }
}

struct VFSInner {
    base: PathBuf,
}

/// virtual file representation
pub struct VFile {
    inner: Mutex<VFileInner>,
}

impl VFile {
    /// Append data to [`VFile`] and return appended size.
    pub async fn append(&mut self, data: &[u8]) -> Result<()> {
        self.inner.lock().await.append(data).await?;
        Ok(())
    }

    /// Read a block of `len` size starting from `offset`.
    pub async fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.inner.lock().await.read_at(offset, len).await
    }

    /// Synchronize file
    pub async fn sync(&self) -> Result<()> {
        self.inner.lock().await.sync().await?;
        Ok(())
    }
}

struct VFileInner {
    writer: File,
    reader: File,
}

impl VFileInner {
    async fn append(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data).await?;
        Ok(())
    }

    async fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.reader.seek(SeekFrom::Start(offset)).await?;
        let mut ret = vec![0u8; len];
        self.reader.read_exact(&mut ret).await?;
        Ok(ret)
    }

    async fn sync(&self) -> Result<()> {
        self.writer.sync_all().await?;
        Ok(())
    }
}

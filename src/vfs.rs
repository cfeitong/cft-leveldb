//! virtual file system

use std::{
    io::SeekFrom,
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

use thiserror::Error;
use tokio::{
    fs::{
        File,
        OpenOptions,
    },
    io::{
        AsyncReadExt,
        AsyncSeekExt,
        AsyncWriteExt,
    },
    sync::Mutex,
};

#[derive(Debug, Error)]
pub enum VfsError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, VfsError>;

/// virtual file system object, which encapsulate all states
#[derive(Clone)]
pub struct Vfs {
    inner: Arc<VfsInner>,
}

struct VfsInner {
    base: PathBuf,
}

impl Vfs {
    /// create new VFS object
    pub fn new(base: PathBuf) -> Self {
        Vfs {
            inner: Arc::new(VfsInner { base }),
        }
    }

    pub async fn open(&self, path: impl AsRef<Path>) -> Result<VFile> {
        let path = self.base().join(path);
        let file = VFile::open(&path).await?;
        Ok(file)
    }

    /// open sstable file by level
    pub async fn open_sstable(&self, level: usize) -> Result<VFile> {
        let path = self.base().join(level.to_string());
        let inner = VFileInner::open(&path).await?;
        Ok(VFile {
            inner: Mutex::new(inner),
        })
    }

    fn base(&self) -> PathBuf {
        self.inner.base.clone()
    }
}

/// virtual file representation
pub struct VFile {
    inner: Mutex<VFileInner>,
}

struct VFileInner {
    writer: File,
    reader: File,
}

impl VFile {
    /// Append data to [`VFile`] and return appended size.
    pub async fn append(&self, data: &[u8]) -> Result<()> {
        self.inner.lock().await.append(data).await?;
        Ok(())
    }

    /// Read a block of `len` size starting from `offset`.
    pub async fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.inner.lock().await.read_at(offset, len).await
    }

    /// Read underline file
    pub async fn read_exact(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.inner.lock().await.reader.read_exact(buf).await?)
    }

    /// Synchronize file
    pub async fn sync(&self) -> Result<()> {
        self.inner.lock().await.sync().await?;
        Ok(())
    }

    /// file length in bytes
    pub async fn len(&self) -> Result<usize> {
        self.inner.lock().await.len().await
    }

    async fn open(path: &Path) -> Result<Self> {
        let inner = VFileInner::open(path).await?;
        Ok(VFile {
            inner: Mutex::new(inner),
        })
    }
}

impl VFileInner {
    async fn open(path: &Path) -> Result<Self> {
        let reader = OpenOptions::new().read(true).open(path).await?;
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(VFileInner { reader, writer })
    }

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

    async fn len(&self) -> Result<usize> {
        Ok(self.reader.metadata().await?.len() as usize)
    }
}

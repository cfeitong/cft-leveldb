//! virtual file system

use std::{
    io::SeekFrom,
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

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

use crate::{
    error::Result,
    wal::WalFile,
};

/// virtual file system object, which encapsulate all states
#[derive(Clone)]
pub struct VFS {
    inner: Arc<VFSInner>,
}

struct VFSInner {
    base: PathBuf,
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
        let inner = VFileInner::open(&path).await?;
        Ok(VFile {
            inner: Mutex::new(inner),
        })
    }

    /// open write ahead log file
    pub async fn open_wal(&self) -> Result<WalFile> {
        let path = self.base().join("wal.log");
        let inner = VFileInner::open(&path).await?;
        let vfile = VFile {
            inner: Mutex::new(inner),
        };
        let wal = WalFile::from_vfile(vfile).await?;
        Ok(wal)
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

    /// Synchronize file
    pub async fn sync(&self) -> Result<()> {
        self.inner.lock().await.sync().await?;
        Ok(())
    }

    /// file length in bytes
    pub async fn len(&self) -> Result<usize> {
        self.inner.lock().await.len().await
    }
}

impl VFileInner {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
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

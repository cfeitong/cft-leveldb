use bytes::{
    BufMut,
    Bytes,
    BytesMut,
};
use futures::prelude::*;
use thiserror::Error;
use tokio::sync::Mutex;
use vfs::VfsError;

use crate::{
    encoding::BufMutExt,
    vfs::{
        self,
        VFile,
        Vfs,
    },
};

#[derive(Debug, Error)]
pub enum WalError {
    #[error(transparent)]
    VfsError(#[from] vfs::VfsError),
    #[error("invalid WAL file")]
    InvalidWalFileError,
    #[error("invalid record type")]
    InvalidRecordTypeError,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, WalError>;

const BLOCK_SIZE: usize = 32768;
// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const HEADER_SIZE: usize = 4 + 2 + 1;

pub struct Wal {
    writer: Mutex<WalFileWriter>,
}

impl Wal {
    pub async fn open(vfs: Vfs) -> Result<Self> {
        Ok(Wal {
            writer: Mutex::new(WalFileWriter::open(vfs).await?),
        })
    }

    pub async fn set(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        self.set_impl(key, value).await
    }

    async fn set_impl(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = BytesMut::new();
        data.put_var_u32_le(key.len() as u32);
        data.put(key);
        data.put_var_u32_le(value.len() as u32);
        data.put(value);
        self.writer.lock().await.write_data(data.freeze()).await?;
        Ok(())
    }
}

/// represent WAL writer
pub struct WalFileWriter {
    file:         VFile,
    block_offset: usize,
}

impl WalFileWriter {
    /// into inner vfile
    pub fn into_inner(self) -> VFile {
        self.file
    }

    /// open wal file
    pub async fn open(vfs: Vfs) -> Result<Self> {
        let vfile = vfs.open("wal.log").await?;
        let wal = WalFileWriter::new(vfile).await?;
        Ok(wal)
    }

    /// write a record
    pub async fn write_data(&mut self, data: Bytes) -> Result<()> {
        let mut rest_data = Some(data.as_ref());
        let mut is_begin = true;
        let mut is_end = false;
        while let Some(data) = rest_data {
            let left_over = BLOCK_SIZE - self.block_offset;
            if left_over < HEADER_SIZE {
                // move to next block
                const ZEROES: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
                self.file.append(&ZEROES[..left_over]).await?;
                self.block_offset = 0;
            }

            let cur_len = if left_over >= data.len() {
                is_end = true;
                data.len()
            } else {
                left_over
            };

            self.emit_physical_record(RecordType::calc(is_begin, is_end), &data[..cur_len])
                .await?;
            self.block_offset += cur_len;
            is_begin = false;

            rest_data = data.get(cur_len..);
            if data.len() == cur_len {
                break;
            }
        }

        Ok(())
    }

    pub async fn new(vfile: VFile) -> Result<Self> {
        let block_offset = vfile.len().await? % BLOCK_SIZE;
        Ok(WalFileWriter {
            file: vfile,
            block_offset,
        })
    }

    async fn emit_physical_record(&mut self, ty: RecordType, rec: &[u8]) -> Result<()> {
        use crc::crc32::Hasher32;
        let mut digest = crc::crc32::Digest::new(crc::crc32::CASTAGNOLI);
        digest.write(&[ty as u8]);
        digest.write(rec);
        let checksum = digest.sum32();
        let mut data = BytesMut::new();
        data.put_u32_le(checksum);
        data.put_u16_le(rec.len() as u16);
        data.put_u8(ty as u8);
        data.put_slice(rec);
        let data = data.freeze();
        self.file.append(&data).await?;
        Ok(())
    }
}

/// represent WAL reader
pub struct WalFileReader {
    file: VFile,
}

impl WalFileReader {
    pub async fn new(file: VFile) -> Result<Self> {
        Ok(WalFileReader { file })
    }

    pub async fn open(vfs: Vfs) -> Result<Self> {
        let vfile = vfs.open("wal.log").await?;
        let wal = WalFileReader::new(vfile).await?;
        Ok(wal)
    }

    async fn read_record(&mut self) -> Result<Option<Record>> {
        let mut buf = vec![0u8; 4 + 2 + 1]; // chucksum(u32) + length(u16) + type(u8), little endian
        let _read = match self.file.read_exact(&mut buf).await {
            Err(VfsError::IoError(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None)
            }
            result @ _ => result?,
        };
        let mut crc = [0u8; 4];
        crc.clone_from_slice(&buf[0..4]);
        let crc = u32::from_le_bytes(crc);

        let mut len = [0u8; 2];
        len.clone_from_slice(&buf[4..6]);
        let len = u16::from_le_bytes(len);

        let ty = buf[6];
        let mut data = vec![0u8; len as usize];
        self.file.read_exact(&mut data).await?;
        let record = Record {
            crc,
            len,
            ty: RecordType::from_u8(ty)?,
            data: data.into(),
        };
        if !record.is_valid() {
            return Err(WalError::InvalidRecordTypeError);
        }

        Ok(Some(record))
    }

    fn into_record_stream(self) -> impl Stream<Item = Result<Record>> {
        futures::stream::unfold(self, |mut reader| async move {
            let record = reader.read_record().await.transpose()?;
            Some((record, reader))
        })
    }

    pub async fn read_data(&mut self) -> Result<Option<Bytes>> {
        let first = self.read_record().await?;
        let first = match first {
            Some(first) => first,
            None => return Ok(None),
        };
        match first.ty {
            RecordType::Full => return Ok(Some(first.data)),
            RecordType::First => {}
            _ => return Err(WalError::InvalidRecordTypeError),
        }
        let mut result = vec![first];
        result.extend(read_rest_records(self).await?);
        Ok(Some(
            result
                .into_iter()
                .fold(BytesMut::new(), |mut data, record| {
                    data.extend(record.data);
                    data
                })
                .freeze(),
        ))
    }

    pub fn into_data_stream(self) -> impl Stream<Item = Result<Bytes>> {
        futures::stream::unfold(self, |mut reader| async move {
            let data = reader.read_data().await.transpose()?;
            Some((data, reader))
        })
    }
}

async fn read_rest_records(reader: &mut WalFileReader) -> Result<Vec<Record>> {
    let mut result = vec![];
    loop {
        let record = reader.read_record().await?;
        let record = match record {
            Some(record) => record,
            None => return Err(WalError::InvalidWalFileError),
        };
        match record.ty {
            RecordType::Middle => result.push(record),
            RecordType::Last => {
                result.push(record);
                break;
            }
            _ => return Err(WalError::InvalidRecordTypeError),
        }
    }
    Ok(result)
}

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum RecordType {
    Full   = 1,
    First  = 2,
    Middle = 3,
    Last   = 4,
}

impl RecordType {
    fn calc(is_begin: bool, is_end: bool) -> Self {
        match (is_begin, is_end) {
            (true, true) => RecordType::Full,
            (true, false) => RecordType::First,
            (false, true) => RecordType::Last,
            (false, false) => RecordType::Middle,
        }
    }

    fn from_u8(ty: u8) -> Result<Self> {
        use RecordType::*;
        match ty {
            1 => Ok(Full),
            2 => Ok(First),
            3 => Ok(Middle),
            4 => Ok(Last),
            _ => Err(WalError::InvalidRecordTypeError),
        }
    }
}

pub struct Record {
    pub crc:  u32,
    pub len:  u16,
    pub ty:   RecordType,
    pub data: Bytes,
}

impl Record {
    fn is_valid(&self) -> bool {
        use crc::crc32::Hasher32;
        let mut digest = crc::crc32::Digest::new(crc::crc32::CASTAGNOLI);
        digest.write(&[self.ty as u8]);
        digest.write(&self.data);
        let crc = digest.sum32();
        crc == self.crc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen_data(bytes: usize) -> Bytes {
        let mut data = BytesMut::new();
        for i in 0..bytes {
            data.put_u8((i % 256) as u8);
        }
        data.freeze()
    }

    fn validate_data(data: &Bytes) -> bool {
        (0..data.len()).all(|i| data[i] == ((i % 256) as u8))
    }

    async fn setup_reader_writer() -> Result<(WalFileReader, WalFileWriter)> {
        let dir = tempfile::tempdir().unwrap().into_path();
        let vfs = Vfs::new(dir).await?;
        let writer = WalFileWriter::open(vfs.clone()).await.unwrap();
        let reader = WalFileReader::open(vfs).await.unwrap();
        Ok((reader, writer))
    }

    async fn write(writer: &mut WalFileWriter, data: &str) -> Result<()> {
        let data = Bytes::copy_from_slice(data.as_bytes());
        writer.write_data(data).await?;
        Ok(())
    }

    async fn read(reader: &mut WalFileReader) -> Result<String> {
        let data = reader
            .read_data()
            .await?
            .as_ref()
            .map(|data| String::from_utf8_lossy(&*data).to_string())
            .unwrap_or_else(|| "EOF".to_string());
        Ok(data)
    }

    #[tokio::test]
    async fn test_wal_data_read_write() {
        let (mut reader, mut writer) = setup_reader_writer().await.unwrap();
        let reader = &mut reader;
        let writer = &mut writer;
        write(writer, "foo").await.unwrap();
        write(writer, "bar").await.unwrap();
        write(writer, "").await.unwrap();
        write(writer, "xxxx").await.unwrap();
        assert_eq!(read(reader).await.unwrap(), "foo");
        assert_eq!(read(reader).await.unwrap(), "bar");
        assert_eq!(read(reader).await.unwrap(), "");
        assert_eq!(read(reader).await.unwrap(), "xxxx");
        assert_eq!(read(reader).await.unwrap(), "EOF");
        assert_eq!(read(reader).await.unwrap(), "EOF");
    }

    #[tokio::test]
    async fn test_wal_data_many_blocks() {
        let (mut reader, mut writer) = setup_reader_writer().await.unwrap();
        for _i in 0..10usize {
            writer.write_data(gen_data(1024)).await.unwrap();
        }

        for _ in 0..10usize {
            writer.write_data(gen_data(102400)).await.unwrap();
        }

        for _i in 0..100usize {
            writer.write_data(gen_data(60)).await.unwrap();
        }
        for _i in 0..10usize {
            let data = reader.read_data().await.unwrap().unwrap();
            assert_eq!(data.len(), 1024);
            assert!(validate_data(&data));
        }
        for _i in 0..10usize {
            let data = reader.read_data().await.unwrap().unwrap();
            assert_eq!(data.len(), 102400);
            assert!(validate_data(&data));
        }
        for _i in 0..100usize {
            let data = reader.read_data().await.unwrap().unwrap();
            assert_eq!(data.len(), 60);
            assert!(validate_data(&data));
        }
        assert!(reader.read_data().await.unwrap().is_none());
    }
}

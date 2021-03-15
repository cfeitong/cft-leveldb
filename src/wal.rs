use bytes::{
    BufMut,
    Bytes,
    BytesMut,
};
use thiserror::Error;

use crate::vfs::{
    self,
    VFile,
    Vfs,
};

#[derive(Debug, Error)]
pub enum WalError {
    #[error(transparent)]
    VfsError(#[from] vfs::VfsError),
    #[error("invalid record type")]
    InvalidRecordTypeError,
}

type Result<T> = std::result::Result<T, WalError>;

const BLOCK_SIZE: usize = 32768;
// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const HEADER_SIZE: usize = 4 + 2 + 1;

/// represent WAL writer
pub struct WalFileWriter {
    file:         VFile,
    block_offset: usize,
}

impl WalFileWriter {
    /// open wal file
    pub async fn open(vfs: Vfs) -> Result<Self> {
        let vfile = vfs.open("wal.log").await?;
        let wal = WalFileWriter::new(vfile).await?;
        Ok(wal)
    }

    /// write a record
    pub async fn write_record(&mut self, data: Bytes) -> Result<()> {
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

            self.emit_physical_record(RecordType::calc(is_begin, is_end), &data[..cur_len]);
            self.block_offset += cur_len;
            is_begin = false;

            rest_data = data.get(cur_len..);
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

    async fn read_record(&mut self) -> Result<Record> {
        let mut buf = vec![0u8; 4 + 2 + 1]; // chucksum(u32) + length(u16) + type(u8), little endian
        self.file.read_exact(&mut buf).await?;
        let mut crc = [0u8; 4];
        crc.clone_from_slice(&buf[0..4]);
        let crc = u32::from_le_bytes(crc);

        let mut len = [0u8; 2];
        len.clone_from_slice(&buf[4..6]);
        let len = u16::from_le_bytes(len);

        let ty = buf[6];
        let mut data = vec![0u8; len as usize];
        self.file.read_exact(&mut data).await?;

        Ok(Record {
            crc,
            len,
            ty: RecordType::from_u8(ty)?,
            data: data.into(),
        })
    }
}

impl IntoIterator for WalFileReader {
    type IntoIter = WalFileIter;
    type Item = Result<Record>;

    fn into_iter(self) -> Self::IntoIter {
        WalFileIter { reader: self }
    }
}

/// iterator of logs
pub struct WalFileIter {
    reader: WalFileReader,
}

impl Iterator for WalFileIter {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
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

struct Block {}

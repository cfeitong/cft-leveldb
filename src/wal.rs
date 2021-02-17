use bytes::{
    BufMut,
    Bytes,
    BytesMut,
};

use crate::{
    error::Result,
    vfs::VFile,
};

const BLOCK_SIZE: usize = 32768;
// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const HEADER_SIZE: usize = 4 + 2 + 1;

/// represent write ahead log file
pub struct WalFile {
    file:         VFile,
    block_offset: usize,
}

impl WalFile {
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

    pub async fn from_vfile(vfile: VFile) -> Result<Self> {
        let block_offset = vfile.len().await? % BLOCK_SIZE;
        Ok(WalFile {
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

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
enum RecordType {
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
}

struct Record {
    crc:  u32,
    len:  u16,
    ty:   RecordType,
    data: Bytes,
}

struct Block {}

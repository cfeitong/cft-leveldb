mod encoding;
mod mem_table;
mod vfs;
mod wal;

pub mod db;
pub mod error;

pub use bytes::Bytes;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

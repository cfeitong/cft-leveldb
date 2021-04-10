mod encoding;
mod mem_table;
mod sorted_stable;
mod vfs;
mod wal;

pub mod db;

pub use bytes::Bytes;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("unknown error")]
    UnknownError,
}

pub type Result<T> = std::result::Result<T, DbError>;

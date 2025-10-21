use std::io::ErrorKind;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    IOError(#[from] std::io::Error),

    #[error("{0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("{0}")]
    PoisonError(String),

    #[error("Corrupted data")]
    CorruptedData,

    #[error("{0}")]
    InvalidData(String),

    #[error("Remove failed")]
    RemoveError,

    #[error("Unknown error")]
    Unknown,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Error::IOError(e) => e.kind(),
            _ => ErrorKind::Other,
        }
    }
}

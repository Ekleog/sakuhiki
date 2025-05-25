use std::result::Result as StdResult;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug, thiserror::Error)]
#[error("{operation}")]
pub struct Error {
    operation: ErrorWhile,
    source: rocksdb::Error,
}

impl Error {
    pub(crate) fn rocksdb(operation: ErrorWhile, source: rocksdb::Error) -> Self {
        Self { operation, source }
    }
}

#[derive(Debug, derive_more::Display)]
#[non_exhaustive]
pub enum ErrorWhile {}

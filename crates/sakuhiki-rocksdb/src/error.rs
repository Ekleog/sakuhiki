use std::path::PathBuf;
use std::result::Result as StdResult;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug, thiserror::Error)]
#[error("{operation}")]
pub struct Error {
    operation: ErrorWhile,
    source: ErrorSource,
}

impl Error {
    pub(crate) fn rocksdb(operation: ErrorWhile, source: rocksdb::Error) -> Self {
        Self {
            operation,
            source: ErrorSource::RocksDb(source),
        }
    }

    pub(crate) fn spawn_blocking(operation: ErrorWhile, source: tokio::task::JoinError) -> Self {
        Self {
            operation,
            source: ErrorSource::SpawnBlocking(source),
        }
    }
}

#[derive(Debug, derive_more::Display)]
#[non_exhaustive]
pub enum ErrorWhile {
    #[display("Failed opening database at {}", _0.display())]
    OpeningDatabase(PathBuf),
}

#[derive(Debug, thiserror::Error)]
enum ErrorSource {
    #[error("RocksDb call failed")]
    RocksDb(#[source] rocksdb::Error),

    #[error("Failed joining blocking task")]
    SpawnBlocking(#[source] tokio::task::JoinError),
}

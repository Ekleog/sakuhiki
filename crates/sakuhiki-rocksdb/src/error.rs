use std::result::Result as StdResult;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug, thiserror::Error)]
#[error("{kind}")]
pub struct Error {
    kind: ErrorKind,
    source: Option<rocksdb::Error>,
}

impl Error {
    pub(crate) fn simple(kind: ErrorKind) -> Self {
        Self { kind, source: None }
    }

    pub(crate) fn rocksdb(kind: ErrorKind, source: rocksdb::Error) -> Self {
        Self {
            kind,
            source: Some(source),
        }
    }
}

#[derive(Debug, derive_more::Display)]
#[non_exhaustive]
pub enum ErrorKind {
    #[display("CF {_0} does not exist")]
    NoSuchCf(&'static str),
}

//! TODO(med): properly document errors: we return eyre::Report, which can be downcast to:
//! - `Backend::Error` or `Index::Error` if applicable
//! - `CfOperationError` if it was operating on a specific column family
//! - `Error` always to retrieve the backend-abstracted broad category (TODO(med): this is not implemented yet)

use std::fmt;

use crate::Mode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid transaction mode: expected {expected:?}, got {actual:?}")]
    InvalidTransactionMode { expected: Mode, actual: Mode },
}

pub struct CfOperationError {
    msg: &'static str,
    cf: &'static str,
}

impl CfOperationError {
    pub fn new(msg: &'static str, cf: &'static str) -> Self {
        Self { msg, cf }
    }

    pub fn retrieving_cf(cf: &'static str) -> Self {
        Self::new("Failed retrieving CF handle for", cf)
    }

    pub fn cf(&self) -> &'static str {
        self.cf
    }
}

impl fmt::Display for CfOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ‘{}’", self.msg, self.cf)
    }
}

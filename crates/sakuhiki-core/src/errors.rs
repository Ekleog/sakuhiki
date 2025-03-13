use crate::{Backend, Datum};

pub(crate) type IndexErr<B, D> = IndexError<<B as Backend>::Error, <D as Datum>::Error>;

#[derive(Debug, thiserror::Error)]
pub enum IndexError<B, D> {
    // TODO(high): this should take CfError<B> instead of B
    #[error(transparent)]
    Backend(B),

    #[error(transparent)]
    Parsing(D),
}

#[derive(Debug, thiserror::Error)]
#[error("operating on cf {cf:?}")]
pub struct CfError<E> {
    pub cf: &'static str,

    #[source]
    pub error: E,
}

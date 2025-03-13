use crate::{Backend, Datum};

#[derive(Debug, thiserror::Error)]
pub enum IndexError<B, D>
where
    B: Backend,
    D: Datum,
{
    // TODO: improve on this
    #[error(transparent)]
    Backend(B::Error),

    #[error(transparent)]
    Parsing(D::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("operating on cf {cf:?}")]
pub struct CfError<E> {
    pub cf: &'static str,

    #[source]
    pub error: E,
}

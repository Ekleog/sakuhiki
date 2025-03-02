use crate::{Backend, Datum};

#[derive(Debug, thiserror::Error)]
pub enum IndexError<B, D>
where
    B: Backend,
    D: Datum<B>,
{
    // TODO: improve on this
    #[error(transparent)]
    Backend(B::Error),

    #[error(transparent)]
    Parsing(D::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("operating on cf {cf:?}")]
pub struct CfError<E> {
    pub cf: &'static str,

    #[source]
    pub error: E,
}

impl<E> CfError<E> {
    pub fn new(cf: &'static str, error: E) -> Self {
        Self { cf, error }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError<B, D> {
    // TODO(med): this should take CfError<B> instead of B once we store the name inside the *Cf structs
    #[error(transparent)]
    Backend(B),

    #[error(transparent)]
    Parsing(D),
}

impl<B, D> IndexError<B, D> {
    pub fn backend(error: B) -> Self {
        Self::Backend(error)
    }

    pub fn parsing(error: D) -> Self {
        Self::Parsing(error)
    }
}

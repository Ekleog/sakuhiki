use std::fmt;

#[derive(Debug, thiserror::Error)]
pub struct CfError<E> {
    pub cf: Option<&'static str>,

    #[source]
    pub error: E,
}

impl<E> fmt::Display for CfError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cf {
            Some(cf) => {
                f.write_str("Failed operation on cf")?;
                cf.fmt(f)?;
            }
            None => {
                f.write_str("Failed database operation")?;
            }
        }
        Ok(())
    }
}

// TODO(high): figure out a better name, now that the cf is optional
impl<E> CfError<E> {
    pub fn cf(cf: &'static str, error: E) -> Self {
        Self {
            cf: Some(cf),
            error,
        }
    }

    pub fn backend(error: E) -> Self {
        Self { cf: None, error }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError<B, D> {
    #[error(transparent)]
    Backend(CfError<B>),

    #[error(transparent)]
    Parsing(D),
}

impl<B, D> IndexError<B, D> {
    pub fn backend(error: B) -> Self {
        Self::Backend(CfError::backend(error))
    }

    pub fn cf(cf: &'static str, error: B) -> Self {
        Self::Backend(CfError::cf(cf, error))
    }

    pub fn parsing(error: D) -> Self {
        Self::Parsing(error)
    }
}

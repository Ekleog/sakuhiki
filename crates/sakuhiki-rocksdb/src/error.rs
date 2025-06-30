#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("CF {_0} does not exist")]
    NoSuchCf(&'static str),
}

use crate::{Backend, Indexer};

pub trait Datum: 'static + Send + Sync + Sized {
    const CF: &'static str;
    fn from_slice(datum: &[u8]) -> eyre::Result<Self>;
}

pub trait IndexedDatum<B: Backend>: 'static + Send + Sync + Datum {
    const INDEXES: &'static [&'static dyn Indexer<B, Datum = Self>];
}

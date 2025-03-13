use std::{error::Error, fmt::Debug};

use crate::{Backend, Indexer};

pub trait Datum: 'static + Send + Sync + Sized {
    const CF: &'static str;
    type Error: Debug + Error;
    fn from_slice(datum: &[u8]) -> Result<Self, Self::Error>;
}

pub trait IndexedDatum<B: Backend>: 'static + Send + Sync + Datum {
    const INDEXES: &'static [&'static dyn Indexer<B, Datum = Self>];
}

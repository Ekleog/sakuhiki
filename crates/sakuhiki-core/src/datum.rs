use std::{error::Error, fmt::Debug};

use crate::{Backend, Index};

pub trait DatumFromSlice: 'static + Send + Sync + Sized {
    type Error: Debug + Error;

    fn from_slice(datum: &[u8]) -> Result<Self, Self::Error>;
}

pub trait Datum<B: Backend>: 'static + Send + Sync + DatumFromSlice {
    const CF: &'static str;
    const INDICES: &'static [&'static dyn Index<B, Datum = Self>];
}

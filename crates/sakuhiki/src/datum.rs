use std::{error::Error, fmt::Debug};

use crate::{Backend, Index};

pub trait Datum<B: Backend>
where
    Self: 'static + Sized,
{
    type Error: Debug + Error;

    const CF: &'static str;
    const INDICES: &'static [&'static dyn Index<B, Datum = Self>];

    fn from_slice(datum: &[u8]) -> Result<Self, Self::Error>;
}

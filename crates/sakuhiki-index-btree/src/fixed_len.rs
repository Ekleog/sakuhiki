use sakuhiki_core::Datum;

use crate::Key;

pub struct FixedLenKey<D> {
    len: usize,
    extractor: fn(&D, &mut [u8]) -> bool,
}

impl<D> FixedLenKey<D>
where
    D: Datum,
{
    pub const fn new(len: usize, extractor: fn(&D, &mut [u8]) -> bool) -> Self {
        Self { len, extractor }
    }
}

#[warn(clippy::missing_trait_methods)]
impl<D> Key for FixedLenKey<D>
where
    D: Datum,
{
    type Datum = D;

    fn len_hint(&self, _: &D) -> usize {
        self.len
    }

    fn extract_key(&self, datum: &D, key: &mut Vec<u8>) -> bool {
        key.resize(self.len, 0);
        (self.extractor)(datum, &mut key[..])
    }

    fn key_len(&self, in_slice: &[u8]) -> usize {
        debug_assert!(in_slice.len() >= self.len);
        self.len
    }
}

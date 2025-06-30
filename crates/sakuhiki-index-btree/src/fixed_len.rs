use sakuhiki_core::Datum;

use crate::Key;

pub struct FixedLenKey<D>
where
    D: Datum,
{
    len: usize,
    extractor: fn(&D, &mut [u8]) -> bool,
    #[allow(clippy::type_complexity)]
    extractor_from_slice: Option<fn(&[u8], &mut [u8]) -> eyre::Result<bool>>,
}

impl<D> FixedLenKey<D>
where
    D: Datum,
{
    /// `extractor` extracts from `&D` into `&mut [u8]
    #[allow(clippy::type_complexity)]
    pub const fn new(
        len: usize,
        extractor: fn(&D, &mut [u8]) -> bool,
        extractor_from_slice: Option<fn(&[u8], &mut [u8]) -> eyre::Result<bool>>,
    ) -> Self {
        Self {
            len,
            extractor,
            extractor_from_slice,
        }
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
        let len = key.len();
        key.resize(len + self.len, 0);
        (self.extractor)(datum, &mut key[len..])
    }

    fn len_hint_from_slice(&self, _: &[u8]) -> eyre::Result<usize> {
        Ok(self.len)
    }

    fn extract_key_from_slice(&self, slice: &[u8], key: &mut Vec<u8>) -> eyre::Result<bool> {
        let len = key.len();
        key.resize(len + self.len, 0);
        if let Some(extractor_from_slice) = self.extractor_from_slice {
            (extractor_from_slice)(slice, &mut key[len..])
        } else {
            let datum = D::from_slice(slice)?;
            Ok((self.extractor)(&datum, &mut key[len..]))
        }
    }

    fn key_len(&self, in_slice: &[u8]) -> usize {
        debug_assert!(in_slice.len() >= self.len);
        self.len
    }
}

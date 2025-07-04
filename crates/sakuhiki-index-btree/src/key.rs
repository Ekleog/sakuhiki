use eyre::WrapErr as _;
use sakuhiki_core::Datum;

pub trait Key: 'static + waaa::Send + waaa::Sync {
    type Datum: Datum;

    /// Hint about the length of the key that will be extracted from `datum`.
    ///
    /// Used to preallocate capacity in `key` for the right size.
    fn len_hint(&self, datum: &Self::Datum) -> usize;

    /// Returns `true` iff `datum` must be part of the index.
    fn extract_key(&self, datum: &Self::Datum, key: &mut Vec<u8>) -> bool;

    /// Hint about the length of the key that will be extracted from `datum`.
    ///
    /// Used to preallocate capacity in `key` for the right size.
    fn len_hint_from_slice(&self, datum: &[u8]) -> eyre::Result<usize> {
        // TODO(med): should maybe replace most of the custom messages with proper variants of a sakuhiki_core::Error enum?
        let datum = Self::Datum::from_slice(datum).wrap_err("Failed to parse datum")?;
        Ok(self.len_hint(&datum))
    }

    /// Returns `true` iff `datum` must be part of the index.
    fn extract_key_from_slice(&self, datum: &[u8], key: &mut Vec<u8>) -> eyre::Result<bool> {
        let datum = Self::Datum::from_slice(datum).wrap_err("Failed to parse datum")?;
        Ok(self.extract_key(&datum, key))
    }

    /// Returns the length of the key in the `in_slice` slice.
    ///
    /// The actual key is a prefix of `in_slice`, and this function must return thelength it
    /// occupies.
    fn key_len(&self, in_slice: &[u8]) -> usize;
}

use crate::{Backend, Datum, IndexError};

pub trait Indexer<B: Backend>: waaa::Send + waaa::Sync {
    type Datum: Datum;

    fn cfs(&self) -> &'static [&'static str];

    /// Estimation of the length of the index key.
    ///
    /// Will be used to preallocate capacity in `index_key_prefix` for the right size.
    fn index_key_len(&self, object_key: &[u8], datum: &Self::Datum) -> usize;

    fn index<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
        index_key_prefix: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
        index_key_prefix: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    fn index_key_len_from_slice(
        &self,
        object_key: &[u8],
        slice: &[u8],
    ) -> Result<usize, <Self::Datum as Datum>::Error> {
        let datum = Self::Datum::from_slice(slice)?;
        Ok(self.index_key_len(object_key, &datum))
    }

    fn index_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
        index_key_prefix: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.index(object_key, &datum, transaction, cf, index_key_prefix)
                .await
                .map_err(IndexError::Backend)
        })
    }

    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
        index_key_prefix: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.unindex(object_key, &datum, transaction, cf, index_key_prefix)
                .await
                .map_err(IndexError::Backend)
        })
    }
}

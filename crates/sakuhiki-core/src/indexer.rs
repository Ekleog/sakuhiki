use crate::{Backend, Datum, IndexError};

pub trait Indexer<B: Backend>: waaa::Send + waaa::Sync {
    type Datum: Datum;

    fn cfs(&self) -> &'static [&'static str];

    fn index<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::RwTransaction<'t>,
        cfs: &'fut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::RwTransaction<'t>,
        cfs: &'fut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    #[allow(clippy::type_complexity)] // No good way to cut this smaller
    fn index_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::RwTransaction<'t>,
        cfs: &'fut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, <Self::Datum as Datum>::Error>>>
    {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.index(object_key, &datum, transaction, cfs)
                .await
                .map_err(|e| IndexError::backend("", e)) // TODO(med): use proper cf name here
        })
    }

    #[allow(clippy::type_complexity)] // No good way to cut this smaller
    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::RwTransaction<'t>,
        cfs: &'fut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, <Self::Datum as Datum>::Error>>>
    {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.unindex(object_key, &datum, transaction, cfs)
                .await
                .map_err(|e| IndexError::backend("", e)) // TODO(med): use proper cf name here
        })
    }
}

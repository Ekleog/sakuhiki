use crate::{Backend, Datum, IndexError, errors::IndexErr};

pub trait Indexer<B: Backend>: waaa::Send + waaa::Sync {
    type Datum: Datum;

    fn cfs(&self) -> &'static [&'static str];

    fn index<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cfs: &'fut mut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cfs: &'fut mut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    fn index_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cfs: &'fut mut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), IndexErr<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.index(object_key, &datum, transaction, cfs)
                .await
                .map_err(IndexError::Backend)
        })
    }

    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cfs: &'fut mut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), IndexErr<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.unindex(object_key, &datum, transaction, cfs)
                .await
                .map_err(IndexError::Backend)
        })
    }
}

use crate::{Backend, Datum, IndexError, IndexedDatum};

pub trait Index<B: Backend>: waaa::Send + waaa::Sync {
    type Datum: IndexedDatum<B>;

    fn cfs(&self) -> &'static [&'static str];

    fn index<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    fn unindex<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>>;

    fn index_from_slice<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.index(key, &datum, transaction, cf)
                .await
                .map_err(IndexError::Backend)
        })
    }

    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.unindex(key, &datum, transaction, cf)
                .await
                .map_err(IndexError::Backend)
        })
    }
}

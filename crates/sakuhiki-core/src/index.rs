use std::pin::Pin;

use crate::{Backend, Datum, IndexError};

type DynFuture<'fut, T> = Pin<Box<dyn 'fut + Future<Output = T>>>;

pub trait Index<B: Backend> {
    type Datum: Datum<B>;

    fn cf(&self) -> &'static str;

    fn index<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> DynFuture<'fut, Result<(), B::Error>>;

    fn unindex<'t>(
        &self,
        key: &[u8],
        datum: &Self::Datum,
        transaction: &mut B::RwTransaction<'t>,
        cf: &mut B::RwTransactionCf<'t>,
    ) -> Result<(), B::Error>;

    fn index_from_slice<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> DynFuture<'fut, Result<(), IndexError<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.index(key, &datum, transaction, cf)
                .await
                .map_err(IndexError::Backend)
        })
    }

    fn unindex_from_slice<'t>(
        &self,
        key: &[u8],
        slice: &[u8],
        transaction: &mut B::RwTransaction<'t>,
        cf: &mut B::RwTransactionCf<'t>,
    ) -> Result<(), IndexError<B, Self::Datum>> {
        let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
        self.unindex(key, &datum, transaction, cf)
            .map_err(IndexError::Backend)
    }
}

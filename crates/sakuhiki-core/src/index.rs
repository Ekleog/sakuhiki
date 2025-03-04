use std::pin::Pin;

use crate::{Backend, Datum, IndexError};

// TODO: replace with waaa::BoxFuture once rustc stops giving this error message:
// note: this is a known limitation that will be removed in the future (see issue #100013
// <https://github.com/rust-lang/rust/issues/100013> for more information)
pub type DynFuture<'fut, T> = Pin<Box<dyn 'fut + Future<Output = T>>>;

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

    fn unindex<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> DynFuture<'fut, Result<(), B::Error>>;

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

    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> DynFuture<'fut, Result<(), IndexError<B, Self::Datum>>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.unindex(key, &datum, transaction, cf)
                .await
                .map_err(IndexError::Backend)
        })
    }
}

use futures_util::StreamExt as _;

use crate::{
    Backend, CfError, Datum, IndexError,
    backend::{BackendCf as _, Transaction as _},
};

pub trait Indexer<B: Backend>: Send + Sync {
    type Datum: Datum;

    fn cfs(&self) -> &'static [&'static str];

    fn index<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), CfError<B::Error>>>;

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), CfError<B::Error>>>;

    #[allow(clippy::type_complexity)] // No good way to cut this smaller
    fn index_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, <Self::Datum as Datum>::Error>>>
    {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.index(object_key, &datum, transaction, cfs)
                .await
                .map_err(IndexError::Backend)
        })
    }

    #[allow(clippy::type_complexity)] // No good way to cut this smaller
    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, <Self::Datum as Datum>::Error>>>
    {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
            self.unindex(object_key, &datum, transaction, cfs)
                .await
                .map_err(IndexError::Backend)
        })
    }

    #[allow(clippy::type_complexity)]
    fn rebuild<'fut, 't>(
        &'fut self,
        transaction: &'fut B::Transaction<'t>,
        index_cfs: &'fut [B::TransactionCf<'t>],
        datum_cf: &'fut B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, <Self::Datum as Datum>::Error>>>
    {
        Box::pin(async move { default_rebuild(self, transaction, index_cfs, datum_cf).await })
    }
}

pub async fn default_rebuild<'fut, 't, B, I>(
    this: &'fut I,
    transaction: &'fut B::Transaction<'t>,
    index_cfs: &'fut [B::TransactionCf<'t>],
    datum_cf: &'fut B::TransactionCf<'t>,
) -> Result<(), IndexError<B::Error, <I::Datum as Datum>::Error>>
where
    B: Backend,
    I: ?Sized + Indexer<B>,
{
    // Dark magic to work around https://github.com/rust-lang/rust/issues/100013
    // See https://github.com/rust-lang/rust/issues/100013#issuecomment-2210995259 for the inspiration
    // This should be removed someday when rustc gets fixed
    fn assert_send<T: Send>(v: T) -> impl Send {
        v
    }
    let _lock = assert_send(
        transaction
            .take_exclusive_lock(datum_cf)
            .await
            .map_err(|e| IndexError::Backend(CfError::new(datum_cf.name(), e)))?,
    );
    for cf in index_cfs {
        transaction
            .clear(cf)
            .await
            .map_err(|e| IndexError::Backend(CfError::new(cf.name(), e)))?;
    }
    let mut all_data = transaction.scan::<[u8]>(datum_cf, ..);
    while let Some(d) = all_data.next().await {
        let (key, datum) = d.map_err(|e| IndexError::Backend(CfError::new(datum_cf.name(), e)))?;
        this.index_from_slice(key.as_ref(), datum.as_ref(), transaction, index_cfs)
            .await?;
    }
    Ok(())
}

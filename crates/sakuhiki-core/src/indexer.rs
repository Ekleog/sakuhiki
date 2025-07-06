use eyre::WrapErr as _;
use futures_util::StreamExt as _;

use crate::{
    Backend, CfOperationError, Datum, Error, Mode,
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
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>>;

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>>;

    fn index_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).wrap_err("Failed to parse datum")?;
            self.index(object_key, &datum, transaction, cfs)
                .await
                .wrap_err("Failed to index datum")
        })
    }

    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
        Box::pin(async move {
            let datum = Self::Datum::from_slice(slice).wrap_err("Failed to parse datum")?;
            self.unindex(object_key, &datum, transaction, cfs)
                .await
                .wrap_err("Failed to unindex datum")
        })
    }

    fn rebuild<'fut, 't>(
        &'fut self,
        transaction: &'fut B::Transaction<'t>,
        index_cfs: &'fut [B::TransactionCf<'t>],
        datum_cf: &'fut B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
        Box::pin(async move { default_rebuild(self, transaction, index_cfs, datum_cf).await })
    }
}

pub async fn default_rebuild<'fut, 't, B, I>(
    this: &'fut I,
    transaction: &'fut B::Transaction<'t>,
    index_cfs: &'fut [B::TransactionCf<'t>],
    datum_cf: &'fut B::TransactionCf<'t>,
) -> eyre::Result<()>
where
    B: Backend,
    I: ?Sized + Indexer<B>,
{
    if transaction.current_mode() != Mode::IndexRebuilding {
        return Err(eyre::Report::from(Error::InvalidTransactionMode {
            expected: Mode::IndexRebuilding,
            actual: transaction.current_mode(),
        }));
    }
    for cf in index_cfs {
        transaction
            .clear(cf)
            .await
            .wrap_err_with(|| CfOperationError::new("Failed clearing", cf.name()))?;
    }
    let mut all_data = transaction.scan::<[u8]>(datum_cf, ..);
    while let Some(d) = all_data.next().await {
        let (key, datum) =
            d.wrap_err_with(|| CfOperationError::new("Failed scanning through", datum_cf.name()))?;
        let key = key.as_ref();
        let datum = datum.as_ref();
        this.index_from_slice(key, datum, transaction, index_cfs)
            .await
            .wrap_err_with(|| format!("Failed indexing {key:?}/{datum:?}"))?;
    }
    Ok(())
}

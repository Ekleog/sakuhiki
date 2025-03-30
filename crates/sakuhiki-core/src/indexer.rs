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

    fn rebuild<'fut, 't>(
        &'fut self,
        transaction: &'fut B::Transaction<'t>,
        index_cfs: &'fut [B::TransactionCf<'t>],
        datum_cf: &'fut B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, anyhow::Error>>> {
        Box::pin(async move {
            for cf in index_cfs {
                transaction
                    .clear(cf)
                    .await
                    .map_err(|e| IndexError::Backend(CfError::new(cf.name(), e)))?;
            }
            let mut all_data = transaction.scan::<[u8]>(datum_cf, ..);
            while let Some(d) = all_data.next().await {
                let (key, datum) =
                    d.map_err(|e| IndexError::Backend(CfError::new(datum_cf.name(), e)))?;
                self.index_from_slice(key.as_ref(), datum.as_ref(), transaction, index_cfs)
                    .await
                    .map_err(|e| match e {
                        IndexError::Backend(e) => IndexError::Backend(e),
                        IndexError::Parsing(e) => IndexError::Parsing(
                            anyhow::Error::from(e)
                                .context(format!("parsing data from cf '{}'", datum_cf.name())),
                        ),
                    })?;
            }
            Ok(())
        })
    }
}

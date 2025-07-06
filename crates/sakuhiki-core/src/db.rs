use std::{collections::VecDeque, iter, ops::RangeBounds};
// TODO(blocked): use AsyncFn everywhere possible, once its return future can be marked Send/Sync

use eyre::WrapErr as _;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use waaa::Stream;

use crate::{
    Backend, CfOperationError, Datum, IndexedDatum, Indexer, Mode,
    backend::{BackendCf as _, Transaction as _},
};

pub struct Db<B> {
    backend: B,
}

impl<B> Db<B>
where
    B: Backend,
{
    pub fn new(backend: B) -> Db<B> {
        Db { backend }
    }

    /// Rebuild an index from scratch.
    ///
    /// This can help recover from data corruption.
    pub async fn rebuild_index<I: Indexer<B>>(&self, index: &'static I) -> eyre::Result<()> {
        let mut all_cfs = stream::iter(index.cfs())
            .then(|cf| async move {
                self.backend
                    .cf_handle(cf)
                    .await
                    .wrap_err_with(|| CfOperationError::retrieving_cf(cf))
            })
            .try_collect::<Vec<_>>()
            .await?;
        all_cfs.push(
            self.backend
                .cf_handle(I::Datum::CF)
                .await
                .wrap_err_with(|| CfOperationError::retrieving_cf(I::Datum::CF))?,
        );
        let all_cfs = all_cfs.iter().collect::<Vec<_>>();
        self.backend
            .transaction(Mode::ReadWrite, &all_cfs, move |_, t, mut cfs| {
                let datum_cf = cfs.pop().unwrap();
                let index_cfs = cfs;
                Box::pin(async move { index.rebuild(&t, &index_cfs, &datum_cf).await })
            })
            .await
            .wrap_err("Failed running index rebuilding transaction")?
    }

    pub async fn cf_handle<D>(&self) -> eyre::Result<Cf<'_, B>>
    where
        D: IndexedDatum<B>,
    {
        Ok(Cf {
            datum_cf: self
                .backend
                .cf_handle(D::CF)
                .await
                .wrap_err_with(|| CfOperationError::retrieving_cf(D::CF))?,
            indexes_cfs: stream::iter(D::INDEXES)
                .then(|i| {
                    stream::iter(i.cfs())
                        .then(async |cf| {
                            self.backend
                                .cf_handle(cf)
                                .await
                                .wrap_err_with(|| CfOperationError::retrieving_cf(cf))
                        })
                        .try_collect()
                })
                .try_collect()
                .await?,
        })
    }

    pub async fn transaction<'fut, const CFS: usize, F, Ret>(
        &'fut self,
        mode: Mode,
        cfs: &'fut [&'fut Cf<'fut, B>; CFS],
        actions: F,
    ) -> eyre::Result<Ret>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(
                Transaction<'t, B>,
                [TransactionCf<'t, B>; CFS],
            ) -> waaa::BoxFuture<'t, Ret>,
    {
        let backend_cfs = cfs
            .iter()
            .flat_map(|cf| {
                iter::once(&cf.datum_cf).chain(cf.indexes_cfs.iter().flat_map(|v| v.iter()))
            })
            .collect::<Vec<_>>();
        let num_backend_cfs = backend_cfs.len();
        self.backend
            .transaction(mode, &backend_cfs, move |_, transaction, backend_cfs| {
                debug_assert!(num_backend_cfs == backend_cfs.len());
                let mut backend_cfs = VecDeque::from(backend_cfs);
                let mut frontend_cfs = Vec::with_capacity(CFS);
                for cf in cfs {
                    let datum_cf = backend_cfs.pop_front().unwrap();
                    let mut indexes_cfs = Vec::with_capacity(cf.indexes_cfs.len());
                    for i in cf.indexes_cfs.iter() {
                        indexes_cfs.push(backend_cfs.drain(0..i.len()).collect());
                    }
                    frontend_cfs.push(TransactionCf {
                        datum_cf,
                        indexes_cfs,
                    });
                }
                debug_assert!(backend_cfs.is_empty());
                let Ok(frontend_cfs) = frontend_cfs.try_into() else {
                    unreachable!("unexpected number of cfs");
                };
                actions(Transaction { transaction }, frontend_cfs)
            })
            .await
    }
}

pub struct Cf<'db, B>
where
    B: Backend,
{
    datum_cf: B::Cf<'db>,
    indexes_cfs: Vec<Vec<B::Cf<'db>>>,
}

pub struct Transaction<'t, B>
where
    B: 't + Backend,
{
    transaction: B::Transaction<'t>,
}

pub struct TransactionCf<'t, B>
where
    B: Backend,
{
    datum_cf: B::TransactionCf<'t>,
    indexes_cfs: Vec<Vec<B::TransactionCf<'t>>>,
}

impl<'t, B> Transaction<'t, B>
where
    B: Backend,
{
    pub async fn get<'op, 'key>(
        &'op self,
        cf: &'op TransactionCf<'t, B>,
        key: &'key [u8],
    ) -> eyre::Result<Option<B::Value<'op>>> {
        self.transaction.get(&cf.datum_cf, key).await
    }

    pub fn scan<'op, 'keys, Keys, R>(
        &'op self,
        cf: &'op TransactionCf<'t, B>,
        keys: Keys,
    ) -> impl Stream<Item = eyre::Result<(B::Key<'op>, B::Value<'op>)>> + use<'t, 'op, 'keys, B, Keys, R>
    where
        'op: 'keys,
        Keys: 'keys + RangeBounds<R>,
        R: ?Sized + AsRef<[u8]>,
    {
        self.transaction.scan(&cf.datum_cf, keys)
    }

    // TODO(med): rename into put_slice, add put
    // TODO(med): add sanity-check that the provided cf is the right one for D indeed, and same everywhere else
    pub async fn put<'op, 'kv, D>(
        &'op self,
        cf: &'op TransactionCf<'t, B>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> eyre::Result<Option<B::Value<'op>>>
    where
        D: IndexedDatum<B>,
    {
        debug_assert!(D::INDEXES.len() == cf.indexes_cfs.len());
        let old = self
            .transaction
            .put(&cf.datum_cf, key, value)
            .await
            .wrap_err_with(|| {
                CfOperationError::new("Failed putting value into", cf.datum_cf.name())
            })?;
        for (i, cfs) in D::INDEXES.iter().zip(cf.indexes_cfs.iter()) {
            if let Some(old) = &old {
                i.unindex_from_slice(key, old.as_ref(), &self.transaction, cfs)
                    .await
                    .wrap_err("Failed unindexing old value")?;
            }
            i.index_from_slice(key, value, &self.transaction, cfs)
                .await
                .wrap_err("Failed indexing new value")?;
        }
        Ok(old)
    }

    pub async fn delete<'op, 'key, D>(
        &'op self,
        cf: &'op TransactionCf<'t, B>,
        key: &'key [u8],
    ) -> eyre::Result<Option<B::Value<'op>>>
    where
        D: IndexedDatum<B>,
    {
        debug_assert!(D::INDEXES.len() == cf.indexes_cfs.len());
        let old = self
            .transaction
            .delete(&cf.datum_cf, key)
            .await
            .wrap_err_with(|| CfOperationError::new("Failed deleting from", cf.datum_cf.name()))?;
        if let Some(old) = &old {
            for (i, cfs) in D::INDEXES.iter().zip(cf.indexes_cfs.iter()) {
                i.unindex_from_slice(key, old.as_ref(), &self.transaction, cfs)
                    .await
                    .wrap_err("Failed unindexing old value")?;
            }
        }
        Ok(old)
    }
}

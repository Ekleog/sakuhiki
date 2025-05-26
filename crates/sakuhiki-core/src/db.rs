use std::{collections::VecDeque, iter, ops::RangeBounds};
// TODO(blocked): use AsyncFn everywhere possible, once its return future can be marked Send/Sync

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use waaa::Stream;

use crate::{
    Backend, CfError, Datum, IndexError, IndexedDatum, Indexer,
    backend::{BackendCf as _, Transaction as _},
};

pub struct Db<B> {
    backend: B,
}

macro_rules! make_transaction_fn {
    ($name:ident) => {
        pub async fn $name<'fut, const CFS: usize, F, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Cf<'fut, B>; CFS],
            actions: F,
        ) -> Result<Ret, CfError<B::Error>>
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
                .$name(&backend_cfs, move |_, transaction, backend_cfs| {
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
                        panic!("unexpected number of cfs");
                    };
                    actions(Transaction { transaction }, frontend_cfs)
                })
                .await
        }
    };
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
    pub async fn rebuild_index<I: Indexer<B>>(
        &self,
        index: &'static I,
    ) -> Result<(), IndexError<B::Error, <I::Datum as Datum>::Error>> {
        let mut all_cfs = stream::iter(index.cfs())
            .then(|cf| async move {
                self.backend
                    .cf_handle(cf)
                    .await
                    .map_err(|e| IndexError::Backend(CfError::cf(cf, e)))
            })
            .try_collect::<Vec<_>>()
            .await?;
        all_cfs.push(
            self.backend
                .cf_handle(I::Datum::CF)
                .await
                .map_err(|e| IndexError::Backend(CfError::cf(I::Datum::CF, e)))?,
        );
        let all_cfs = all_cfs.iter().collect::<Vec<_>>();
        self.backend
            .rw_transaction(&all_cfs, move |_, t, mut cfs| {
                let datum_cf = cfs.pop().unwrap();
                let index_cfs = cfs;
                Box::pin(async move { index.rebuild(&t, &index_cfs, &datum_cf).await })
            })
            .await
            .map_err(IndexError::Backend)?
    }

    pub async fn cf_handle<D>(&self) -> Result<Cf<'_, B>, CfError<B::Error>>
    where
        D: IndexedDatum<B>,
    {
        Ok(Cf {
            datum_cf: self
                .backend
                .cf_handle(D::CF)
                .await
                .map_err(|e| CfError::cf(D::CF, e))?,
            indexes_cfs: stream::iter(D::INDEXES)
                .then(|i| {
                    stream::iter(i.cfs())
                        .then(async |cf| {
                            self.backend
                                .cf_handle(cf)
                                .await
                                .map_err(|error| CfError::cf(cf, error))
                        })
                        .try_collect()
                })
                .try_collect()
                .await?,
        })
    }

    make_transaction_fn!(ro_transaction);
    make_transaction_fn!(rw_transaction);
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
    ) -> Result<Option<B::Value<'op>>, B::Error> {
        self.transaction.get(&cf.datum_cf, key).await
    }

    pub fn scan<'op, 'keys, Keys, R>(
        &'op self,
        cf: &'op TransactionCf<'t, B>,
        keys: Keys,
    ) -> impl Stream<Item = Result<(B::Key<'op>, B::Value<'op>), B::Error>>
    + use<'t, 'op, 'keys, B, Keys, R>
    where
        'op: 'keys,
        Keys: 'keys + RangeBounds<R>,
        R: ?Sized + AsRef<[u8]>,
    {
        self.transaction.scan(&cf.datum_cf, keys)
    }

    // TODO(med): rename into put_slice, add put
    pub async fn put<'op, 'kv, D>(
        &'op self,
        cf: &'op TransactionCf<'t, B>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> Result<Option<B::Value<'op>>, IndexError<B::Error, D::Error>>
    where
        D: IndexedDatum<B>,
    {
        debug_assert!(D::INDEXES.len() == cf.indexes_cfs.len());
        let old = self
            .transaction
            .put(&cf.datum_cf, key, value)
            .await
            .map_err(|e| IndexError::cf(cf.datum_cf.name(), e))?;
        for (i, cfs) in D::INDEXES.iter().zip(cf.indexes_cfs.iter()) {
            if let Some(old) = &old {
                i.unindex_from_slice(key, old.as_ref(), &self.transaction, cfs)
                    .await?;
            }
            i.index_from_slice(key, value, &self.transaction, cfs)
                .await?;
        }
        Ok(old)
    }

    pub async fn delete<'op, 'key, D>(
        &'op self,
        cf: &'op TransactionCf<'t, B>,
        key: &'key [u8],
    ) -> Result<Option<B::Value<'op>>, IndexError<B::Error, D::Error>>
    where
        D: IndexedDatum<B>,
    {
        debug_assert!(D::INDEXES.len() == cf.indexes_cfs.len());
        let old = self
            .transaction
            .delete(&cf.datum_cf, key)
            .await
            .map_err(|e| IndexError::cf(cf.datum_cf.name(), e))?;
        if let Some(old) = &old {
            for (i, cfs) in D::INDEXES.iter().zip(cf.indexes_cfs.iter()) {
                i.unindex_from_slice(key, old.as_ref(), &self.transaction, cfs)
                    .await?;
            }
        }
        Ok(old)
    }
}

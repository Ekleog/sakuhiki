use std::{collections::VecDeque, future, iter, ops::RangeBounds};
// TODO(blocked): use AsyncFn everywhere possible, once its return future can be marked Send/Sync

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use waaa::Stream;

use crate::{
    Backend, CfError, IndexError, IndexedDatum, Indexer,
    backend::{BackendBuilder, BackendCf as _, CfBuilder, Transaction as _},
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
        ) -> Result<Ret, B::Error>
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
    // This is unsafe because it can lead to data corruption if there's
    // another writer in parallel with the index rebuilding.
    pub async unsafe fn rebuild_index<I: Indexer<B>>(
        &self,
        _index: &I,
    ) -> Result<(), IndexError<B, I::Datum>> {
        todo!() // TODO(high): implement index rebuilding
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
                .map_err(|error| CfError { cf: D::CF, error })?,
            indexes_cfs: stream::iter(D::INDEXES)
                .then(|i| {
                    stream::iter(i.cfs())
                        .then(async |cf| {
                            self.backend
                                .cf_handle(cf)
                                .await
                                .map_err(|error| CfError { cf, error })
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

pub struct DbBuilder<Builder>
where
    Builder: BackendBuilder,
{
    builder: Builder,
    cf_builder_list: Vec<CfBuilder<<Builder as BackendBuilder>::Target>>,
}

impl<Builder> DbBuilder<Builder>
where
    Builder: BackendBuilder,
{
    // See Backend::builder()
    pub fn new(builder: Builder) -> Self {
        Self {
            builder,
            cf_builder_list: Vec::new(),
        }
    }

    pub fn config(mut self, f: impl FnOnce(&mut Builder)) -> Self {
        f(&mut self.builder);
        self
    }

    // TODO(med): add a way to kill all CFs that are not listed in one of the datum calls
    pub fn datum<D: IndexedDatum<Builder::Target>>(mut self) -> Self {
        self.cf_builder_list.push(CfBuilder {
            cfs: vec![D::CF],
            builds_using: None,
            builder: Box::new(|_, _, _, _| Box::pin(future::ready(Ok(())))),
        });
        for index in D::INDEXES {
            self.cf_builder_list.push(CfBuilder {
                cfs: index.cfs().to_owned(),
                builds_using: Some(D::CF),
                builder: Box::new(|_, t, index_cfs, datum_cf| {
                    let datum_cf = datum_cf.unwrap();
                    let _ = (t, index_cfs, datum_cf);
                    todo!() // TODO(high): implement index initial build
                }),
            })
        }
        self
    }

    // Note: while a new index is building, no new data should be added!
    // This is especially important for eg. tikv that could have multiple concurrent clients.
    // TODO(med): figure out a way to lock writes while indexes are rebuilding
    // (eg. get_for_update exclusive=false on a metadata cf and exclusive=true when rebuilding the index)
    pub async fn build(
        self,
    ) -> Result<Db<Builder::Target>, CfError<<Builder::Target as Backend>::Error>> {
        Ok(Db {
            backend: self.builder.build(self.cf_builder_list).await?,
        })
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
            .map_err(|e| IndexError::backend(cf.datum_cf.name(), e))?;
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
            .map_err(|e| IndexError::backend(cf.datum_cf.name(), e))?;
        if let Some(old) = &old {
            for (i, cfs) in D::INDEXES.iter().zip(cf.indexes_cfs.iter()) {
                i.unindex_from_slice(key, old.as_ref(), &self.transaction, cfs)
                    .await?;
            }
        }
        Ok(old)
    }
}

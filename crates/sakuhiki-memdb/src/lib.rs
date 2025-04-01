use std::{
    collections::BTreeMap,
    future::{self, Ready, ready},
    ops::{Bound, RangeBounds},
    sync::Mutex,
};

use async_lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use futures_util::stream;
use sakuhiki_core::{
    Backend, CfError, Datum, Db, IndexError,
    backend::{BackendBuilder, BackendCf},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Column family does not exist in memory database")]
    NonExistentColumnFamily,
}

type ColumnFamily = BTreeMap<Vec<u8>, Vec<u8>>;

pub struct TransactionCf<'t> {
    cf: Mutex<AsyncMutexGuard<'t, ColumnFamily>>,
    name: &'static str,
}

impl BackendCf for TransactionCf<'_> {
    fn name(&self) -> &'static str {
        self.name
    }
}

pub struct MemDb {
    db: BTreeMap<String, AsyncMutex<ColumnFamily>>,
}

impl MemDb {
    pub fn new() -> MemDb {
        MemDb {
            db: BTreeMap::new(),
        }
    }

    pub fn builder() -> MemDbBuilder {
        MemDbBuilder::new()
    }
}

impl Default for MemDb {
    fn default() -> MemDb {
        MemDb::new()
    }
}

#[warn(clippy::missing_trait_methods)]
impl Backend for MemDb {
    type Error = Error;

    type Builder = MemDbBuilder;

    type Cf<'db> = &'static str;

    type CfHandleFuture<'op> = Ready<Result<Self::Cf<'op>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db> {
        ready(Ok(name))
    }

    type Transaction<'t> = Transaction;
    type TransactionCf<'t> = TransactionCf<'t>;

    fn ro_transaction<'fut, 'db, F, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, CfError<Self::Error>>>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(&'t (), Transaction, Vec<TransactionCf<'t>>) -> waaa::BoxFuture<'t, Ret>,
    {
        Box::pin(async move {
            let t = Transaction { _private: () };
            let mut cfs = cfs.iter().enumerate().collect::<Vec<_>>();
            cfs.sort_by_key(|e| e.1);
            let mut transaction_cfs = Vec::with_capacity(cfs.len());
            for (i, &name) in cfs {
                let cf = self
                    .db
                    .get(*name)
                    .ok_or_else(|| CfError::new(name, Error::NonExistentColumnFamily))?;
                let cf = Mutex::new(cf.lock().await);
                transaction_cfs.push((i, TransactionCf { name, cf }));
            }
            transaction_cfs.sort_by_key(|e| e.0);
            let transaction_cfs = transaction_cfs
                .into_iter()
                .map(|(_, cf)| cf)
                .collect::<Vec<_>>();
            Ok(actions(&(), t, transaction_cfs).await)
        })
    }

    fn rw_transaction<'fut, 'db, F, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, CfError<Self::Error>>>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(
                &'t (),
                Self::Transaction<'t>,
                Vec<Self::TransactionCf<'t>>,
            ) -> waaa::BoxFuture<'t, Ret>,
    {
        self.ro_transaction(cfs, actions)
    }

    type Key<'op> = Vec<u8>;
    type Value<'op> = Vec<u8>;
}

pub struct Transaction {
    _private: (),
}

// #[warn(clippy::missing_trait_methods)] // MemDb is used only for tests, we can use default impls
impl<'t> sakuhiki_core::backend::Transaction<'t, MemDb> for Transaction {
    type ExclusiveLock<'op>
        = ()
    where
        't: 'op;

    fn take_exclusive_lock<'op>(
        &'op self,
        _cf: &'op <MemDb as Backend>::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, Result<Self::ExclusiveLock<'op>, <MemDb as Backend>::Error>>
    where
        't: 'op,
    {
        // MemDb already locks literally all the CFs when starting the transaction anyway
        Box::pin(future::ready(Ok(())))
    }

    fn get<'op, 'key>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<Vec<u8>>, Error>>
    where
        'op: 'key,
    {
        Box::pin(ready(Ok(cf
            .cf
            .lock()
            .unwrap()
            .get(key)
            .map(|v| v.to_owned()))))
    }

    fn scan<'op, 'keys, R>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        keys: impl 'keys + RangeBounds<R>,
    ) -> waaa::BoxStream<'keys, Result<(Vec<u8>, Vec<u8>), Error>>
    where
        't: 'op,
        'op: 'keys,
        R: ?Sized + AsRef<[u8]>,
    {
        let start: Bound<&[u8]> = keys.start_bound().map(|k| k.as_ref());
        let end: Bound<&[u8]> = keys.end_bound().map(|k| k.as_ref());
        Box::pin(stream::iter(
            cf.cf
                .lock()
                .unwrap()
                .range::<[u8], _>((start, end))
                .map(|(k, v)| Ok((k.to_owned(), v.to_owned())))
                .collect::<Vec<_>>(),
        ))
    }

    fn put<'op, 'kv>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> waaa::BoxFuture<'kv, Result<Option<Vec<u8>>, Error>>
    where
        't: 'op,
        'op: 'kv,
    {
        let data = cf.cf.lock().unwrap().insert(key.to_vec(), value.to_vec());
        Box::pin(ready(Ok(data)))
    }

    fn delete<'op, 'key>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<Vec<u8>>, Error>>
    where
        't: 'op,
        'op: 'key,
    {
        let data = cf.cf.lock().unwrap().remove(key);
        Box::pin(ready(Ok(data)))
    }

    fn clear<'op>(
        &'op self,
        cf: &'op <MemDb as Backend>::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, Result<(), <MemDb as Backend>::Error>> {
        cf.cf.lock().unwrap().clear();
        Box::pin(ready(Ok(())))
    }
}

pub struct MemDbBuilder {
    db: MemDb,
}

impl MemDbBuilder {
    pub fn new() -> MemDbBuilder {
        MemDbBuilder { db: MemDb::new() }
    }
}

impl Default for MemDbBuilder {
    fn default() -> MemDbBuilder {
        MemDbBuilder::new()
    }
}

impl BackendBuilder for MemDbBuilder {
    type Target = MemDb;

    fn build_datum_cf(
        mut self,
        cf: &'static str,
    ) -> waaa::BoxFuture<'static, Result<Self, <Self::Target as Backend>::Error>> {
        self.db
            .db
            .insert(cf.to_string(), AsyncMutex::new(ColumnFamily::new()));
        Box::pin(future::ready(Ok(self)))
    }

    fn build_index_cf<I: ?Sized + sakuhiki_core::Indexer<Self::Target>>(
        mut self,
        index: &I,
    ) -> waaa::BoxFuture<
        '_,
        Result<Self, IndexError<<Self::Target as Backend>::Error, <I::Datum as Datum>::Error>>,
    > {
        let index_cf_names = index.cfs();
        if self.db.db.contains_key(index_cf_names[0]) {
            assert!(index_cf_names.iter().all(|cf| self.db.db.contains_key(*cf))); // TODO(med): will be interesting to make this wrong for comparative fuzz-testing
            return Box::pin(future::ready(Ok(self)));
        }
        for cf in index_cf_names {
            self.db
                .db
                .insert(cf.to_string(), AsyncMutex::new(ColumnFamily::new()));
        }

        let datum_cf_name = <I::Datum as Datum>::CF;

        Box::pin(async move {
            {
                let mut index_cfs = Vec::with_capacity(index_cf_names.len());
                for cf in index_cf_names {
                    index_cfs.push(TransactionCf {
                        name: cf,
                        cf: Mutex::new(self.db.db.get(*cf).unwrap().lock().await),
                    });
                }
                let datum_cf = TransactionCf {
                    name: datum_cf_name,
                    cf: Mutex::new(self.db.db.get(datum_cf_name).unwrap().lock().await),
                };
                index
                    .rebuild(&Transaction { _private: () }, &index_cfs, &datum_cf)
                    .await?;
            }
            Ok(self)
        })
    }

    fn drop_unknown_cfs(self) -> waaa::BoxFuture<'static, Result<MemDbBuilder, Error>> {
        todo!() // TODO(med): not implemented in memdb yet, will be useful for comparative fuzz-testing later
    }

    type BuildFuture =
        waaa::BoxFuture<'static, Result<Db<Self::Target>, IndexError<Error, anyhow::Error>>>;

    fn build(self) -> Self::BuildFuture {
        Box::pin(future::ready(Ok(Db::new(self.db))))
    }
}

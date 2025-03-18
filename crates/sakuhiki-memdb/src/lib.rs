use std::{
    collections::BTreeMap,
    future::{Ready, ready},
    ops::RangeBounds,
    sync::Mutex,
};

use async_lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use futures_util::stream;
use sakuhiki_core::backend::BackendCf;

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

impl Default for MemDb {
    fn default() -> Self {
        Self::new()
    }
}

impl MemDb {
    pub fn new() -> Self {
        Self {
            db: BTreeMap::new(),
        }
    }

    pub fn create_cf(&mut self, cf: &'static str) {
        self.db
            .insert(cf.to_string(), AsyncMutex::new(ColumnFamily::new()));
    }
}

impl sakuhiki_core::Backend for MemDb {
    type Error = Error;

    type Key<'op> = Vec<u8>;
    type Value<'op> = Vec<u8>;

    type Cf<'db> = &'static str;
    type TransactionCf<'t> = TransactionCf<'t>;

    type CfHandleFuture<'op> = Ready<Result<Self::Cf<'op>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db> {
        ready(Ok(name))
    }

    type Transaction<'t> = Transaction;

    fn transaction<'fut, 'db, F, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, Self::Error>>
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
                let cf = self.db.get(*name).ok_or(Error::NonExistentColumnFamily)?;
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
}

pub struct Transaction {
    _private: (),
}

impl<'t> sakuhiki_core::backend::Transaction<'t, MemDb> for Transaction {
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

    fn scan<'op, 'keys>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        keys: impl 'keys + RangeBounds<[u8]>,
    ) -> waaa::BoxStream<'keys, Result<(Vec<u8>, Vec<u8>), Error>>
    where
        't: 'op,
        'op: 'keys,
    {
        Box::pin(stream::iter(
            cf.cf
                .lock()
                .unwrap()
                .range(keys)
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
}

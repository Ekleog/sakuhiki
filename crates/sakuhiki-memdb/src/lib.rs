use std::{
    collections::BTreeMap,
    future::{Ready, ready},
    ops::RangeBounds,
    sync::Mutex,
};

use async_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use futures_util::stream;
use sakuhiki_core::backend::BackendCf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Column family does not exist in memory database")]
    NonExistentColumnFamily,
}

type ColumnFamily = BTreeMap<Vec<u8>, Vec<u8>>;

pub struct RoCf<'t> {
    cf: RwLockReadGuard<'t, ColumnFamily>,
    name: &'static str,
}

pub struct RwCf<'t> {
    cf: Mutex<RwLockWriteGuard<'t, ColumnFamily>>,
    name: &'static str,
}

macro_rules! cf_impl {
    ($struct:ident) => {
        impl<'t> BackendCf for $struct<'t> {
            fn name(&self) -> &'static str {
                self.name
            }
        }
    };
}

cf_impl!(RoCf);
cf_impl!(RwCf);

pub struct MemDb {
    db: BTreeMap<String, RwLock<ColumnFamily>>,
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
            .insert(cf.to_string(), RwLock::new(ColumnFamily::new()));
    }
}

macro_rules! transaction_impl {
    ($fn:ident, $locker:ident, $mapper:expr, $cf:ident, $transac:ident) => {
        type $transac<'t> = Transaction;

        fn $fn<'fut, 'db, F, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Self::Cf<'db>],
            actions: F,
        ) -> waaa::BoxFuture<'fut, Result<Ret, Self::Error>>
        where
            F: 'fut
                + waaa::Send
                + for<'t> FnOnce(&'t (), Self::$transac<'t>, Vec<$cf<'t>>) -> waaa::BoxFuture<'t, Ret>,
        {
            Box::pin(async move {
                let t = Transaction { _private: () };
                let mut cfs = cfs.iter().enumerate().collect::<Vec<_>>();
                cfs.sort_by_key(|e| e.1);
                let mut transaction_cfs = Vec::with_capacity(cfs.len());
                for (i, &name) in cfs {
                    let cf = self.db.get(*name).ok_or(Error::NonExistentColumnFamily)?;
                    let cf = $mapper(cf.$locker().await);
                    transaction_cfs.push((i, $cf { name, cf }));
                }
                transaction_cfs.sort_by_key(|e| e.0);
                let transaction_cfs = transaction_cfs.into_iter().map(|(_, cf)| cf).collect::<Vec<_>>();
                Ok(actions(&(), t, transaction_cfs).await)
            })
        }
    };
}

impl sakuhiki_core::Backend for MemDb {
    type Error = Error;

    type Key<'op> = Vec<u8>;
    type Value<'op> = Vec<u8>;

    type Cf<'db> = &'static str;
    type RoTransactionCf<'t> = RoCf<'t>;
    type RwTransactionCf<'t> = RwCf<'t>;

    type CfHandleFuture<'op> = Ready<Result<Self::Cf<'op>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db> {
        ready(Ok(name))
    }

    transaction_impl!(ro_transaction, read, |cf| cf, RoCf, RoTransaction);
    transaction_impl!(rw_transaction, write, Mutex::new, RwCf, RwTransaction);
}

pub struct Transaction {
    _private: (),
}

macro_rules! ro_transaction_methods {
    ($cf:ident, |$var:ident| $mapper:expr) => {
        fn get<'op, 'key>(
            &'op self,
            $var: &'op $cf<'t>,
            key: &'key [u8],
        ) -> waaa::BoxFuture<'key, Result<Option<Vec<u8>>, Error>>
        where
            'op: 'key,
        {
            Box::pin(ready(Ok($mapper.get(key).map(|v| v.to_owned()))))
        }

        fn scan<'op, 'keys>(
            &'op self,
            $var: &'op $cf<'t>,
            keys: impl 'keys + RangeBounds<[u8]>,
        ) -> waaa::BoxStream<'keys, Result<(Vec<u8>, Vec<u8>), Error>>
        where
            't: 'op,
            'op: 'keys,
        {
            Box::pin(stream::iter(
                $mapper
                    .range(keys)
                    .map(|(k, v)| Ok((k.to_owned(), v.to_owned())))
                    .collect::<Vec<_>>(),
            ))
        }
    };
}

impl<'t> sakuhiki_core::backend::RoTransaction<'t, MemDb> for Transaction {
    ro_transaction_methods!(RoCf, |cf| cf.cf);
}

impl<'t> sakuhiki_core::backend::RwTransaction<'t, MemDb> for Transaction {
    ro_transaction_methods!(RwCf, |cf| cf.cf.lock().unwrap());

    fn put<'op, 'kv>(
        &'op self,
        cf: &'op RwCf<'t>,
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
        cf: &'op RwCf<'t>,
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

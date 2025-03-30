use std::{
    collections::BTreeMap,
    future::{Ready, ready},
    ops::{Bound, RangeBounds},
    sync::Mutex,
};

use async_lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use futures_util::{StreamExt as _, stream};
use sakuhiki_core::{
    CfError, DbBuilder, IndexError,
    backend::{BackendBuilder, BackendCf, CfBuilder},
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

#[warn(clippy::missing_trait_methods)]
impl sakuhiki_core::Backend for MemDb {
    type Error = Error;

    type Builder = MemDbBuilder;

    fn builder() -> DbBuilder<MemDbBuilder> {
        DbBuilder::new(MemDbBuilder::new())
    }

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
        cf: &'op <MemDb as sakuhiki_core::Backend>::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, Result<(), <MemDb as sakuhiki_core::Backend>::Error>> {
        cf.cf.lock().unwrap().clear();
        Box::pin(ready(Ok(())))
    }
}

pub struct MemDbBuilder {}

impl MemDbBuilder {
    fn new() -> MemDbBuilder {
        MemDbBuilder {}
    }
}

impl BackendBuilder for MemDbBuilder {
    type Target = MemDb;

    type BuildFuture =
        waaa::BoxFuture<'static, Result<Self::Target, IndexError<Error, anyhow::Error>>>;

    fn build(
        self,
        cf_builder_list: Vec<CfBuilder<Self::Target>>,
        _drop_unknown_cfs: bool, // Never relevant, as we're always starting from scratch
    ) -> Self::BuildFuture {
        Box::pin(async move {
            let mut res = MemDb {
                db: BTreeMap::new(),
            };

            for b in cf_builder_list {
                if b.cfs.is_empty() || res.db.contains_key(b.cfs[0]) {
                    assert!(b.cfs.iter().all(|cf| res.db.contains_key(*cf)));
                    continue;
                }

                for cf in b.cfs.iter() {
                    res.db
                        .insert(cf.to_string(), AsyncMutex::new(ColumnFamily::new()));
                }

                let cfs = stream::iter(b.cfs.iter())
                    .then(async |cf| TransactionCf {
                        name: cf,
                        cf: Mutex::new(res.db.get(*cf).unwrap().lock().await),
                    })
                    .collect()
                    .await;
                let builds_using = match b.builds_using {
                    None => None,
                    Some(cf) => Some(TransactionCf {
                        name: cf,
                        cf: Mutex::new(res.db.get(cf).unwrap().lock().await),
                    }),
                };
                (b.builder)(&(), Transaction { _private: () }, cfs, builds_using).await?;
            }

            Ok(res)
        })
    }
}

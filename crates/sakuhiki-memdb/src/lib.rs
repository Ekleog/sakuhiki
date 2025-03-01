use std::{
    collections::BTreeMap,
    future::Future,
    future::{ready, Ready},
    ops::RangeBounds,
    pin::Pin,
};

use async_lock::RwLock;
use futures_util::{stream, Stream};

// TODO: look into using something like concread's BptreeMap? But is it actually able to check for transaction conflict?
pub struct MemDb {
    db: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl sakuhiki::Backend for MemDb {
    type Error = std::convert::Infallible;

    type RoTransaction<'t> = RoTransaction<'t>;

    // TODO(blocked): return impl Future (and in all the other Pin<Box<dyn Future<...>> too)
    type RoTransactionFuture<'t, F, Return>
        = Pin<Box<dyn 't + Future<Output = Result<Return, Self::Error>>>>
    where
        F: 't;

    fn ro_transaction<'fut, F, RetFut, Ret>(
        &'fut self,
        actions: F,
    ) -> Self::RoTransactionFuture<'fut, F, Ret>
    where
        F: 'fut + for<'t> FnOnce(&'t Self::RoTransaction<'t>) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        Box::pin(async {
            let db = self.db.read().await;
            let t = RoTransaction { db: &db };
            Ok(actions(&t).await)
        })
    }

    type RwTransaction<'t> = RwTransaction<'t>;

    type RwTransactionFuture<'t, F, Return>
        = Pin<Box<dyn 't + Future<Output = Result<Return, Self::Error>>>>
    where
        F: 't;

    fn rw_transaction<'fut, F, RetFut, Ret>(
        &'fut self,
        actions: F,
    ) -> Self::RwTransactionFuture<'fut, F, Ret>
    where
        F: 'fut + for<'t> FnOnce(&'t Self::RwTransaction<'t>) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        Box::pin(async {
            let mut db = self.db.write().await;
            let t = RwTransaction { db: &mut db };
            Ok(actions(&t).await)
        })
    }
}

pub struct RoTransaction<'t> {
    db: &'t BTreeMap<Vec<u8>, Vec<u8>>,
}

macro_rules! impl_ro_transaction {
    ($struct:ident) => {
        impl<'t> sakuhiki::backend::RoTransaction for $struct<'t> {
            type Error = std::convert::Infallible;

            type Key<'db>
                = &'db [u8]
            where
                Self: 'db;

            type Value<'db>
                = &'db [u8]
            where
                Self: 'db;

            type GetFuture<'key, 'db>
                = Ready<Result<Option<Self::Key<'db>>, Self::Error>>
            where
                Self: 'db,
                'db: 'key;

            fn get<'db, 'key>(&'db mut self, key: &'key [u8]) -> Self::GetFuture<'key, 'db>
            where
                'db: 'key,
            {
                ready(Ok(self.db.get(key).map(|v| v.as_slice())))
            }

            type ScanStream<'keys, 'db>
                = Pin<
                Box<
                    dyn 'keys
                        + Stream<Item = Result<(Self::Key<'db>, Self::Value<'db>), Self::Error>>,
                >,
            >
            where
                Self: 'db,
                'db: 'keys;

            fn scan<'db, 'keys>(
                &'db mut self,
                keys: impl 'keys + RangeBounds<[u8]>,
            ) -> Self::ScanStream<'keys, 'db>
            where
                'db: 'keys,
            {
                Box::pin(stream::iter(
                    self.db
                        .range(keys)
                        .map(|(k, v)| Ok((k.as_slice(), v.as_slice()))),
                ))
            }
        }
    };
}

impl_ro_transaction!(RoTransaction);

pub struct RwTransaction<'t> {
    db: &'t mut BTreeMap<Vec<u8>, Vec<u8>>,
}

impl_ro_transaction!(RwTransaction);

impl sakuhiki::backend::RwTransaction for RwTransaction<'_> {
    type PutFuture<'db>
        = Ready<Result<(), Self::Error>>
    where
        Self: 'db;

    fn put<'db>(&'db mut self, key: &'db [u8], value: &'db [u8]) -> Self::PutFuture<'db> {
        self.db.insert(key.to_vec(), value.to_vec());
        ready(Ok(()))
    }

    type DeleteFuture<'db>
        = Pin<Box<dyn 'db + Future<Output = Result<(), Self::Error>>>>
    where
        Self: 'db;

    fn delete<'db>(&'db mut self, _key: &'db [u8]) -> Self::DeleteFuture<'db> {
        todo!()
    }
}

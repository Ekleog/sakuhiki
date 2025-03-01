use std::{collections::BTreeMap, future::Future, future::Ready, ops::RangeBounds, pin::Pin};

use async_lock::RwLock;
use futures_util::Stream;

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

            fn get<'db, 'key>(&'db self, _key: &'key [u8]) -> Self::GetFuture<'key, 'db>
            where
                'db: 'key,
            {
                todo!()
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
                &'db self,
                _keys: impl RangeBounds<&'keys [u8]>,
            ) -> Self::ScanStream<'keys, 'db>
            where
                'db: 'keys,
            {
                todo!()
            }
        }
    };
}

impl_ro_transaction!(RoTransaction);

pub struct RwTransaction<'t> {
    db: &'t mut BTreeMap<Vec<u8>, Vec<u8>>,
}

impl_ro_transaction!(RwTransaction);

impl<'t> sakuhiki::backend::RwTransaction for RwTransaction<'t> {
    type PutFuture<'db>
        = Pin<Box<dyn 'db + Future<Output = Result<(), Self::Error>>>>
    where
        Self: 'db;

    fn put<'db>(&'db self, _key: &'db [u8], _value: &'db [u8]) -> Self::PutFuture<'db> {
        todo!()
    }

    type DeleteFuture<'db>
        = Pin<Box<dyn 'db + Future<Output = Result<(), Self::Error>>>>
    where
        Self: 'db;

    fn delete<'db>(&'db self, _key: &'db [u8]) -> Self::DeleteFuture<'db> {
        todo!()
    }
}

use std::{
    borrow::{Borrow, BorrowMut},
    collections::BTreeMap,
    future::{ready, Future, Ready},
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

    type Cf<'db> = String;
    type TransactionCf<'t> = &'t str;

    type CfHandleFuture<'db>
        = Ready<Result<Self::Cf<'db>, Self::Error>>
    where
        Self: 'db;

    fn cf_handle<'db>(&'db self, name: &str) -> Self::CfHandleFuture<'db> {
        ready(Ok(name.to_string()))
    }

    type RoTransaction<'t> = Transaction<&'t BTreeMap<Vec<u8>, Vec<u8>>>;

    // TODO(blocked): return impl Future (and in all the other Pin<Box<dyn Future<...>> too)
    type RoTransactionFuture<'t, F, Return>
        = Pin<Box<dyn 't + Future<Output = Result<Return, Self::Error>>>>
    where
        F: 't;

    fn ro_transaction<'fut, const CFS: usize, F, RetFut, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'fut>; CFS],
        actions: F,
    ) -> Self::RoTransactionFuture<'fut, F, Ret>
    where
        F: 'fut
            + for<'t> FnOnce(
                &'t Self::RoTransaction<'t>,
                &'t [Self::TransactionCf<'t>; CFS],
            ) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        Box::pin(async {
            let db = self.db.read().await;
            let t = Transaction { db: &*db };
            let transaction_cfs = cfs.map(|cf| cf.as_str());
            Ok(actions(&t, &transaction_cfs).await)
        })
    }

    type RwTransaction<'t> = Transaction<&'t mut BTreeMap<Vec<u8>, Vec<u8>>>;

    type RwTransactionFuture<'t, F, Return>
        = Pin<Box<dyn 't + Future<Output = Result<Return, Self::Error>>>>
    where
        F: 't;

    fn rw_transaction<'fut, const CFS: usize, F, RetFut, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'fut>; CFS],
        actions: F,
    ) -> Self::RwTransactionFuture<'fut, F, Ret>
    where
        F: 'fut
            + for<'t> FnOnce(
                &'t Self::RwTransaction<'t>,
                &'t [Self::TransactionCf<'t>; CFS],
            ) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        Box::pin(async {
            let mut db = self.db.write().await;
            let t = Transaction { db: &mut *db };
            let transaction_cfs = cfs.map(|cf| cf.as_str());
            Ok(actions(&t, &transaction_cfs).await)
        })
    }
}

pub struct Transaction<Ref> {
    db: Ref,
}

impl<'t, Ref> sakuhiki::backend::RoTransaction<&'t str> for Transaction<Ref>
where
    Ref: Borrow<BTreeMap<Vec<u8>, Vec<u8>>>,
{
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
        &'t str: 'key,
        'db: 'key;

    fn get<'db, 'key>(
        &'db mut self,
        cf: &'key &'t str,
        key: &'key [u8],
    ) -> Self::GetFuture<'key, 'db>
    where
        'db: 'key,
    {
        ready(Ok(self.db.borrow().get(key).map(|v| v.as_slice())))
    }

    type ScanStream<'keys, 'db>
        =
        Pin<Box<dyn 'keys + Stream<Item = Result<(Self::Key<'db>, Self::Value<'db>), Self::Error>>>>
    where
        Self: 'db,
        &'t str: 'keys,
        'db: 'keys;

    fn scan<'db, 'keys>(
        &'db mut self,
        cf: &'keys &'t str,
        keys: impl 'keys + RangeBounds<[u8]>,
    ) -> Self::ScanStream<'keys, 'db>
    where
        'db: 'keys,
    {
        Box::pin(stream::iter(
            self.db
                .borrow()
                .range(keys)
                .map(|(k, v)| Ok((k.as_slice(), v.as_slice()))),
        ))
    }
}

impl<'t, Ref> sakuhiki::backend::RwTransaction<&'t str> for Transaction<Ref>
where
    Ref: BorrowMut<BTreeMap<Vec<u8>, Vec<u8>>> + Borrow<BTreeMap<Vec<u8>, Vec<u8>>>,
{
    type PutFuture<'db>
        = Ready<Result<(), Self::Error>>
    where
        Self: 'db,
        &'t str: 'db;

    fn put<'db>(
        &'db mut self,
        cf: &'db &'t str,
        key: &'db [u8],
        value: &'db [u8],
    ) -> Self::PutFuture<'db> {
        self.db.borrow_mut().insert(key.to_vec(), value.to_vec());
        ready(Ok(()))
    }

    type DeleteFuture<'db>
        = Ready<Result<(), Self::Error>>
    where
        Self: 'db,
        &'t str: 'db;

    fn delete<'db>(&'db mut self, cf: &'db &'t str, key: &'db [u8]) -> Self::DeleteFuture<'db> {
        self.db.borrow_mut().remove(key);
        ready(Ok(()))
    }
}

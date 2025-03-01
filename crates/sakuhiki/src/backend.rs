use std::{future::Future, ops::RangeBounds};

use futures_util::Stream;

pub trait RoTransaction {
    type Error;

    type Key<'db>: AsRef<[u8]>
    where
        Self: 'db;

    type Value<'db>: AsRef<[u8]>
    where
        Self: 'db;

    type GetFuture<'key, 'db>: Future<Output = Result<Option<Self::Value<'db>>, Self::Error>>
    where
        Self: 'db,
        'db: 'key;

    fn get<'db, 'key>(&'db self, key: &'key [u8]) -> Self::GetFuture<'key, 'db>
    where
        'db: 'key;

    type ScanStream<'keys, 'db>: Stream<
        Item = Result<(Self::Key<'db>, Self::Value<'db>), Self::Error>,
    >
    where
        Self: 'db,
        'db: 'keys;

    // TODO: do we need get_many / multi_get?
    fn scan<'db, 'keys>(
        &'db self,
        keys: impl RangeBounds<&'keys [u8]>,
    ) -> Self::ScanStream<'keys, 'db>
    where
        'db: 'keys;
}

pub trait RwTransaction: RoTransaction {
    type PutFuture<'db>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'db;

    fn put<'db>(&'db self, key: &'db [u8], value: &'db [u8]) -> Self::PutFuture<'db>;

    type DeleteFuture<'db>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'db;

    fn delete<'db>(&'db self, key: &'db [u8]) -> Self::DeleteFuture<'db>;
}

pub trait Backend {
    type Error;

    type RoTransaction<'t>: RoTransaction
    where
        Self: 't;

    type RoTransactionFuture<'t, F, Return>: Future<Output = Result<Return, Self::Error>>
    where
        Self: 't;

    fn ro_transaction<'db, F, RetFut, Ret>(
        &'db self,
        actions: F,
    ) -> Self::RoTransactionFuture<'db, F, Ret>
    where
        F: for<'t> FnOnce(&'t Self::RoTransaction<'t>) -> RetFut,
        RetFut: Future<Output = Ret>;

    type RwTransaction<'t>: RwTransaction
    where
        Self: 't;

    type RwTransactionFuture<'t, F, Return>: Future<Output = Result<Return, Self::Error>>
    where
        Self: 't;

    fn rw_transaction<'db, F, RetFut, Ret>(
        &'db self,
        actions: F,
    ) -> Self::RwTransactionFuture<'db, F, Ret>
    where
        F: for<'t> FnOnce(&'t Self::RwTransaction<'t>) -> RetFut,
        RetFut: Future<Output = Ret>;
}

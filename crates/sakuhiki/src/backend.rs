use std::{future::Future, ops::RangeBounds};

use futures_util::Stream;

pub trait RoTransaction<Cf> {
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
        Cf: 'key,
        'db: 'key;

    fn get<'db, 'key>(&'db mut self, cf: &'key Cf, key: &'key [u8]) -> Self::GetFuture<'key, 'db>
    where
        'db: 'key;

    type ScanStream<'keys, 'db>: Stream<
        Item = Result<(Self::Key<'db>, Self::Value<'db>), Self::Error>,
    >
    where
        Self: 'db,
        Cf: 'keys,
        'db: 'keys;

    // TODO: do we need get_many / multi_get?
    fn scan<'db, 'keys>(
        &'db mut self,
        cf: &'keys Cf,
        keys: impl 'keys + RangeBounds<[u8]>,
    ) -> Self::ScanStream<'keys, 'db>
    where
        'db: 'keys;
}

pub trait RwTransaction<Cf>: RoTransaction<Cf> {
    type PutFuture<'db>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'db,
        Cf: 'db;

    fn put<'db>(
        &'db mut self,
        cf: &'db Cf,
        key: &'db [u8],
        value: &'db [u8],
    ) -> Self::PutFuture<'db>;

    type DeleteFuture<'db>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'db,
        Cf: 'db;

    fn delete<'db>(&'db mut self, cf: &'db Cf, key: &'db [u8]) -> Self::DeleteFuture<'db>;
}

pub trait Backend {
    type Error;

    type Cf<'db>: Clone
    where
        Self: 'db;

    type TransactionCf<'t>: Clone
    where
        Self: 't;

    type CfHandleFuture<'db>: Future<Output = Result<Self::Cf<'db>, Self::Error>>
    where
        Self: 'db;

    fn cf_handle<'db>(&'db self, name: &str) -> Self::CfHandleFuture<'db>;

    type RoTransaction<'t>: RoTransaction<Self::TransactionCf<'t>>
    where
        Self: 't;

    type RoTransactionFuture<'t, F, Return>: Future<Output = Result<Return, Self::Error>>
    where
        Self: 't,
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
        RetFut: Future<Output = Ret>;

    type RwTransaction<'t>: RwTransaction<Self::TransactionCf<'t>>
    where
        Self: 't;

    type RwTransactionFuture<'t, F, Return>: Future<Output = Result<Return, Self::Error>>
    where
        Self: 't,
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
        RetFut: Future<Output = Ret>;
}

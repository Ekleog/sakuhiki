use std::future::Future;

use futures_util::Stream;

pub trait RoTransaction {
    type Error;

    type Key<'a>: AsRef<[u8]>
    where
        Self: 'a;

    type Value<'a>: AsRef<[u8]>
    where
        Self: 'a;

    type GetFuture<'a>: Future<Output = Result<Option<Self::Value<'a>>, Self::Error>>
    where
        Self: 'a;

    type ScanStream<'a>: Stream<Item = Result<(Self::Key<'a>, Self::Value<'a>), Self::Error>>
    where
        Self: 'a;

    fn get(&'_ self, key: &'_ [u8]) -> Self::GetFuture<'_>;
    // TODO: do we need get_many / multi_get?
    fn scan(&'_ self, start: &'_ [u8], end: &'_ [u8]) -> Self::ScanStream<'_>;
}

pub trait RwTransaction: RoTransaction {
    type PutFuture<'a>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'a;

    type DeleteFuture<'a>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'a;

    fn put(&'_ self, key: &'_ [u8], value: &'_ [u8]) -> Self::PutFuture<'_>;
    fn delete(&'_ self, key: &'_ [u8]) -> Self::DeleteFuture<'_>;
}

pub trait Backend {
    type Error;

    type RoTransaction<'a>: RoTransaction
    where
        Self: 'a;

    type RwTransaction<'a>: RwTransaction
    where
        Self: 'a;

    type RoTransactionFuture<'a>: Future<Output = Result<Self::RoTransaction<'a>, Self::Error>>
    where
        Self: 'a;

    type RwTransactionFuture<'a>: Future<Output = Result<Self::RwTransaction<'a>, Self::Error>>
    where
        Self: 'a;

    fn ro_transaction(&self) -> Self::RoTransactionFuture<'_>;

    fn rw_transaction(&self) -> Self::RwTransactionFuture<'_>;
}

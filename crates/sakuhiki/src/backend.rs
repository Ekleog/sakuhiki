use std::{future::Future, ops::RangeBounds};

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

    /// Guarantees that the returned value still has this value at the end of the transaction.
    fn get(&'_ self, key: &'_ [u8]) -> Self::GetFuture<'_>;

    type ScanStream<'a>: Stream<Item = Result<(Self::Key<'a>, Self::Value<'a>), Self::Error>>
    where
        Self: 'a;

    // TODO: do we need get_many / multi_get?
    fn scan<'a>(&'a self, keys: impl RangeBounds<&'a [u8]>) -> Self::ScanStream<'a>;
}

pub trait RwTransaction: RoTransaction {
    type PutFuture<'a>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'a;

    fn put(&'_ self, key: &'_ [u8], value: &'_ [u8]) -> Self::PutFuture<'_>;

    type DeleteFuture<'a>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'a;

    fn delete(&'_ self, key: &'_ [u8]) -> Self::DeleteFuture<'_>;
}

pub trait Backend {
    type Error;

    type RoTransaction<'a>: RoTransaction
    where
        Self: 'a;

    type RoTransactionFuture<'a, F, Return>: Future<Output = Result<Return, Self::Error>>
    where
        Self: 'a;

    fn ro_transaction<F, RetFut, Ret>(&self, actions: F) -> Self::RoTransactionFuture<'_, F, Ret>
    where
        F: FnOnce(&'_ Self::RoTransaction<'_>) -> RetFut,
        RetFut: Future<Output = Ret>;

    type RwTransaction<'a>: RwTransaction
    where
        Self: 'a;

    type RwTransactionFuture<'a, F, Return>: Future<Output = Result<Return, Self::Error>>
    where
        Self: 'a;

    fn rw_transaction<F, RetFut, Ret>(&self, actions: F) -> Self::RwTransactionFuture<'_, F, Ret>
    where
        F: FnOnce(&'_ Self::RwTransaction<'_>) -> RetFut,
        RetFut: Future<Output = Ret>;
}

use std::{
    borrow::{Borrow, BorrowMut},
    collections::BTreeMap,
    future::{ready, Future, Ready},
    ops::RangeBounds,
    pin::Pin,
};

use async_lock::RwLock;
use futures_util::{stream, Stream};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Column family does not exist in memory database")]
    NonExistentColumnFamily,
}

type ColumnFamily = BTreeMap<Vec<u8>, Vec<u8>>;

// TODO: look into using something like concread's BptreeMap? But is it actually able to check for transaction conflict?
pub struct MemDb {
    db: BTreeMap<String, RwLock<ColumnFamily>>,
}

impl sakuhiki::Backend for MemDb {
    type Error = Error;

    type Cf<'db> = String;
    type RoTransactionCf<'t> = &'t ColumnFamily;
    type RwTransactionCf<'t> = &'t mut ColumnFamily;

    type CfHandleFuture<'db>
        = Ready<Result<Self::Cf<'db>, Self::Error>>
    where
        Self: 'db;

    fn cf_handle<'db>(&'db self, name: &str) -> Self::CfHandleFuture<'db> {
        ready(Ok(name.to_string()))
    }

    type RoTransaction<'t> = Transaction;

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
                &'t mut Self::RoTransaction<'t>,
                [Self::RoTransactionCf<'t>; CFS],
            ) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        Box::pin(async {
            let mut t = Transaction { _private: () };
            let mut cfs = cfs.iter().enumerate().collect::<Vec<_>>();
            cfs.sort_by_key(|e| e.1);
            let mut transaction_cfs = Vec::with_capacity(CFS);
            for (i, &cf) in cfs {
                let cf = self.db.get(cf).ok_or(Error::NonExistentColumnFamily)?;
                transaction_cfs.push((i, cf.read().await));
            }
            transaction_cfs.sort_by_key(|e| e.0);
            let transaction_cfs = transaction_cfs
                .iter()
                .map(|(_, cf)| &**cf)
                .collect::<Vec<_>>();
            let transaction_cfs = transaction_cfs.try_into().unwrap();
            Ok(actions(&mut t, transaction_cfs).await)
        })
    }

    type RwTransaction<'t> = Transaction;

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
                &'t mut Self::RwTransaction<'t>,
                [Self::RwTransactionCf<'t>; CFS],
            ) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        Box::pin(async {
            let mut t = Transaction { _private: () };
            let mut cfs = cfs.iter().enumerate().collect::<Vec<_>>();
            cfs.sort_by_key(|e| e.1);
            let mut transaction_cfs = Vec::with_capacity(CFS);
            for (i, &cf) in cfs {
                let cf = self.db.get(cf).ok_or(Error::NonExistentColumnFamily)?;
                transaction_cfs.push((i, cf.write().await));
            }
            transaction_cfs.sort_by_key(|e| e.0);
            let transaction_cfs = transaction_cfs
                .iter_mut()
                .map(|(_, cf)| &mut **cf)
                .collect::<Vec<_>>();
            let transaction_cfs = transaction_cfs.try_into().unwrap();
            Ok(actions(&mut t, transaction_cfs).await)
        })
    }
}

pub struct Transaction {
    _private: (),
}

impl<Cf> sakuhiki::backend::RoTransaction<Cf> for Transaction
where
    Cf: Borrow<ColumnFamily>,
{
    type Error = std::convert::Infallible;

    type Key<'db>
        = &'db [u8]
    where
        Self: 'db,
        Cf: 'db;

    type Value<'db>
        = &'db [u8]
    where
        Self: 'db,
        Cf: 'db;

    type GetFuture<'key, 'db>
        = Ready<Result<Option<Self::Key<'db>>, Self::Error>>
    where
        Self: 'db,
        Cf: 'db,
        'db: 'key;

    fn get<'db, 'key>(&'db mut self, cf: &'db mut Cf, key: &'key [u8]) -> Self::GetFuture<'key, 'db>
    where
        'db: 'key,
    {
        ready(Ok((*cf).borrow().get(key).map(|v| v.as_slice())))
    }

    type ScanStream<'keys, 'db>
        =
        Pin<Box<dyn 'keys + Stream<Item = Result<(Self::Key<'db>, Self::Value<'db>), Self::Error>>>>
    where
        Self: 'db,
        Cf: 'db,
        'db: 'keys;

    fn scan<'db, 'keys>(
        &'db mut self,
        cf: &'db mut Cf,
        keys: impl 'keys + RangeBounds<[u8]>,
    ) -> Self::ScanStream<'keys, 'db>
    where
        'db: 'keys,
    {
        Box::pin(stream::iter(
            (*cf)
                .borrow()
                .range(keys)
                .map(|(k, v)| Ok((k.as_slice(), v.as_slice()))),
        ))
    }
}

impl<Cf> sakuhiki::backend::RwTransaction<Cf> for Transaction
where
    Cf: BorrowMut<ColumnFamily> + Borrow<ColumnFamily>,
{
    type PutFuture<'db>
        = Ready<Result<(), Self::Error>>
    where
        Self: 'db,
        Cf: 'db;

    fn put<'db>(
        &'db mut self,
        cf: &'db mut Cf,
        key: &'db [u8],
        value: &'db [u8],
    ) -> Self::PutFuture<'db> {
        cf.borrow_mut().insert(key.to_vec(), value.to_vec());
        ready(Ok(()))
    }

    type DeleteFuture<'db>
        = Ready<Result<(), Self::Error>>
    where
        Self: 'db,
        Cf: 'db;

    fn delete<'db>(&'db mut self, cf: &'db mut Cf, key: &'db [u8]) -> Self::DeleteFuture<'db> {
        cf.borrow_mut().remove(key);
        ready(Ok(()))
    }
}

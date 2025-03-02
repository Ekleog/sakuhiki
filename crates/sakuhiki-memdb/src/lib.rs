use std::{
    collections::BTreeMap,
    future::{Future, Ready, ready},
    ops::RangeBounds,
    pin::Pin,
};

use async_lock::RwLock;
use futures_util::{Stream, stream};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Column family does not exist in memory database")]
    NonExistentColumnFamily,
}

type ColumnFamily = BTreeMap<Vec<u8>, Vec<u8>>;
type RoCf<'t> = &'t ColumnFamily;
type RwCf<'t> = &'t mut ColumnFamily;

// TODO: look into using something like concread's BptreeMap? But is it actually able to check for transaction conflict?
pub struct MemDb {
    db: BTreeMap<String, RwLock<ColumnFamily>>,
}

macro_rules! transaction_impl {
    ($fn:ident, $iter:ident, $locker:ident, $mapper:expr, $cf:ident, $transac:ident, $transacfut:ident) => {
        type $transac<'t> = Transaction;

        // TODO(blocked): return impl Future (and in all the other Pin<Box<dyn Future<...>> too)
        type $transacfut<'t, F, Return>
            = Pin<Box<dyn 't + Future<Output = Result<Return, Self::Error>>>>
        where
            F: 't;

        fn $fn<'fut, const CFS: usize, F, RetFut, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Self::Cf<'fut>; CFS],
            actions: F,
        ) -> Self::$transacfut<'fut, F, Ret>
        where
            F: 'fut + for<'t> FnOnce(&'t mut Self::$transac<'t>, [$cf<'t>; CFS]) -> RetFut,
            RetFut: Future<Output = Ret>,
        {
            Box::pin(async {
                let mut t = Transaction { _private: () };
                let mut cfs = cfs.iter().enumerate().collect::<Vec<_>>();
                cfs.sort_by_key(|e| e.1);
                let mut transaction_cfs = Vec::with_capacity(CFS);
                for (i, &cf) in cfs {
                    let cf = self.db.get(cf).ok_or(Error::NonExistentColumnFamily)?;
                    transaction_cfs.push((i, cf.$locker().await));
                }
                transaction_cfs.sort_by_key(|e| e.0);
                let transaction_cfs = transaction_cfs.$iter().map($mapper).collect::<Vec<_>>();
                let transaction_cfs = transaction_cfs.try_into().unwrap();
                Ok(actions(&mut t, transaction_cfs).await)
            })
        }
    };
}

impl sakuhiki::Backend for MemDb {
    type Error = Error;

    type Key<'op> = &'op [u8];

    type Value<'op> = &'op [u8];

    type Cf<'db> = String;
    type RoTransactionCf<'t> = RoCf<'t>;
    type RwTransactionCf<'t> = RwCf<'t>;

    type CfHandleFuture<'op> = Ready<Result<Self::Cf<'op>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &str) -> Self::CfHandleFuture<'db> {
        ready(Ok(name.to_string()))
    }

    transaction_impl!(
        ro_transaction,
        iter,
        read,
        |(_, cf)| &**cf,
        RoCf,
        RoTransaction,
        RoTransactionFuture
    );

    transaction_impl!(
        rw_transaction,
        iter_mut,
        write,
        |(_, cf)| &mut **cf,
        RwCf,
        RwTransaction,
        RwTransactionFuture
    );
}

pub struct Transaction {
    _private: (),
}

macro_rules! ro_transaction_methods {
    ($cf:ident) => {
        type GetFuture<'op, 'key>
            = Ready<Result<Option<&'op [u8]>, Error>>
        where
            't: 'op,
            'op: 'key;

        fn get<'op, 'key>(
            &'op mut self,
            cf: &'op mut $cf<'t>,
            key: &'key [u8],
        ) -> Self::GetFuture<'op, 'key>
        where
            'op: 'key,
        {
            ready(Ok(cf.get(key).map(|v| v.as_slice())))
        }

        type ScanStream<'op, 'keys>
            = Pin<Box<dyn 'keys + Stream<Item = Result<(&'op [u8], &'op [u8]), Error>>>>
        where
            't: 'op,
            'op: 'keys;

        fn scan<'op, 'keys>(
            &'op mut self,
            cf: &'op mut $cf<'t>,
            keys: impl 'keys + RangeBounds<[u8]>,
        ) -> Self::ScanStream<'op, 'keys>
        where
            't: 'op,
            'op: 'keys,
        {
            Box::pin(stream::iter(
                cf.range(keys)
                    .map(|(k, v)| Ok((k.as_slice(), v.as_slice()))),
            ))
        }
    };
}

impl<'t> sakuhiki::backend::RoTransaction<'t, MemDb> for Transaction {
    ro_transaction_methods!(RoCf);
}

impl<'t> sakuhiki::backend::RwTransaction<'t, MemDb> for Transaction {
    ro_transaction_methods!(RwCf);

    type PutFuture<'op>
        = Ready<Result<(), Error>>
    where
        't: 'op;

    fn put<'op>(
        &'op mut self,
        cf: &'op mut RwCf<'t>,
        key: &'op [u8],
        value: &'op [u8],
    ) -> Self::PutFuture<'op>
    where
        't: 'op,
    {
        cf.insert(key.to_vec(), value.to_vec());
        ready(Ok(()))
    }

    type DeleteFuture<'op>
        = Ready<Result<(), Error>>
    where
        't: 'op;

    fn delete<'op>(&'op mut self, cf: &'op mut RwCf<'t>, key: &'op [u8]) -> Self::DeleteFuture<'op>
    where
        't: 'op,
    {
        cf.remove(key);
        ready(Ok(()))
    }
}

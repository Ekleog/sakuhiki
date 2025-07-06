use std::{ops::RangeBounds, sync::Mutex};

use sakuhiki_core::Backend;

use crate::{Cf, RocksDb};

pub struct Transaction<'t> {
    transaction: Mutex<rocksdb::Transaction<'t, rocksdb::TransactionDB>>,
    rw: bool,
}

impl<'t> Transaction<'t> {
    pub(crate) fn new(t: rocksdb::Transaction<'t, rocksdb::TransactionDB>, rw: bool) -> Self {
        Self {
            transaction: Mutex::new(t),
            rw,
        }
    }
}

impl<'t> sakuhiki_core::backend::Transaction<'t, RocksDb> for Transaction<'t> {
    type ExclusiveLock<'op>
        = ()
    where
        't: 'op;

    fn take_exclusive_lock<'op>(
        &'op self,
        _cf: &'op <RocksDb as Backend>::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, eyre::Result<Self::ExclusiveLock<'op>>>
    where
        't: 'op,
    {
        todo!() // TODO(high)
    }

    fn get<'op, 'key>(
        &'op self,
        cf: &'op Cf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, eyre::Result<Option<Vec<u8>>>>
    where
        'op: 'key,
    {
        todo!() // TODO(high)
    }

    fn scan<'op, 'keys, R>(
        &'op self,
        cf: &'op Cf<'t>,
        keys: impl 'keys + RangeBounds<R>,
    ) -> waaa::BoxStream<'keys, eyre::Result<(Vec<u8>, Vec<u8>)>>
    where
        't: 'op,
        'op: 'keys,
        R: ?Sized + AsRef<[u8]>,
    {
        todo!() // TODO(high)
    }

    fn put<'op, 'kv>(
        &'op self,
        cf: &'op Cf<'t>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> waaa::BoxFuture<'kv, eyre::Result<Option<Vec<u8>>>>
    where
        't: 'op,
        'op: 'kv,
    {
        todo!() // TODO(high)
    }

    fn delete<'op, 'key>(
        &'op self,
        cf: &'op Cf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, eyre::Result<Option<Vec<u8>>>>
    where
        't: 'op,
        'op: 'key,
    {
        todo!() // TODO(high)
    }

    fn clear<'op>(
        &'op self,
        cf: &'op <RocksDb as Backend>::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, eyre::Result<()>> {
        todo!() // TODO(high)
    }
}

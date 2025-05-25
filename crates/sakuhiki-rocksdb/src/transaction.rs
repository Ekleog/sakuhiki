use std::ops::RangeBounds;

use sakuhiki_core::Backend;

use crate::{Error, RocksDb, TransactionCf};

pub struct Transaction<'t> {
    transaction: &'t (), // TODO(high): rocksdb::Transaction<'t, rocksdb::TransactionDB>,
}

impl<'t> sakuhiki_core::backend::Transaction<'t, RocksDb> for Transaction<'t> {
    type ExclusiveLock<'op>
        = ()
    where
        't: 'op;

    fn take_exclusive_lock<'op>(
        &'op self,
        _cf: &'op <RocksDb as Backend>::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, Result<Self::ExclusiveLock<'op>, <RocksDb as Backend>::Error>>
    where
        't: 'op,
    {
        todo!() // TODO(high)
    }

    fn get<'op, 'key>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<Vec<u8>>, Error>>
    where
        'op: 'key,
    {
        todo!() // TODO(high)
    }

    fn scan<'op, 'keys, R>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        keys: impl 'keys + RangeBounds<R>,
    ) -> waaa::BoxStream<'keys, Result<(Vec<u8>, Vec<u8>), Error>>
    where
        't: 'op,
        'op: 'keys,
        R: ?Sized + AsRef<[u8]>,
    {
        todo!() // TODO(high)
    }

    fn put<'op, 'kv>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> waaa::BoxFuture<'kv, Result<Option<Vec<u8>>, Error>>
    where
        't: 'op,
        'op: 'kv,
    {
        todo!() // TODO(high)
    }

    fn delete<'op, 'key>(
        &'op self,
        cf: &'op TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<Vec<u8>>, Error>>
    where
        't: 'op,
        'op: 'key,
    {
        todo!() // TODO(high)
    }

    fn clear<'op>(
        &'op self,
        cf: &'op <RocksDb as Backend>::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, Result<(), <RocksDb as Backend>::Error>> {
        todo!() // TODO(high)
    }
}

use std::{ops::RangeBounds, sync::Mutex};

use sakuhiki_core::{Backend, Mode};

use crate::{Cf, RocksDb};

pub struct Transaction<'t> {
    transaction: Mutex<rocksdb::Transaction<'t, rocksdb::TransactionDB>>,
    mode: Mode,
}

impl<'t> Transaction<'t> {
    pub(crate) fn new(t: rocksdb::Transaction<'t, rocksdb::TransactionDB>, mode: Mode) -> Self {
        Self {
            transaction: Mutex::new(t),
            mode,
        }
    }
}

impl<'t> sakuhiki_core::backend::Transaction<'t, RocksDb> for Transaction<'t> {
    fn current_mode(&self) -> Mode {
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

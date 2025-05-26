use std::{
    future::{self, Ready},
    path::Path,
};

use sakuhiki_core::{Backend, CfError, backend::Builder};
use tokio::task::block_in_place;

use crate::{Error, ErrorKind, RocksDbBuilder, Transaction, TransactionCf};

pub struct RocksDb {
    db: rocksdb::TransactionDB<rocksdb::SingleThreaded>,
}

impl RocksDb {
    pub fn builder<P: AsRef<Path>>(path: P) -> Builder<RocksDb> {
        Builder::new(RocksDbBuilder::new(path))
    }

    pub(crate) fn new(db: rocksdb::TransactionDB<rocksdb::SingleThreaded>) -> RocksDb {
        RocksDb { db }
    }

    pub(crate) async fn start_transaction(&self, rw: bool) -> crate::Result<Transaction<'_>> {
        let t = block_in_place(|| self.db.transaction());
        Ok(Transaction::new(t, rw))
    }

    fn transaction<'fut, 'db, F, Ret>(
        &'fut self,
        rw: bool,
        cfs: &'fut [&'fut TransactionCf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, CfError<Error>>>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(
                &'t (),
                Transaction<'t>,
                Vec<TransactionCf<'t>>,
            ) -> waaa::BoxFuture<'t, Ret>,
    {
        Box::pin(async move {
            let t = self.start_transaction(rw).await.map_err(CfError::backend)?;
            let cfs = cfs.iter().map(|cf| (**cf).clone()).collect();
            Ok((actions)(&(), t, cfs).await)
        })
    }
}

#[warn(clippy::missing_trait_methods)] // TODO(med): should set that at crate level
impl Backend for RocksDb {
    type Error = Error;

    type Builder = RocksDbBuilder;

    type Cf<'db> = TransactionCf<'db>;

    type CfHandleFuture<'op> = Ready<Result<Self::Cf<'op>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db> {
        let result = block_in_place(|| self.db.cf_handle(name))
            .ok_or_else(|| Error::simple(ErrorKind::NoSuchCf(name)))
            .map(|cf| TransactionCf::new(name, cf));
        future::ready(result)
    }

    type Transaction<'t> = Transaction<'t>;
    type TransactionCf<'t> = TransactionCf<'t>;

    fn ro_transaction<'fut, 'db, F, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, CfError<Self::Error>>>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(
                &'t (),
                Transaction<'t>,
                Vec<TransactionCf<'t>>,
            ) -> waaa::BoxFuture<'t, Ret>,
    {
        self.transaction(false, cfs, actions)
    }

    fn rw_transaction<'fut, 'db, F, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, CfError<Self::Error>>>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(
                &'t (),
                Self::Transaction<'t>,
                Vec<Self::TransactionCf<'t>>,
            ) -> waaa::BoxFuture<'t, Ret>,
    {
        self.transaction(true, cfs, actions)
    }

    // TODO(blocked): This should be DbPinnableSlice, as soon as
    // https://github.com/rust-rocksdb/rust-rocksdb/issues/1005 gets fixed
    // TODO(med): still, key/value should probably borrow the Db and not the Transaction?
    type Key<'op> = Vec<u8>;
    type Value<'op> = Vec<u8>;
}

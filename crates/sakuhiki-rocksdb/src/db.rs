use std::{future::Ready, path::Path};

use sakuhiki_core::{Backend, CfError, backend::Builder};

use crate::{Error, RocksDbBuilder, Transaction, TransactionCf};

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
}

#[warn(clippy::missing_trait_methods)] // TODO(med): should set that at crate level
impl Backend for RocksDb {
    type Error = Error;

    type Builder = RocksDbBuilder;

    type Cf<'db> = &'static str;

    type CfHandleFuture<'op> = Ready<Result<Self::Cf<'op>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db> {
        todo!() // TODO(high)
    }

    type Transaction<'t> = Transaction;
    type TransactionCf<'t> = TransactionCf<'t>;

    fn ro_transaction<'fut, 'db, F, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, CfError<Self::Error>>>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(&'t (), Transaction, Vec<TransactionCf<'t>>) -> waaa::BoxFuture<'t, Ret>,
    {
        Box::pin(async move {
            todo!() // TODO(high)
        })
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
        self.ro_transaction(cfs, actions) // TODO(high)
    }

    type Key<'op> = Vec<u8>; // TODO(high)
    type Value<'op> = Vec<u8>; // TODO(high)
}

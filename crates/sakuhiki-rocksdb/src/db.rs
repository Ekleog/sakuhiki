use std::{
    borrow::Borrow,
    future::{self, Ready},
    path::Path,
};

use sakuhiki_core::{Backend, Mode, backend::Builder};
use tokio::task::block_in_place;

use crate::{Cf, Error, RocksDbBuilder, Transaction};

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
    type Builder = RocksDbBuilder;

    type Cf<'db> = Cf<'db>;

    type CfHandleFuture<'op> = Ready<eyre::Result<Self::Cf<'op>>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db> {
        let result = block_in_place(|| self.db.cf_handle(name))
            .ok_or_else(|| eyre::Report::from(Error::NoSuchCf(name)))
            .map(|cf| Cf::new(name, cf));
        future::ready(result)
    }

    type Transaction<'t> = Transaction<'t>;
    type TransactionCf<'t> = Cf<'t>;

    fn transaction<'fut, 'db, Bcf, F, Ret>(
        &'fut self,
        mode: Mode,
        cfs: &'fut [Bcf],
        actions: F,
    ) -> waaa::BoxFuture<'fut, eyre::Result<Ret>>
    where
        Bcf: 'fut + waaa::Send + waaa::Sync + Borrow<Cf<'db>>,
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(&'t (), Transaction<'t>, Vec<Cf<'t>>) -> waaa::BoxFuture<'t, Ret>,
    {
        // TODO(high): IndexRebuilding should exclusively lock the requested CFs
        Box::pin(async move {
            let t = block_in_place(|| self.db.transaction());
            let t = Transaction::new(t, mode);
            let cfs = cfs.iter().map(|cf| cf.borrow().clone()).collect();
            Ok((actions)(&(), t, cfs).await)
        })
    }

    // TODO(blocked): This should be DbPinnableSlice, as soon as
    // https://github.com/rust-rocksdb/rust-rocksdb/issues/1005 gets fixed
    // TODO(med): still, key/value should probably borrow the Db and not the Transaction?
    type Key<'op> = Vec<u8>;
    type Value<'op> = Vec<u8>;
}

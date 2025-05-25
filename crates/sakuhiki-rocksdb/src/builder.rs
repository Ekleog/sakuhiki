use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use sakuhiki_core::{Backend, BackendBuilder, Datum, Db, IndexError};
use tokio::task::spawn_blocking;

use crate::{Error, ErrorWhile, RocksDb};

pub struct RocksDbBuilder {
    path: PathBuf,
    global_opts: Option<rocksdb::Options>,
    txn_db_opts: Option<rocksdb::TransactionDBOptions>,
    configured_cf_opts: HashMap<&'static str, rocksdb::Options>,
    built_cf_opts: HashMap<&'static str, rocksdb::Options>,
}

impl RocksDbBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        RocksDbBuilder {
            path: path.as_ref().to_owned(),
            global_opts: None,
            txn_db_opts: None,
            configured_cf_opts: HashMap::new(),
            built_cf_opts: HashMap::new(),
        }
    }

    pub fn global_opts(mut self, opts: rocksdb::Options) -> Self {
        assert!(
            self.global_opts.is_none(),
            "Tried setting global options multiple times"
        );
        self.global_opts = Some(opts);
        self
    }

    pub fn txn_db_opts(mut self, txn_db_opts: rocksdb::TransactionDBOptions) -> Self {
        assert!(
            self.txn_db_opts.is_none(),
            "Tried setting TransactionDB options multiple times"
        );
        self.txn_db_opts = Some(txn_db_opts);
        self
    }

    pub fn cf_option(mut self, cf: &'static str, opts: rocksdb::Options) -> Self {
        let previous_opts = self.configured_cf_opts.insert(cf, opts);
        if previous_opts.is_some() {
            panic!("Configured the same CF {cf:?} multiple times");
        }
        self
    }
}

impl BackendBuilder for RocksDbBuilder {
    type Target = RocksDb;

    fn build_datum_cf(
        mut self,
        cf: &'static str,
    ) -> waaa::BoxFuture<'static, Result<Self, <Self::Target as Backend>::Error>> {
        Box::pin(async move {
            todo!() // TODO(high)
        })
    }

    fn build_index_cf<I: ?Sized + sakuhiki_core::Indexer<Self::Target>>(
        mut self,
        index: &I,
    ) -> waaa::BoxFuture<
        '_,
        Result<Self, IndexError<<Self::Target as Backend>::Error, <I::Datum as Datum>::Error>>,
    > {
        todo!() // TODO(high)
    }

    fn drop_unknown_cfs(self) -> waaa::BoxFuture<'static, Result<RocksDbBuilder, Error>> {
        todo!() // TODO(high)
    }

    type BuildFuture =
        waaa::BoxFuture<'static, Result<Db<Self::Target>, IndexError<Error, anyhow::Error>>>;

    fn build(self) -> Self::BuildFuture {
        todo!() // TODO(high)

        /*
        let path = path.as_ref();
        let path_buf = path.to_owned();
        let db =
            spawn_blocking(move || rocksdb::TransactionDB::open(&opts, &txn_db_opts, path_buf))
                .await
                .map_err(|e| {
                    Error::spawn_blocking(ErrorWhile::OpeningDatabase(path.to_owned()), e)
                })?
                .map_err(|e| Error::rocksdb(ErrorWhile::OpeningDatabase(path.to_owned()), e))?;
        let cf_options = HashMap::new();
        Ok(RocksDbBuilder { db, cf_options })
        */
    }
}

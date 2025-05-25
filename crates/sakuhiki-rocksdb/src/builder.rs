use std::{collections::HashMap, path::Path};

use sakuhiki_core::{Backend, BackendBuilder, Datum, Db, IndexError};
use tokio::task::spawn_blocking;

use crate::{Error, ErrorWhile, RocksDb};

pub struct RocksDbBuilder {
    db: rocksdb::TransactionDB<rocksdb::SingleThreaded>,
    cf_options: HashMap<&'static str, rocksdb::Options>,
}

impl RocksDbBuilder {
    pub async fn new<P: AsRef<Path>>(path: P) -> crate::Result<RocksDbBuilder> {
        let path = path.as_ref();
        let path_buf = path.to_owned();
        let db = spawn_blocking(move || rocksdb::TransactionDB::open_default(path_buf))
            .await
            .map_err(|e| Error::spawn_blocking(ErrorWhile::OpeningDatabase(path.to_owned()), e))?
            .map_err(|e| Error::rocksdb(ErrorWhile::OpeningDatabase(path.to_owned()), e))?;
        let cf_options = HashMap::new();
        Ok(RocksDbBuilder { db, cf_options })
    }

    pub async fn with_options<P: AsRef<Path>>(
        opts: rocksdb::Options,
        txn_db_opts: rocksdb::TransactionDBOptions,
        path: P,
    ) -> crate::Result<RocksDbBuilder> {
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
    }

    pub fn cf_option(self, cf: &'static str, opts: rocksdb::Options) -> Self {
        let mut this = self;
        let previous_opts = this.cf_options.insert(cf, opts);
        if previous_opts.is_some() {
            panic!("Configured the same CF {cf:?} multiple times");
        }
        this
    }
}

impl BackendBuilder for RocksDbBuilder {
    type Target = RocksDb;

    fn build_datum_cf(
        mut self,
        cf: &'static str,
    ) -> waaa::BoxFuture<'static, Result<Self, <Self::Target as Backend>::Error>> {
        todo!() // TODO(high)
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
    }
}

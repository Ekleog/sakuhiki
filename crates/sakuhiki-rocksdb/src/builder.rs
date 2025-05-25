use std::path::{Path, PathBuf};

use sakuhiki_core::{BackendBuilder, IndexError, backend::BuilderConfig};

use crate::{Error, RocksDb};

pub struct RocksDbBuilder {
    path: PathBuf,
    global_opts: Option<rocksdb::Options>,
    txn_db_opts: Option<rocksdb::TransactionDBOptions>,
}

impl RocksDbBuilder {
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> Self {
        RocksDbBuilder {
            path: path.as_ref().to_owned(),
            global_opts: None,
            txn_db_opts: None,
        }
    }

    pub fn global_opts(&mut self, opts: rocksdb::Options) -> &mut Self {
        assert!(
            self.global_opts.is_none(),
            "Tried setting global options multiple times"
        );
        self.global_opts = Some(opts);
        self
    }

    pub fn txn_db_opts(&mut self, txn_db_opts: rocksdb::TransactionDBOptions) -> &mut Self {
        assert!(
            self.txn_db_opts.is_none(),
            "Tried setting TransactionDB options multiple times"
        );
        self.txn_db_opts = Some(txn_db_opts);
        self
    }
}

impl BackendBuilder for RocksDbBuilder {
    type Target = RocksDb;
    type CfOptions = rocksdb::Options;

    type BuildFuture = waaa::BoxFuture<'static, Result<RocksDb, IndexError<Error, anyhow::Error>>>;

    fn build(self, config: BuilderConfig<RocksDb>) -> Self::BuildFuture {
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

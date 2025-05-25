use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context as _;
use rocksdb::ColumnFamilyDescriptor;
use sakuhiki_core::{
    BackendBuilder,
    backend::{BuilderConfig, CfOptions},
};
use tokio::task::spawn_blocking;

use crate::RocksDb;

pub struct RocksDbBuilder {
    path: Arc<PathBuf>,
    global_opts: Option<rocksdb::Options>,
    txn_db_opts: Option<rocksdb::TransactionDBOptions>,
}

impl RocksDbBuilder {
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> Self {
        RocksDbBuilder {
            path: Arc::new(path.as_ref().to_owned()),
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

    type BuildFuture = waaa::BoxFuture<'static, anyhow::Result<RocksDb>>;

    fn build(self, mut config: BuilderConfig<RocksDb>) -> Self::BuildFuture {
        Box::pin(async move {
            let path_d = self.path.display();

            let (options, mut cfs) = spawn_blocking({
                let path = self.path.clone();
                move || {
                    rocksdb::Options::load_latest(
                        &*path,
                        rocksdb::Env::new()?,
                        true,
                        rocksdb::Cache::new_lru_cache(1024),
                    )
                }
            })
            .await
            .with_context(|| format!("Failed joining task that lists CFs for {path_d}"))?
            .with_context(|| format!("Failed listing CFs for {path_d}"))?;

            let mut opened_cfs = HashSet::new();
            for cfd in &mut cfs {
                let cf = cfd.name().to_owned();
                if let Some(CfOptions::Configured(options)) = config.cfs.remove(&cf as &str) {
                    // Both ReuseLast and NotConfigured means we'll actually reuse the last configuration
                    *cfd = ColumnFamilyDescriptor::new(&cf, options);
                    opened_cfs.insert(cf);
                }
            }

            let opts = self.global_opts.unwrap_or_default();
            let txn_db_opts = self.txn_db_opts.unwrap_or_default();
            let db = spawn_blocking({
                let path = self.path.clone();
                move || {
                    rocksdb::TransactionDB::open_cf_descriptors(&opts, &txn_db_opts, &*path, cfs)
                }
            })
            .await
            .with_context(|| format!("Failed joining task that opens database {path_d}"))?
            .with_context(|| format!("Failed opening database {path_d}"))?;

            // TODO(high): implement drop_unknown_cfs and index_rebuilders

            Ok(RocksDb::new(db))
        })
    }
}

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use eyre::WrapErr as _;
use rocksdb::{ColumnFamilyDescriptor, SingleThreaded};
use sakuhiki_core::{
    Backend as _, BackendBuilder,
    backend::{BuilderConfig, CfOptions},
};
use tokio::task::spawn_blocking;

use crate::RocksDb;

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

    fn blocking_build_without_index_rebuilding(
        self,
        mut cfs: HashMap<&'static str, CfOptions<RocksDb>>,
        drop_unknown_cfs: bool,
    ) -> eyre::Result<(RocksDb, HashSet<&'static str>)> {
        let path_d = self.path.display();

        // List pre-existing CFs
        let (options, mut preexisting_cfs) = rocksdb::Options::load_latest(
            &self.path,
            rocksdb::Env::new()?,
            true,
            rocksdb::Cache::new_lru_cache(1024),
        )
        .wrap_err_with(|| format!("Failed listing CFs for {path_d}"))?;

        // Prepare opening configuration
        let mut opened_unknown_cfs = HashSet::new();
        for cfd in &mut preexisting_cfs {
            let cf = cfd.name().to_owned();
            if let Some(CfOptions::Configured(options)) = cfs.remove(&cf as &str) {
                // Both ReuseLast and NotConfigured means we'll actually reuse the last configuration
                *cfd = ColumnFamilyDescriptor::new(&cf, options);
            } else {
                opened_unknown_cfs.insert(cf);
            }
        }

        // Open the database
        let opts = self.global_opts.unwrap_or_default();
        let txn_db_opts = self.txn_db_opts.unwrap_or_default();
        let mut db = rocksdb::TransactionDB::<SingleThreaded>::open_cf_descriptors(
            &opts,
            &txn_db_opts,
            &self.path,
            preexisting_cfs,
        )
        .wrap_err_with(|| format!("Failed opening database {path_d}"))?;

        // Drop unknown CFs
        if drop_unknown_cfs {
            for cf in opened_unknown_cfs {
                db.drop_cf(&cf)
                    .wrap_err_with(|| format!("Dropping unknown CF {cf}"))?;
            }
        }

        // Create missing CFs
        let mut created_cfs = HashSet::new();
        for (cf, options) in cfs {
            let options = match options {
                CfOptions::Configured(options) => options,
                CfOptions::NotConfigured => rocksdb::Options::default(),
                CfOptions::ReuseLast => {
                    panic!("Missing CF {cf} is configured to reuse the last options")
                }
            };
            db.create_cf(cf, &options)
                .wrap_err_with(|| format!("Creating new CF {cf}"))?;
            created_cfs.insert(cf);
        }

        Ok((RocksDb::new(db), created_cfs))
    }
}

impl BackendBuilder for RocksDbBuilder {
    type Target = RocksDb;
    type CfOptions = rocksdb::Options;

    type BuildFuture = waaa::BoxFuture<'static, eyre::Result<RocksDb>>;

    fn build(self, mut config: BuilderConfig<RocksDb>) -> Self::BuildFuture {
        Box::pin(async move {
            let path_d = self.path.display().to_string();
            let (db, created_cfs) = spawn_blocking(move || {
                self.blocking_build_without_index_rebuilding(config.cfs, config.drop_unknown_cfs)
            })
            .await
            .wrap_err_with(|| {
                format!("Failed joining task that builds the database for {path_d}")
            })??;

            // Rebuild indexes if needed
            for i in config.index_rebuilders {
                if created_cfs.contains(i.datum_cf)
                    || i.index_cfs.iter().any(|cf| created_cfs.contains(cf))
                {
                    let datum_cf = db
                        .cf_handle(i.datum_cf)
                        .await
                        .wrap_err_with(|| format!("Failed opening CF {}", i.datum_cf))?;
                    let mut index_cfs = Vec::with_capacity(i.index_cfs.len());
                    for cf in i.index_cfs {
                        index_cfs.push(
                            db.cf_handle(cf)
                                .await
                                .wrap_err_with(|| format!("Failed opening CF {}", i.datum_cf))?,
                        );
                    }
                    let t = db.start_transaction(true).await?;
                    (i.rebuilder)(&t, &index_cfs, &datum_cf)
                        .await
                        .wrap_err_with(|| format!("Rebuilding index with CFs {:?}", i.index_cfs))?;
                }
            }

            Ok(db)
        })
    }
}

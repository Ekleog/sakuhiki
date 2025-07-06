use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    ops::RangeBounds,
};

use waaa::Future;

use crate::{Db, IndexedDatum, Mode};

const SAKUHIKI_PREFIX: &str = "__sakuhiki";

// TODO(low): add OpenDAL-based backend
// TODO(low): add cache backend that has two backend layers?
pub trait Transaction<'t, B: ?Sized + Backend>
where
    Self: 't,
    B: 't,
{
    type ExclusiveLock<'op>: Send
    where
        't: 'op;

    fn take_exclusive_lock<'op>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, eyre::Result<Self::ExclusiveLock<'op>>>
    where
        't: 'op;

    fn get<'op, 'key>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, eyre::Result<Option<B::Value<'op>>>>
    where
        't: 'op,
        'op: 'key;

    // TODO(low): do we need get_many / multi_get?
    fn scan<'op, 'keys, R>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        keys: impl 'keys + RangeBounds<R>,
    ) -> waaa::BoxStream<'keys, eyre::Result<(B::Key<'op>, B::Value<'op>)>>
    where
        't: 'op,
        'op: 'keys,
        R: ?Sized + AsRef<[u8]>;

    fn scan_prefix<'op, 'key>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        prefix: &'key [u8],
    ) -> waaa::BoxStream<'key, eyre::Result<(B::Key<'op>, B::Value<'op>)>>
    where
        't: 'op,
        'op: 'key,
    {
        fn plus_one(prefix: &mut [u8]) -> bool {
            let mut prefix = prefix.to_owned();
            for b in prefix.iter_mut().rev() {
                if *b < 0xFF {
                    *b += 1;
                    return true;
                } else {
                    *b = 0;
                }
            }
            false
        }
        let mut prefix_plus_one = prefix.to_owned();
        if plus_one(&mut prefix_plus_one) {
            self.scan(cf, prefix.to_owned()..prefix_plus_one)
        } else {
            self.scan(cf, prefix..)
        }
    }

    fn put<'op, 'kv>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> waaa::BoxFuture<'kv, eyre::Result<Option<B::Value<'op>>>>
    where
        't: 'op,
        'op: 'kv;

    fn delete<'op, 'key>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, eyre::Result<Option<B::Value<'op>>>>
    where
        't: 'op,
        'op: 'key;

    fn clear<'op>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, eyre::Result<()>>;
}

pub trait Backend: 'static {
    type Builder: BackendBuilder<Target = Self>;

    type Cf<'db>: waaa::Send + waaa::Sync;

    type CfHandleFuture<'db>: waaa::Send + Future<Output = eyre::Result<Self::Cf<'db>>>;

    // Note: CF handles starting with `__sakuhiki` are reserved for implementation details.
    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db>;

    type Transaction<'t>: waaa::Send + waaa::Sync + Transaction<'t, Self>;
    type TransactionCf<'t>: BackendCf;

    fn transaction<'fut, 'db, Bcf, F, Ret>(
        &'fut self,
        mode: Mode,
        cfs: &'fut [Bcf],
        actions: F,
    ) -> waaa::BoxFuture<'fut, eyre::Result<Ret>>
    where
        Bcf: 'fut + waaa::Send + waaa::Sync + Borrow<Self::Cf<'db>>,
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(
                &'t (),
                Self::Transaction<'t>,
                Vec<Self::TransactionCf<'t>>,
            ) -> waaa::BoxFuture<'t, Ret>;

    type Key<'op>: waaa::Send + waaa::Sync + AsRef<[u8]>;
    type Value<'op>: waaa::Send + waaa::Sync + AsRef<[u8]>;
}

pub trait BackendCf: waaa::Send + waaa::Sync {
    fn name(&self) -> &'static str;
}

pub trait BackendBuilder: 'static + Sized + Send {
    type Target: Backend;
    type CfOptions;

    type BuildFuture: waaa::Send + Future<Output = eyre::Result<Self::Target>>;

    fn build(self, config: BuilderConfig<Self::Target>) -> Self::BuildFuture;
}

pub enum CfOptions<B: Backend> {
    Configured(<B::Builder as BackendBuilder>::CfOptions),
    ReuseLast,
    NotConfigured,
}

pub struct IndexRebuilder<B: Backend> {
    pub datum_cf: &'static str,
    pub index_cfs: &'static [&'static str],
    #[allow(clippy::type_complexity)]
    pub rebuilder: Box<
        dyn Send
            + for<'fut, 't> FnOnce(
                &'fut B::Transaction<'t>,
                &'fut [B::TransactionCf<'t>],
                &'fut B::TransactionCf<'t>,
            ) -> waaa::BoxFuture<'fut, eyre::Result<()>>,
    >,
}

pub struct BuilderConfig<B: Backend> {
    pub cfs: HashMap<&'static str, CfOptions<B>>,
    pub drop_unknown_cfs: bool,
    pub index_rebuilders: Vec<IndexRebuilder<B>>,
}

pub struct Builder<B: Backend> {
    builder: Option<B::Builder>,
    config: Option<BuilderConfig<B>>,
    used_cfs: HashSet<&'static str>,
    require_all_cfs_configured: bool,
    allow_extra_cf_config: bool,
}

impl<B: Backend> Builder<B> {
    pub fn new(builder: B::Builder) -> Self {
        Self {
            builder: Some(builder),
            config: Some(BuilderConfig {
                cfs: HashMap::new(),
                drop_unknown_cfs: false,
                index_rebuilders: Vec::new(),
            }),
            used_cfs: HashSet::new(),
            require_all_cfs_configured: false,
            allow_extra_cf_config: false,
        }
    }

    pub fn backend_config(&mut self, f: impl FnOnce(&mut B::Builder)) -> &mut Self {
        let builder = self.builder.as_mut().expect("Reusing consumed builder");
        (f)(builder);
        self
    }

    pub fn require_all_cfs_configured(&mut self) -> &mut Self {
        self.require_all_cfs_configured = true;
        self
    }

    pub fn allow_extra_cf_config(&mut self) -> &mut Self {
        self.allow_extra_cf_config = true;
        self
    }

    pub fn drop_unknown_cfs(&mut self) -> &mut Self {
        let config = self.config.as_mut().expect("Reusing consumed builder");
        config.drop_unknown_cfs = true;
        self
    }

    pub fn cf_options(
        &mut self,
        cf: &'static str,
        options: <B::Builder as BackendBuilder>::CfOptions,
    ) -> &mut Self {
        if cf.starts_with(SAKUHIKI_PREFIX) {
            panic!("CFs starting with {SAKUHIKI_PREFIX} are reserved for internal use");
        }
        let config = self.config.as_mut().expect("Reusing consumed builder");
        let previous = config.cfs.insert(cf, CfOptions::Configured(options));
        assert!(previous.is_none(), "Configured CF {cf} multiple times");
        self
    }

    pub fn cf_options_reuse_last(&mut self, cf: &'static str) -> &mut Self {
        if cf.starts_with(SAKUHIKI_PREFIX) {
            panic!("CFs starting with {SAKUHIKI_PREFIX} are reserved for internal use");
        }
        let config = self.config.as_mut().expect("Reusing consumed builder");
        let previous = config.cfs.insert(cf, CfOptions::ReuseLast);
        assert!(previous.is_none(), "Configured CF {cf} multiple times");
        self
    }

    pub fn datum<D: IndexedDatum<B>>(&mut self) -> &mut Self {
        fn require_cf(used_cfs: &mut HashSet<&'static str>, cf: &'static str) {
            if cf.starts_with(SAKUHIKI_PREFIX) {
                panic!("CFs starting with {SAKUHIKI_PREFIX} are reserved for internal use");
            }
            let new_insert = used_cfs.insert(cf);
            assert!(new_insert, "Multiple datum types require the same CF {cf}");
        }

        let config = self.config.as_mut().expect("Reusing consumed builder");
        require_cf(&mut self.used_cfs, D::CF);
        for i in D::INDEXES {
            for cf in i.cfs() {
                require_cf(&mut self.used_cfs, cf);
            }
            config.index_rebuilders.push(IndexRebuilder {
                datum_cf: D::CF,
                index_cfs: i.cfs(),
                rebuilder: Box::new(move |t, index_cfs, datum_cf| {
                    Box::pin(async move { i.rebuild(t, index_cfs, datum_cf).await })
                }),
            });
        }
        self
    }

    pub async fn build(&mut self) -> eyre::Result<Db<B>> {
        let mut config = self.config.take().expect("Reusing consumed builder");
        let builder = self.builder.take().expect("Reusing consumed builder");
        if self.require_all_cfs_configured {
            for cf in &self.used_cfs {
                assert!(
                    config.cfs.contains_key(cf),
                    "All CFs must be configured but {cf} is not"
                );
            }
        } else {
            for cf in &self.used_cfs {
                config.cfs.entry(cf).or_insert(CfOptions::NotConfigured);
            }
        }
        if self.allow_extra_cf_config {
            config.cfs.retain(|k, _| self.used_cfs.contains(k));
        } else {
            for cf in config.cfs.keys() {
                assert!(
                    self.used_cfs.contains(cf),
                    "Unused CF configuration for {cf}"
                );
            }
        }
        builder.build(config).await.map(Db::new)
    }
}

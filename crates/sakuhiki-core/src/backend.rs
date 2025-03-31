use std::ops::RangeBounds;

use futures_util::{TryStreamExt as _, stream};
use waaa::Future;

use crate::{CfError, Datum, Db, IndexError, IndexedDatum, Indexer};

pub trait Transaction<'t, B: ?Sized + Backend>
where
    Self: 't,
    B: 't,
{
    fn get<'op, 'key>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<B::Value<'op>>, B::Error>>
    where
        't: 'op,
        'op: 'key;

    // TODO(low): do we need get_many / multi_get?
    #[allow(clippy::type_complexity)] // No meaningful way to split the type
    fn scan<'op, 'keys, R>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        keys: impl 'keys + RangeBounds<R>,
    ) -> waaa::BoxStream<'keys, Result<(B::Key<'op>, B::Value<'op>), B::Error>>
    where
        't: 'op,
        'op: 'keys,
        R: ?Sized + AsRef<[u8]>;

    #[allow(clippy::type_complexity)]
    fn scan_prefix<'op, 'key>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        prefix: &'key [u8],
    ) -> waaa::BoxStream<'key, Result<(B::Key<'op>, B::Value<'op>), B::Error>>
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
    ) -> waaa::BoxFuture<'kv, Result<Option<B::Value<'op>>, B::Error>>
    where
        't: 'op,
        'op: 'kv;

    fn delete<'op, 'key>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<B::Value<'op>>, B::Error>>
    where
        't: 'op,
        'op: 'key;

    fn clear<'op>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'op, Result<(), B::Error>>;
}

macro_rules! make_transaction_fn {
    ($name:ident) => {
        fn $name<'fut, 'db, F, Ret>(
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
                ) -> waaa::BoxFuture<'t, Ret>;
    };
}

pub trait Backend: 'static {
    type Error: waaa::Send + waaa::Sync + std::error::Error;

    type Builder: BackendBuilder<Target = Self>;

    type Cf<'db>: waaa::Send + waaa::Sync;

    type CfHandleFuture<'db>: waaa::Send + Future<Output = Result<Self::Cf<'db>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db>;

    type Transaction<'t>: waaa::Send + waaa::Sync + Transaction<'t, Self>;
    type TransactionCf<'t>: BackendCf;

    make_transaction_fn!(ro_transaction);
    make_transaction_fn!(rw_transaction);

    type Key<'op>: waaa::Send + waaa::Sync + AsRef<[u8]>;
    type Value<'op>: waaa::Send + waaa::Sync + AsRef<[u8]>;
}

pub trait BackendCf: waaa::Send + waaa::Sync {
    fn name(&self) -> &'static str;
}

pub trait BackendBuilder: 'static + Sized + Send {
    type Target: Backend;

    #[allow(clippy::type_complexity)]
    fn datum<D: IndexedDatum<Self::Target>>(
        self,
    ) -> waaa::BoxFuture<
        'static,
        Result<Self, IndexError<<Self::Target as Backend>::Error, anyhow::Error>>,
    > {
        Box::pin(async move {
            let this = self
                .build_datum_cf(D::CF)
                .await
                .map_err(|e| IndexError::Backend(CfError::new(D::CF, e)))?;
            stream::iter(D::INDEXES.iter().map(Ok))
                .try_fold(this, |this, i| async move {
                    this.build_index_cf(*i).await.map_err(|e| match e {
                        IndexError::Backend(e) => IndexError::Backend(e),
                        IndexError::Parsing(e) => IndexError::Parsing(
                            anyhow::Error::from(e)
                                .context(format!("building index cfs '{:?}'", i.cfs())),
                        ),
                    })
                })
                .await
        })
    }

    fn build_datum_cf(
        self,
        cf: &'static str,
    ) -> waaa::BoxFuture<'static, Result<Self, <Self::Target as Backend>::Error>>;

    #[allow(clippy::type_complexity)]
    fn build_index_cf<I: ?Sized + Indexer<Self::Target>>(
        self,
        index: &I,
    ) -> waaa::BoxFuture<
        '_,
        Result<Self, IndexError<<Self::Target as Backend>::Error, <I::Datum as Datum>::Error>>,
    >;

    fn drop_unknown_cfs(
        self,
    ) -> waaa::BoxFuture<'static, Result<Self, <Self::Target as Backend>::Error>>;

    type BuildFuture: waaa::Send
        + Future<
            Output = Result<
                Db<Self::Target>,
                IndexError<<Self::Target as Backend>::Error, anyhow::Error>,
            >,
        >;

    fn build(self) -> Self::BuildFuture;
}

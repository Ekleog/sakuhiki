use std::ops::RangeBounds;

use waaa::Future;

use crate::{CfError, DbBuilder, IndexError};

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

    fn builder() -> DbBuilder<Self::Builder>;

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

pub struct CfBuilder<B>
where
    B: Backend,
{
    pub cfs: Vec<&'static str>,
    pub builds_using: Option<&'static str>,
    #[allow(clippy::type_complexity)]
    pub builder: Box<
        dyn Send
            + for<'t> FnOnce(
                &'t (),
                B::Transaction<'t>,
                Vec<B::TransactionCf<'t>>,
                Option<B::TransactionCf<'t>>,
            )
                -> waaa::BoxFuture<'t, Result<(), IndexError<B::Error, anyhow::Error>>>,
    >,
}

pub trait BackendBuilder {
    type Target: Backend;

    type BuildFuture: waaa::Send
        + Future<
            Output = Result<
                Self::Target,
                IndexError<<Self::Target as Backend>::Error, anyhow::Error>,
            >,
        >;

    fn build(self, cf_builder_list: Vec<CfBuilder<Self::Target>>) -> Self::BuildFuture;
}

use std::ops::RangeBounds;

use waaa::Future;

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
    fn scan<'op, 'keys>(
        &'op self,
        cf: &'op B::TransactionCf<'t>,
        keys: impl 'keys + RangeBounds<[u8]>,
    ) -> waaa::BoxStream<'keys, Result<(B::Key<'op>, B::Value<'op>), B::Error>>
    where
        't: 'op,
        'op: 'keys;

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
}

pub trait Backend: 'static {
    type Error: waaa::Send + waaa::Sync + std::error::Error;

    type Cf<'db>: waaa::Send + waaa::Sync;

    type Key<'op>: waaa::Send + waaa::Sync + AsRef<[u8]>;
    type Value<'op>: waaa::Send + waaa::Sync + AsRef<[u8]>;

    type CfHandleFuture<'db>: Future<Output = Result<Self::Cf<'db>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &'static str) -> Self::CfHandleFuture<'db>;

    type Transaction<'t>: waaa::Send + waaa::Sync + Transaction<'t, Self>;
    type TransactionCf<'t>: BackendCf;

    fn transaction<'fut, 'db, F, Ret>(
        &'fut self,
        cfs: &'fut [&'fut Self::Cf<'db>],
        actions: F,
    ) -> waaa::BoxFuture<'fut, Result<Ret, Self::Error>>
    where
        F: 'fut
            + waaa::Send
            + for<'t> FnOnce(
                &'t (),
                Self::Transaction<'t>,
                Vec<Self::TransactionCf<'t>>,
            ) -> waaa::BoxFuture<'t, Ret>;
}

pub trait BackendCf: waaa::Send + waaa::Sync {
    fn name(&self) -> &'static str;
}

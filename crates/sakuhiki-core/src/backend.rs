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
            return false;
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
}

macro_rules! make_transaction_fn {
    ($name:ident) => {
        fn $name<'fut, 'db, F, Ret>(
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
    };
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

    make_transaction_fn!(ro_transaction);
    make_transaction_fn!(rw_transaction);
}

pub trait BackendCf: waaa::Send + waaa::Sync {
    fn name(&self) -> &'static str;
}

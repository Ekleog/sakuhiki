use std::ops::RangeBounds;

use waaa::Future;

macro_rules! ro_transaction_fns {
    ($t:lifetime, $cf:ident) => {
        fn get<'op, 'key>(
            &'op self,
            cf: &'op B::$cf<$t>,
            key: &'key [u8],
        ) -> waaa::BoxFuture<'key, Result<Option<B::Value<'op>>, B::Error>>
        where
            $t: 'op,
            'op: 'key;

        // TODO(low): do we need get_many / multi_get?
        fn scan<'op, 'keys>(
            &'op self,
            cf: &'op B::$cf<$t>,
            keys: impl 'keys + RangeBounds<[u8]>,
        ) -> waaa::BoxStream<'keys, Result<(B::Key<'op>, B::Value<'op>), B::Error>>
        where
            't: 'op,
            'op: 'keys;
    };
}

pub trait RoTransaction<'t, B: ?Sized + Backend>
where
    Self: 't,
    B: 't,
{
    ro_transaction_fns!('t, RoTransactionCf);
}

pub trait RwTransaction<'t, B: ?Sized + Backend>
where
    Self: 't,
    B: 't,
{
    ro_transaction_fns!('t, RwTransactionCf);

    fn put<'op, 'kv>(
        &'op self,
        cf: &'op B::RwTransactionCf<'t>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> waaa::BoxFuture<'kv, Result<Option<B::Value<'op>>, B::Error>>
    where
        't: 'op,
        'op: 'kv;

    fn delete<'op, 'key>(
        &'op self,
        cf: &'op B::RwTransactionCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<B::Value<'op>>, B::Error>>
    where
        't: 'op,
        'op: 'key;
}

macro_rules! transaction_fn {
    ($fn:ident, $cf:ident, $transac:ident) => {
        type $cf<'t>: BackendCf;

        type $transac<'t>: waaa::Send + waaa::Sync + $transac<'t, Self>;

        fn $fn<'fut, 'db, F, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Self::Cf<'db>],
            actions: F,
        ) -> waaa::BoxFuture<'fut, Result<Ret, Self::Error>>
        where
            F: 'fut
                + waaa::Send
                + for<'t> FnOnce(
                    &'t (),
                    Self::$transac<'t>,
                    Vec<Self::$cf<'t>>,
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

    transaction_fn!(ro_transaction, RoTransactionCf, RoTransaction);
    transaction_fn!(rw_transaction, RwTransactionCf, RwTransaction);
}

pub trait BackendCf: waaa::Send + waaa::Sync {
    fn name(&self) -> &'static str;
}

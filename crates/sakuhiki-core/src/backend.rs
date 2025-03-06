use std::ops::RangeBounds;

use waaa::Future;

macro_rules! ro_transaction_fns {
    ($t:lifetime, $cf:ident) => {
        fn get<'op, 'key>(
            &'op mut self,
            cf: &'op mut B::$cf<$t>,
            key: &'key [u8],
        ) -> waaa::BoxFuture<'key, Result<Option<B::Value<'op>>, B::Error>>
        where
            $t: 'op,
            'op: 'key;

        // TODO: do we need get_many / multi_get?
        fn scan<'op, 'keys>(
            &'op mut self,
            cf: &'op mut B::$cf<$t>,
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

    fn put<'op>(
        &'op mut self,
        cf: &'op mut B::RwTransactionCf<'t>,
        key: &'op [u8],
        value: &'op [u8],
    ) -> waaa::BoxFuture<'op, Result<(), B::Error>>
    where
        't: 'op;

    fn delete<'op>(
        &'op mut self,
        cf: &'op mut B::RwTransactionCf<'t>,
        key: &'op [u8],
    ) -> waaa::BoxFuture<'op, Result<(), B::Error>>
    where
        't: 'op;
}

macro_rules! transaction_fn {
    ($fn:ident, $cf:ident, $transac:ident) => {
        type $cf<'t>: waaa::Send;

        type $transac<'t>: waaa::Send + $transac<'t, Self>;

        fn $fn<'fut, const CFS: usize, F, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Self::Cf<'fut>; CFS],
            actions: F,
        ) -> waaa::BoxFuture<'fut, Result<Ret, Self::Error>>
        where
            F: 'fut
                + waaa::Send
                + for<'t> FnOnce(
                    &'t mut Self::$transac<'t>,
                    [Self::$cf<'t>; CFS],
                ) -> waaa::BoxFuture<'t, Ret>;
    };
}

pub trait Backend: 'static {
    type Error;

    type Cf<'db>;

    type Key<'op>: AsRef<[u8]>;

    type Value<'op>: AsRef<[u8]>;

    type CfHandleFuture<'db>: Future<Output = Result<Self::Cf<'db>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &str) -> Self::CfHandleFuture<'db>;

    transaction_fn!(ro_transaction, RoTransactionCf, RoTransaction);
    transaction_fn!(rw_transaction, RwTransactionCf, RwTransaction);
}

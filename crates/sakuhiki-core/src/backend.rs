use std::ops::RangeBounds;

use futures_util::Stream;
use waaa::Future;

macro_rules! ro_transaction_fns {
    ($t:lifetime, $cf:ident) => {
        type GetFuture<'op, 'key>: Future<Output = Result<Option<B::Value<'op>>, B::Error>>
        where
            $t: 'op,
            'op: 'key;

        fn get<'op, 'key>(
            &'op mut self,
            cf: &'op mut B::$cf<$t>,
            key: &'key [u8],
        ) -> Self::GetFuture<'op, 'key>
        where
            $t: 'op,
            'op: 'key;

        type ScanStream<'op, 'keys>: waaa::Send
            + Stream<Item = Result<(B::Key<'op>, B::Value<'op>), B::Error>>
        where
            $t: 'op,
            'op: 'keys;

        // TODO: do we need get_many / multi_get?
        fn scan<'op, 'keys>(
            &'op mut self,
            cf: &'op mut B::$cf<$t>,
            keys: impl 'keys + RangeBounds<[u8]>,
        ) -> Self::ScanStream<'op, 'keys>
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

    type PutFuture<'op>: Future<Output = Result<(), B::Error>>
    where
        't: 'op;

    fn put<'op>(
        &'op mut self,
        cf: &'op mut B::RwTransactionCf<'t>,
        key: &'op [u8],
        value: &'op [u8],
    ) -> Self::PutFuture<'op>
    where
        't: 'op;

    type DeleteFuture<'op>: Future<Output = Result<(), B::Error>>
    where
        't: 'op;

    fn delete<'op>(
        &'op mut self,
        cf: &'op mut B::RwTransactionCf<'t>,
        key: &'op [u8],
    ) -> Self::DeleteFuture<'op>
    where
        't: 'op;
}

macro_rules! transaction_fn {
    ($fn:ident, $cf:ident, $transac:ident, $transacfut:ident) => {
        type $cf<'t>;

        type $transac<'t>: $transac<'t, Self>;

        type $transacfut<'t, F, Return>: Future<Output = Result<Return, Self::Error>>
        where
            F: 't;

        fn $fn<'fut, const CFS: usize, F, RetFut, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Self::Cf<'fut>; CFS],
            actions: F,
        ) -> Self::$transacfut<'fut, F, Ret>
        where
            F: 'fut
                + waaa::Send
                + for<'t> FnOnce(&'t mut Self::$transac<'t>, [Self::$cf<'t>; CFS]) -> RetFut,
            RetFut: Future<Output = Ret>;
    };
}

pub trait Backend: 'static {
    type Error;

    type Cf<'db>;

    type Key<'op>: AsRef<[u8]>;

    type Value<'op>: AsRef<[u8]>;

    type CfHandleFuture<'db>: Future<Output = Result<Self::Cf<'db>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &str) -> Self::CfHandleFuture<'db>;

    transaction_fn!(
        ro_transaction,
        RoTransactionCf,
        RoTransaction,
        RoTransactionFuture
    );

    transaction_fn!(
        rw_transaction,
        RwTransactionCf,
        RwTransaction,
        RwTransactionFuture
    );
}

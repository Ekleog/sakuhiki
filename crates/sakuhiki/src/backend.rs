use std::{future::Future, ops::RangeBounds};

use futures_util::Stream;

macro_rules! ro_transaction_fns {
    ($t:lifetime, $cf:ident) => {
        type GetFuture<'db, 'key>: Future<Output = Result<Option<B::Value<'db>>, B::Error>>
        where
            $t: 'db,
            'db: 'key;

        fn get<'db, 'key>(
            &'db mut self,
            cf: &'db mut B::$cf<$t>,
            key: &'key [u8],
        ) -> Self::GetFuture<'db, 'key>
        where
            $t: 'db,
            'db: 'key;

        type ScanStream<'db, 'keys>: Stream<Item = Result<(B::Key<'db>, B::Value<'db>), B::Error>>
        where
            $t: 'db,
            'db: 'keys;

        // TODO: do we need get_many / multi_get?
        fn scan<'db, 'keys>(
            &'db mut self,
            cf: &'db mut B::$cf<$t>,
            keys: impl 'keys + RangeBounds<[u8]>,
        ) -> Self::ScanStream<'db, 'keys>
        where
            't: 'db,
            'db: 'keys;
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

    type PutFuture<'db>: Future<Output = Result<(), B::Error>>
    where
        't: 'db;

    fn put<'db>(
        &'db mut self,
        cf: &'db mut B::RwTransactionCf<'t>,
        key: &'db [u8],
        value: &'db [u8],
    ) -> Self::PutFuture<'db>
    where
        't: 'db;

    type DeleteFuture<'db>: Future<Output = Result<(), B::Error>>
    where
        't: 'db;

    fn delete<'db>(
        &'db mut self,
        cf: &'db mut B::RwTransactionCf<'t>,
        key: &'db [u8],
    ) -> Self::DeleteFuture<'db>
    where
        't: 'db;
}

macro_rules! transaction_fn {
    ($fn:ident, $cf:ident, $transac:ident, $transacfut:ident) => {
        type $cf<'t>
        where
            Self: 't;

        type $transac<'t>: $transac<'t, Self>
        where
            Self: 't;

        type $transacfut<'t, F, Return>: Future<Output = Result<Return, Self::Error>>
        where
            Self: 't,
            F: 't;

        fn $fn<'fut, const CFS: usize, F, RetFut, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Self::Cf<'fut>; CFS],
            actions: F,
        ) -> Self::$transacfut<'fut, F, Ret>
        where
            F: 'fut + for<'t> FnOnce(&'t mut Self::$transac<'t>, [Self::$cf<'t>; CFS]) -> RetFut,
            RetFut: Future<Output = Ret>;
    };
}

pub trait Backend {
    type Error;

    type Cf<'db>
    where
        Self: 'db;

    type Key<'db>: AsRef<[u8]>
    where
        Self: 'db;

    type Value<'db>: AsRef<[u8]>
    where
        Self: 'db;

    type CfHandleFuture<'db>: Future<Output = Result<Self::Cf<'db>, Self::Error>>
    where
        Self: 'db;

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

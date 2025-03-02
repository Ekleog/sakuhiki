use std::{future::Future, ops::RangeBounds};

use futures_util::Stream;

use crate::{
    backend::{self, RoTransaction as _},
    Backend,
};

pub struct Db<B> {
    backend: B,
}

impl<B> Db<B>
where
    B: Backend,
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn cf_handle(&self, name: &str) -> Result<B::Cf<'_>, B::Error> {
        self.backend.cf_handle(name).await
    }

    pub async fn ro_transaction<'fut, const CFS: usize, F, RetFut, Ret>(
        &'fut self,
        cfs: &'fut [&'fut B::Cf<'fut>; CFS],
        actions: F,
    ) -> Result<Ret, B::Error>
    where
        F: 'fut + for<'t> FnOnce(&RoTransaction<'t, B>, [B::RoTransactionCf<'t>; CFS]) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        self.backend
            .ro_transaction(cfs, move |transaction, cfs| {
                actions(&RoTransaction { transaction }, cfs)
            })
            .await
    }

    pub async fn rw_transaction<'fut, const CFS: usize, F, RetFut, Ret>(
        &'fut self,
        cfs: &'fut [&'fut B::Cf<'fut>; CFS],
        actions: F,
    ) -> Result<Ret, B::Error>
    where
        F: 'fut + for<'t> FnOnce(&RwTransaction<'t, B>, [B::RwTransactionCf<'t>; CFS]) -> RetFut,
        RetFut: Future<Output = Ret>,
    {
        self.backend
            .rw_transaction(cfs, move |_transaction, cfs| {
                actions(&RwTransaction { _transaction }, cfs)
            })
            .await
    }
}

pub type Key<'t, 'db, B> = <<B as Backend>::RoTransaction<'t> as backend::RoTransaction<
    <B as Backend>::RoTransactionCf<'t>,
    <B as Backend>::Error,
>>::Key<'db>;

pub type Value<'t, 'db, B> = <<B as Backend>::RoTransaction<'t> as backend::RoTransaction<
    <B as Backend>::RoTransactionCf<'t>,
    <B as Backend>::Error,
>>::Value<'db>;

pub struct RoTransaction<'t, B>
where
    B: 't + Backend,
{
    transaction: &'t mut B::RoTransaction<'t>,
}

impl<'t, B> RoTransaction<'t, B>
where
    B: Backend,
{
    pub async fn get<'db, 'key>(
        &'db mut self,
        cf: &'db mut B::RoTransactionCf<'t>,
        key: &'key [u8],
    ) -> Result<Option<Value<'t, 'db, B>>, B::Error> {
        self.transaction.get(cf, key).await
    }

    pub fn scan<'db, 'keys>(
        &'db mut self,
        cf: &'db mut B::RoTransactionCf<'t>,
        keys: impl 'keys + RangeBounds<[u8]>,
    ) -> impl 'keys + Stream<Item = Result<(Key<'t, 'db, B>, Value<'t, 'db, B>), B::Error>>
    where
        'db: 'keys,
    {
        self.transaction.scan(cf, keys)
    }
}

pub struct RwTransaction<'t, B>
where
    B: 't + Backend,
{
    _transaction: &'t mut B::RwTransaction<'t>,
}

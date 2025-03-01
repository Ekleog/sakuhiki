use std::future::Future;

use crate::Backend;

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
            .ro_transaction(cfs, move |_transaction, cfs| {
                actions(&RoTransaction { _transaction }, cfs)
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

pub struct RoTransaction<'t, B>
where
    B: 't + Backend,
{
    _transaction: &'t B::RoTransaction<'t>,
}

pub struct RwTransaction<'t, B>
where
    B: 't + Backend,
{
    _transaction: &'t B::RwTransaction<'t>,
}

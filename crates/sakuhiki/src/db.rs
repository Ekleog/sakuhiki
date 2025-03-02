use std::{future::Future, ops::RangeBounds};

use futures_util::Stream;

use crate::{Backend, backend::RoTransaction as _, backend::RwTransaction as _};

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
            .rw_transaction(cfs, move |transaction, cfs| {
                actions(&RwTransaction { transaction }, cfs)
            })
            .await
    }
}

pub struct RoTransaction<'t, B>
where
    B: 't + Backend,
{
    transaction: &'t mut B::RoTransaction<'t>,
}

macro_rules! ro_transaction_methods {
    ($cf:ident) => {
        pub async fn get<'db, 'key>(
            &'db mut self,
            cf: &'db mut B::$cf<'t>,
            key: &'key [u8],
        ) -> Result<Option<B::Value<'db>>, B::Error> {
            self.transaction.get(cf, key).await
        }

        pub fn scan<'db, 'keys, Keys>(
            &'db mut self,
            cf: &'db mut B::$cf<'t>,
            keys: Keys,
        ) -> impl Stream<Item = Result<(B::Key<'db>, B::Value<'db>), B::Error>>
        + use<'t, 'db, 'keys, B, Keys>
        where
            Keys: 'keys + RangeBounds<[u8]>,
            'db: 'keys,
        {
            self.transaction.scan(cf, keys)
        }
    };
}

impl<'t, B> RoTransaction<'t, B>
where
    B: Backend,
{
    ro_transaction_methods!(RoTransactionCf);
}

pub struct RwTransaction<'t, B>
where
    B: 't + Backend,
{
    transaction: &'t mut B::RwTransaction<'t>,
}

impl<'t, B> RwTransaction<'t, B>
where
    B: Backend,
{
    ro_transaction_methods!(RwTransactionCf);

    pub async fn put<'db>(
        &'db mut self,
        cf: &'db mut B::RwTransactionCf<'t>,
        key: &'db [u8],
        value: &'db [u8],
    ) -> Result<(), B::Error> {
        self.transaction.put(cf, key, value).await
    }
}

use std::{future::Future, ops::RangeBounds};

use futures_util::Stream;

use crate::{
    Backend, Datum, Index,
    backend::{RoTransaction as _, RwTransaction as _},
};

#[derive(Debug, thiserror::Error)]
pub enum RebuildIndexError<B, I>
where
    B: Backend,
    I: Index<B>,
{
    // TODO: improve on this
    #[error(transparent)]
    Backend(B::Error),

    #[error(transparent)]
    Parsing(<I::Datum as Datum<B>>::Error),
}

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

    pub async fn rebuild_index<I: Index<B>>(
        &self,
        _index: &I,
    ) -> Result<(), RebuildIndexError<B, I>> {
        // Note: NEED TO BLOCK PUTS WHILE THE TRANSACTION IS IN PROGRESS
        todo!()
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
        pub async fn get<'op, 'key>(
            &'op mut self,
            cf: &'op mut B::$cf<'t>,
            key: &'key [u8],
        ) -> Result<Option<B::Value<'op>>, B::Error> {
            self.transaction.get(cf, key).await
        }

        pub fn scan<'op, 'keys, Keys>(
            &'op mut self,
            cf: &'op mut B::$cf<'t>,
            keys: Keys,
        ) -> impl Stream<Item = Result<(B::Key<'op>, B::Value<'op>), B::Error>>
        + use<'t, 'op, 'keys, B, Keys>
        where
            Keys: 'keys + RangeBounds<[u8]>,
            'op: 'keys,
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

    pub async fn put<'op>(
        &'op mut self,
        cf: &'op mut B::RwTransactionCf<'t>,
        key: &'op [u8],
        value: &'op [u8],
    ) -> Result<(), B::Error> {
        self.transaction.put(cf, key, value).await
    }

    pub async fn delete<'op>(
        &'op mut self,
        cf: &'op mut B::RwTransactionCf<'t>,
        key: &'op [u8],
    ) -> Result<(), B::Error> {
        self.transaction.delete(cf, key).await
    }
}

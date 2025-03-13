use std::{iter, ops::RangeBounds};

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use waaa::Stream;

use crate::{
    Backend, IndexError, IndexedDatum, Indexer,
    backend::{RoTransaction as _, RwTransaction as _},
};

pub struct Db<B> {
    backend: B,
}

macro_rules! transaction_fn {
    ($fn:ident, $transac:ident, $cf:ident) => {
        pub async fn $fn<'fut, const CFS: usize, F, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Cf<'fut, B>; CFS],
            actions: F,
        ) -> Result<Ret, B::Error>
        where
            F: 'fut
                + waaa::Send
                + for<'t> FnOnce($transac<'t, B>, [$cf<'t, B>; CFS]) -> waaa::BoxFuture<'t, Ret>,
        {
            let backend_cfs = cfs
                .iter()
                .flat_map(|cf| {
                    iter::once(&cf.datum_cf).chain(cf.indexes_cfs.iter().flat_map(|v| v.iter()))
                })
                .collect::<Vec<_>>();
            let num_backend_cfs = backend_cfs.len();
            self.backend
                .$fn(&backend_cfs, move |transaction, mut backend_cfs| {
                    debug_assert!(num_backend_cfs == backend_cfs.len());
                    let mut inner_cfs = Vec::with_capacity(CFS);
                    for d in 0..CFS {
                        let datum_cf;
                        (datum_cf, backend_cfs) = backend_cfs.split_first_mut().unwrap();
                        let mut indexes_cfs = Vec::with_capacity(cfs[d].indexes_cfs.len());
                        for i in cfs[d].indexes_cfs.iter() {
                            let backend_indexes_cfs;
                            (backend_indexes_cfs, backend_cfs) = backend_cfs.split_at_mut(i.len());
                            indexes_cfs.push(backend_indexes_cfs);
                        }
                        inner_cfs.push($cf {
                            datum_cf,
                            indexes_cfs,
                        });
                    }
                    debug_assert!(backend_cfs.is_empty());
                    let Ok(inner_cfs) = inner_cfs.try_into() else {
                        panic!("unexpected number of cfs");
                    };
                    actions($transac { transaction }, inner_cfs)
                })
                .await
        }
    };
}

impl<B> Db<B>
where
    B: Backend,
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn rebuild_index<I: Indexer<B>>(
        &self,
        _index: &I,
    ) -> Result<(), IndexError<B, I::Datum>> {
        // Note: NEED TO BLOCK PUTS WHILE THE TRANSACTION IS IN PROGRESS
        todo!()
    }

    pub async fn cf_handle<D>(&self) -> Result<Cf<'_, B>, CfHandleError<B::Error>>
    where
        D: IndexedDatum<B>,
    {
        Ok(Cf {
            datum_cf: self
                .backend
                .cf_handle(D::CF)
                .await
                .map_err(|error| CfHandleError { cf: D::CF, error })?,
            indexes_cfs: stream::iter(D::INDICES)
                .then(|i| {
                    stream::iter(i.cfs())
                        .then(async |cf| {
                            self.backend
                                .cf_handle(cf)
                                .await
                                .map_err(|error| CfHandleError { cf, error })
                        })
                        .try_collect()
                })
                .try_collect()
                .await?,
        })
    }

    transaction_fn!(ro_transaction, RoTransaction, RoTransactionCf);
    transaction_fn!(rw_transaction, RwTransaction, RwTransactionCf);
}

#[derive(Debug, thiserror::Error)]
#[error("getting cf handle for {cf:?}")]
pub struct CfHandleError<E> {
    pub cf: &'static str,

    #[source]
    pub error: E,
}

pub struct Cf<'db, B>
where
    B: Backend,
{
    datum_cf: B::Cf<'db>,
    indexes_cfs: Vec<Vec<B::Cf<'db>>>,
}

macro_rules! transaction_structs {
    ($transac:ident, $cf:ident) => {
        pub struct $transac<'t, B>
        where
            B: 't + Backend,
        {
            transaction: &'t mut B::$transac<'t>,
        }

        pub struct $cf<'t, B>
        where
            B: Backend,
        {
            datum_cf: &'t mut B::$cf<'t>,
            indexes_cfs: Vec<&'t mut [B::$cf<'t>]>,
        }
    };
}

transaction_structs!(RoTransaction, RoTransactionCf);
transaction_structs!(RwTransaction, RwTransactionCf);

macro_rules! ro_transaction_methods {
    ($cf:ident) => {
        pub async fn get<'op, 'key>(
            &'op mut self,
            cf: &'op mut $cf<'t, B>,
            key: &'key [u8],
        ) -> Result<Option<B::Value<'op>>, B::Error> {
            self.transaction.get(cf.datum_cf, key).await
        }

        pub fn scan<'op, 'keys, Keys>(
            &'op mut self,
            cf: &'op mut $cf<'t, B>,
            keys: Keys,
        ) -> impl Stream<Item = Result<(B::Key<'op>, B::Value<'op>), B::Error>>
        + use<'t, 'op, 'keys, B, Keys>
        where
            Keys: 'keys + RangeBounds<[u8]>,
            'op: 'keys,
        {
            self.transaction.scan(cf.datum_cf, keys)
        }
    };
}

impl<'t, B> RoTransaction<'t, B>
where
    B: Backend,
{
    ro_transaction_methods!(RoTransactionCf);
}

impl<'t, B> RwTransaction<'t, B>
where
    B: Backend,
{
    ro_transaction_methods!(RwTransactionCf);

    pub async fn put<'op>(
        &'op mut self,
        cf: &'op mut RwTransactionCf<'t, B>,
        key: &'op [u8],
        value: &'op [u8],
    ) -> Result<(), B::Error> {
        self.transaction.put(cf.datum_cf, key, value).await
    }

    pub async fn delete<'op>(
        &'op mut self,
        cf: &'op mut RwTransactionCf<'t, B>,
        key: &'op [u8],
    ) -> Result<(), B::Error> {
        self.transaction.delete(cf.datum_cf, key).await
    }
}

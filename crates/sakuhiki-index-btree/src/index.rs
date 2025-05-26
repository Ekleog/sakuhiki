use futures_util::StreamExt as _;
use sakuhiki_core::{
    Backend, CfError, Datum, Index, IndexError, Indexer,
    backend::{BackendCf as _, Transaction as _},
    indexer,
};

use crate::{BTreeQuery, Key, query::Query};

pub struct BTreeIndex<K> {
    cf: &'static [&'static str; 1],
    key: K,
}

impl<K> BTreeIndex<K> {
    pub const fn new(cf: &'static [&'static str; 1], key: K) -> Self {
        Self { cf, key }
    }
}

#[warn(clippy::missing_trait_methods)]
impl<B, K> Indexer<B> for BTreeIndex<K>
where
    B: Backend,
    K: Key,
{
    type Datum = K::Datum;

    fn cfs(&self) -> &'static [&'static str] {
        self.cf
    }

    fn index<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), CfError<B::Error>>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(self.key.len_hint(datum) + object_key.len());
            let do_index = self.key.extract_key(datum, &mut key);
            if do_index {
                key.extend(object_key);
                transaction
                    .put(&cfs[0], &key, &[])
                    .await
                    .map_err(|e| CfError::cf(cfs[0].name(), e))?;
            }
            Ok(())
        })
    }

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), CfError<B::Error>>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(self.key.len_hint(datum) + object_key.len());
            let do_index = self.key.extract_key(datum, &mut key);
            if do_index {
                key.extend(object_key);
                transaction
                    .put(&cfs[0], &key, &[])
                    .await
                    .map_err(|e| CfError::cf(cfs[0].name(), e))?;
            }
            Ok(())
        })
    }

    fn index_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<
        'fut,
        Result<(), IndexError<<B as Backend>::Error, <K::Datum as Datum>::Error>>,
    > {
        Box::pin(async move {
            let mut key = Vec::with_capacity(
                self.key
                    .len_hint_from_slice(slice)
                    .map_err(IndexError::Parsing)?
                    + object_key.len(),
            );
            let do_index = self
                .key
                .extract_key_from_slice(slice, &mut key)
                .map_err(IndexError::Parsing)?;
            if do_index {
                key.extend(object_key);
                transaction
                    .put(&cfs[0], &key, &[])
                    .await
                    .map_err(|e| IndexError::Backend(CfError::cf(cfs[0].name(), e)))?;
            }
            Ok(())
        })
    }

    fn unindex_from_slice<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        slice: &'fut [u8],
        transaction: &'fut B::Transaction<'t>,
        cfs: &'fut [B::TransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, <K::Datum as Datum>::Error>>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(
                self.key
                    .len_hint_from_slice(slice)
                    .map_err(IndexError::Parsing)?
                    + object_key.len(),
            );
            let do_index = self
                .key
                .extract_key_from_slice(slice, &mut key)
                .map_err(IndexError::Parsing)?;
            if do_index {
                key.extend(object_key);
                transaction
                    .put(&cfs[0], &key, &[])
                    .await
                    .map_err(|e| IndexError::Backend(CfError::cf(cfs[0].name(), e)))?;
            }
            Ok(())
        })
    }

    fn rebuild<'fut, 't>(
        &'fut self,
        transaction: &'fut B::Transaction<'t>,
        index_cfs: &'fut [B::TransactionCf<'t>],
        datum_cf: &'fut B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, Result<(), IndexError<B::Error, <Self::Datum as Datum>::Error>>>
    {
        Box::pin(async move {
            indexer::default_rebuild::<B, Self>(self, transaction, index_cfs, datum_cf).await
        })
    }
}

pub struct BTreeQueryKey<'k, B>
where
    B: Backend,
{
    key: B::Key<'k>,
    start: usize,
}

impl<B> AsRef<[u8]> for BTreeQueryKey<'_, B>
where
    B: Backend,
{
    fn as_ref(&self) -> &[u8] {
        &self.key.as_ref()[self.start..]
    }
}

#[warn(clippy::missing_trait_methods)]
impl<B, K> Index<B> for BTreeIndex<K>
where
    B: Backend,
    K: Key,
{
    type Query<'q> = BTreeQuery<'q, K>;
    type QueryKey<'k> = BTreeQueryKey<'k, B>;

    fn query<'q, 'op: 'q, 't: 'op>(
        &'q self,
        query: &'q Self::Query<'q>,
        transaction: &'op B::Transaction<'t>,
        object_cf: &'op B::TransactionCf<'t>,
        cfs: &'op [B::TransactionCf<'t>],
    ) -> waaa::BoxStream<'q, Result<(Self::QueryKey<'op>, B::Value<'op>), CfError<B::Error>>> {
        #[allow(clippy::type_complexity)]
        let on_each_result = async |res: Result<(B::Key<'op>, B::Value<'op>), B::Error>| -> Result<
            (Self::QueryKey<'op>, B::Value<'op>),
            CfError<B::Error>,
        > {
            let (index_key, _) = res.map_err(|e| CfError::cf(cfs[0].name(), e))?;
            let key_len = self.key.key_len(index_key.as_ref());
            let object_key = BTreeQueryKey {
                key: index_key,
                start: key_len,
            };
            let object_value = transaction
                .get(object_cf, object_key.as_ref())
                .await
                .map_err(|e| CfError::cf(object_cf.name(), e))?
                .expect("Object was present in index but not in real table");
            Ok((object_key, object_value))
        };
        Box::pin(match query.query {
            Query::Prefix(prefix) => transaction
                .scan_prefix(&cfs[0], prefix)
                .then(on_each_result),
            Query::Range { start, end } => transaction
                .scan::<[u8]>(&cfs[0], (start, end))
                .then(on_each_result),
        })
    }
}

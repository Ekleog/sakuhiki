use eyre::WrapErr as _;
use futures_util::StreamExt as _;
use sakuhiki_core::{
    Backend, CfOperationError, Index, Indexer,
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
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(self.key.len_hint(datum) + object_key.len());
            let do_index = self.key.extract_key(datum, &mut key);
            if do_index {
                key.extend(object_key);
                transaction
                    .put(&cfs[0], &key, &[])
                    .await
                    .wrap_err_with(|| {
                        CfOperationError::new("Failed putting key into", cfs[0].name())
                    })?;
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
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(self.key.len_hint(datum) + object_key.len());
            let do_unindex = self.key.extract_key(datum, &mut key);
            if do_unindex {
                key.extend(object_key);
                transaction.delete(&cfs[0], &key).await.wrap_err_with(|| {
                    CfOperationError::new("Failed deleting key from", cfs[0].name())
                })?;
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
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(
                self.key
                    .len_hint_from_slice(slice)
                    .wrap_err("Failed estimating key length from slice")?
                    + object_key.len(),
            );
            let do_index = self
                .key
                .extract_key_from_slice(slice, &mut key)
                .wrap_err("Failed extracting key from slice")?;
            if do_index {
                key.extend(object_key);
                transaction
                    .put(&cfs[0], &key, &[])
                    .await
                    .wrap_err_with(|| {
                        CfOperationError::new("Failed putting key into", cfs[0].name())
                    })?;
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
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(
                self.key
                    .len_hint_from_slice(slice)
                    .wrap_err("Failed estimating key length")?
                    + object_key.len(),
            );
            let do_unindex = self
                .key
                .extract_key_from_slice(slice, &mut key)
                .wrap_err("Failed extracting key from slice")?;
            if do_unindex {
                key.extend(object_key);
                transaction.delete(&cfs[0], &key).await.wrap_err_with(|| {
                    CfOperationError::new("Failed deleting key from", cfs[0].name())
                })?;
            }
            Ok(())
        })
    }

    fn rebuild<'fut, 't>(
        &'fut self,
        transaction: &'fut B::Transaction<'t>,
        index_cfs: &'fut [B::TransactionCf<'t>],
        datum_cf: &'fut B::TransactionCf<'t>,
    ) -> waaa::BoxFuture<'fut, eyre::Result<()>> {
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
    ) -> waaa::BoxStream<'q, eyre::Result<(Self::QueryKey<'op>, B::Value<'op>)>> {
        let on_each_result = async |res: eyre::Result<(B::Key<'op>, B::Value<'op>)>| -> eyre::Result<
            (Self::QueryKey<'op>, B::Value<'op>)
        > {
            let (index_key, _) = res.wrap_err_with(|| CfOperationError::new("Failed scanning", cfs[0].name()))?;
            let key_len = self.key.key_len(index_key.as_ref());
            let object_key = BTreeQueryKey {
                key: index_key,
                start: key_len,
            };
            let object_value = transaction
                .get(object_cf, object_key.as_ref())
                .await
            .wrap_err_with(|| CfOperationError::new("Failed getting object", object_cf.name()))?
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

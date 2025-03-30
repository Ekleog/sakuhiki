use futures_util::StreamExt as _;
use sakuhiki_core::{
    Backend, CfError, Index, Indexer,
    backend::{BackendCf as _, Transaction as _},
};

use crate::{BTreeQuery, Key};

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
                    .map_err(|e| CfError::new(cfs[0].name(), e))?;
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
                    .map_err(|e| CfError::new(cfs[0].name(), e))?;
            }
            Ok(())
        })
    }

    // TODO(med): implement _from_slice variants with the KeyExtractor-specific method
}

#[warn(clippy::missing_trait_methods)]
impl<B, K> Index<B> for BTreeIndex<K>
where
    B: Backend,
    K: Key,
{
    type Query<'q> = BTreeQuery<'q, K>;
    type QueryKey<'k> = Vec<u8>; // TODO(med): introduce a type to not allocate

    fn query<'q, 'op: 'q, 't: 'op>(
        &'q self,
        query: &'q Self::Query<'q>,
        transaction: &'op B::Transaction<'t>,
        object_cf: &'op B::TransactionCf<'t>,
        cfs: &'op [B::TransactionCf<'t>],
    ) -> waaa::BoxStream<'q, Result<(Self::QueryKey<'op>, B::Value<'op>), CfError<B::Error>>> {
        match query.query {
            crate::query::Query::Equal(_b) => todo!(), // TODO(high): introduce scan_prefix in backend
            crate::query::Query::Prefix(_b) => todo!(),
            crate::query::Query::Range { start, end } => Box::pin(
                transaction
                    .scan::<[u8]>(&cfs[0], (start, end))
                    .then(async |res| {
                        let (index_key, _) = res.map_err(|e| CfError::new(cfs[0].name(), e))?;
                        let index_key = index_key.as_ref();
                        let object_key = &index_key[self.key.key_len(index_key)..];
                        Ok((
                            object_key.to_owned(),
                            transaction
                                .get(object_cf, object_key)
                                .await
                                .map_err(|e| CfError::new(object_cf.name(), e))?
                                .expect("Object was present in index but not in real table"),
                        ))
                    }),
            ),
        }
    }
}

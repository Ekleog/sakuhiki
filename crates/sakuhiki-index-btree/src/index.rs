use sakuhiki_core::{Backend, Index, Indexer, backend::RwTransaction as _};

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
        transaction: &'fut B::RwTransaction<'t>,
        cfs: &'fut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(self.key.len_hint(datum) + object_key.len());
            let do_index = self.key.extract_key(datum, &mut key);
            if do_index {
                key.extend(object_key);
                transaction.put(&cfs[0], &key, &[]).await?;
            }
            Ok(())
        })
    }

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut B::RwTransaction<'t>,
        cfs: &'fut [B::RwTransactionCf<'t>],
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>> {
        Box::pin(async move {
            let mut key = Vec::with_capacity(self.key.len_hint(datum) + object_key.len());
            let do_index = self.key.extract_key(datum, &mut key);
            if do_index {
                key.extend(object_key);
                transaction.put(&cfs[0], &key, &[]).await?;
            }
            Ok(())
        })
    }

    // TODO(med): implement _from_slice variants with the KeyExtractor-specific method
}

impl<B, K> Index<B> for BTreeIndex<K>
where
    B: Backend,
    K: Key,
{
    type Query<'q> = BTreeQuery<'q, K>;
}

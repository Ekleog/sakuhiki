use std::marker::PhantomData;

use crate::{Backend, Datum, Index, Indexer, backend::RwTransaction as _};

pub struct EndIndex<D> {
    cfs: &'static [&'static str; 1],
    _phantom: PhantomData<fn(D)>,
}

impl<D> EndIndex<D> {
    pub const fn new(cfs: &'static [&'static str; 1]) -> Self {
        Self {
            cfs,
            _phantom: PhantomData,
        }
    }
}

impl<B, D> Indexer<B> for EndIndex<D>
where
    B: Backend,
    D: Datum,
{
    type Datum = D;

    fn cfs(&self) -> &'static [&'static str] {
        self.cfs
    }

    fn index_key_len(&self, object_key: &[u8], _datum: &Self::Datum) -> usize {
        object_key.len()
    }

    fn index<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        _datum: &'fut Self::Datum,
        transaction: &'fut mut <B as Backend>::RwTransaction<'t>,
        cf: &'fut mut <B as Backend>::RwTransactionCf<'t>,
        index_key: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), <B as Backend>::Error>> {
        Box::pin(async move {
            index_key.extend(object_key);
            transaction.put(cf, index_key, &[]).await?;
            Ok(())
        })
    }

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        _datum: &'fut Self::Datum,
        transaction: &'fut mut <B as Backend>::RwTransaction<'t>,
        cf: &'fut mut <B as Backend>::RwTransactionCf<'t>,
        index_key: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), <B as Backend>::Error>> {
        Box::pin(async move {
            index_key.extend(object_key);
            transaction.delete(cf, index_key).await?;
            Ok(())
        })
    }

    // TODO: implement _from_slice variants
}

impl<B, D> Index<B> for EndIndex<D>
where
    B: Backend,
    D: Datum,
{
    type Query<'q> = ();
}

use std::marker::PhantomData;

use sakuhiki_core::{Backend, Datum, DynFuture, Index, backend::RwTransaction};

// TODO: add sub-index, instead of forcing the object key
pub struct BTreeIndex<D, KeyExtractor> {
    cf: &'static str,
    key_extractor: KeyExtractor,
    delimiter: Option<u8>,
    _phantom: PhantomData<D>,
}

impl<B, D, KeyExtractor, K> Index<B> for BTreeIndex<D, KeyExtractor>
where
    B: Backend,
    D: Datum<B>,
    KeyExtractor: Fn(&D) -> K,
    K: AsRef<[u8]>,
{
    type Datum = D;

    fn cf(&self) -> &'static str {
        self.cf
    }

    fn index<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> DynFuture<'fut, Result<(), B::Error>> {
        Box::pin(async move {
            let index = (self.key_extractor)(datum);
            let index = index.as_ref();
            match self.delimiter {
                None => {
                    let mut index_key = Vec::with_capacity(index.len() + key.len());
                    index_key.extend(index);
                    index_key.extend(key);
                    transaction.put(cf, &index_key, key).await?;
                }
                Some(delimiter) => {
                    let mut index_key = Vec::with_capacity(index.len() * 2 + 1 + key.len());
                    for c in index {
                        index_key.push(*c);
                        if *c == delimiter {
                            index_key.push(delimiter);
                        }
                    }
                    index_key.push(delimiter);
                    index_key.extend(key);
                    transaction.put(cf, &index_key, key).await?;
                }
            }
            Ok(())
        })
    }

    fn unindex<'fut, 't>(
        &'fut self,
        key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
    ) -> DynFuture<'fut, Result<(), B::Error>> {
        Box::pin(async move {
            let index = (self.key_extractor)(datum);
            let index = index.as_ref();
            match self.delimiter {
                None => {
                    let mut index_key = Vec::with_capacity(index.len() + key.len());
                    index_key.extend(index);
                    index_key.extend(key);
                    transaction.delete(cf, &index_key).await?;
                }
                Some(delimiter) => {
                    let mut index_key = Vec::with_capacity(index.len() * 2 + 1 + key.len());
                    for c in index {
                        index_key.push(*c);
                        if *c == delimiter {
                            index_key.push(delimiter);
                        }
                    }
                    index_key.push(delimiter);
                    index_key.extend(key);
                    transaction.delete(cf, &index_key).await?;
                }
            }
            Ok(())
        })
    }
}

use std::marker::PhantomData;

use sakuhiki_core::{Backend, Datum, Index, backend::RwTransaction};

// TODO: add sub-index, instead of forcing the object key
pub struct BTreeIndex<D, KeyExtractor> {
    cf: &'static str,
    key_extractor: KeyExtractor,
    delimiter: Option<u8>,
    _phantom: PhantomData<fn(D)>,
}

impl<D, KeyExtractor> BTreeIndex<D, KeyExtractor> {
    pub const fn new(cf: &'static str, key_extractor: KeyExtractor, delimiter: Option<u8>) -> Self {
        Self {
            cf,
            key_extractor,
            delimiter,
            _phantom: PhantomData,
        }
    }
}

impl<B, D, KeyExtractor, K> Index<B> for BTreeIndex<D, KeyExtractor>
where
    B: Backend,
    D: Datum<B>,
    KeyExtractor: waaa::Send + waaa::Sync + Fn(&D) -> K,
    K: waaa::Send + AsRef<[u8]>,
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
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>> {
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
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    struct Datum {
        value: u32,
    }

    impl<B: Backend> sakuhiki_core::Datum<B> for Datum {
        type Error = std::convert::Infallible;

        const CF: &'static str = "datum";

        const INDICES: &'static [&'static dyn Index<B, Datum = Self>] =
            &[&BTreeIndex::<Datum, _>::new(
                "index",
                |d: &Datum| d.value.to_be_bytes(),
                None,
            )];

        fn from_slice(_datum: &[u8]) -> Result<Self, Self::Error> {
            todo!()
        }
    }

    #[tokio::test]
    async fn test_index() {
        let _db = sakuhiki_memdb::MemDb::new();
        // TODO
    }
}

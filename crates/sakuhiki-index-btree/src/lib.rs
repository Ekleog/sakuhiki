use std::marker::PhantomData;

use sakuhiki_core::{Backend, Index, IndexedDatum, backend::RwTransaction};

// TODO: add sub-index, instead of forcing the object key
pub struct BTreeIndex<D, KeyExtractor> {
    cfs: &'static [&'static str; 1],
    key_extractor: KeyExtractor,
    delimiter: Option<u8>,
    _phantom: PhantomData<fn(D)>,
}

impl<D, KeyExtractor> BTreeIndex<D, KeyExtractor> {
    pub const fn new(
        cfs: &'static [&'static str; 1],
        key_extractor: KeyExtractor,
        delimiter: Option<u8>,
    ) -> Self {
        Self {
            cfs,
            key_extractor,
            delimiter,
            _phantom: PhantomData,
        }
    }
}

impl<B, D, KeyExtractor, K> Index<B> for BTreeIndex<D, KeyExtractor>
where
    B: Backend,
    D: IndexedDatum<B>,
    KeyExtractor: waaa::Send + waaa::Sync + Fn(&D) -> K,
    K: waaa::Send + AsRef<[u8]>,
{
    type Datum = D;

    fn cfs(&self) -> &'static [&'static str] {
        self.cfs
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
    use std::io;

    use sakuhiki_core::Datum as _;

    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct Datum {
        foo: u32,
        bar: u32,
    }

    impl Datum {
        fn new(foo: u32, bar: u32) -> Self {
            Self { foo, bar }
        }

        fn to_array(&self) -> [u8; 8] {
            let mut array = [0; 8];
            array[..4].copy_from_slice(&self.foo.to_be_bytes());
            array[4..].copy_from_slice(&self.bar.to_be_bytes());
            array
        }
    }

    impl sakuhiki_core::Datum for Datum {
        const CF: &'static str = "datum";
        type Error = io::Error;
        fn from_slice(datum: &[u8]) -> Result<Self, Self::Error> {
            if datum.len() != 8 {
                return Err(io::Error::other(format!(
                    "expected 8-long slice, got {} bytes",
                    datum.len()
                )));
            }
            Ok(Self {
                foo: u32::from_be_bytes(datum[..4].try_into().unwrap()),
                bar: u32::from_be_bytes(datum[4..].try_into().unwrap()),
            })
        }
    }

    #[allow(unused)] // TODO: remove once actually used
    impl Datum {
        const fn index_foo<B: Backend>() -> &'static dyn Index<B, Datum = Self> {
            <Self as sakuhiki_core::IndexedDatum<B>>::INDICES[0]
        }
        const fn index_bar<B: Backend>() -> &'static dyn Index<B, Datum = Self> {
            <Self as sakuhiki_core::IndexedDatum<B>>::INDICES[1]
        }
    }

    impl<B: Backend> sakuhiki_core::IndexedDatum<B> for Datum {
        const INDICES: &'static [&'static dyn Index<B, Datum = Self>] = &[
            &BTreeIndex::<Datum, _>::new(&["datum-foo"], |d: &Datum| d.foo.to_be_bytes(), None),
            &BTreeIndex::<Datum, _>::new(&["datum-bar"], |d: &Datum| d.bar.to_be_bytes(), None),
        ];
    }

    #[tokio::test]
    async fn test_index() {
        // TODO: Should have a better migration story for adding/removing indices
        // Maybe just have create_cf be added to backend and auto-rebuilding?
        let mut backend = sakuhiki_memdb::MemDb::new();
        backend.create_cf("datum");
        backend.create_cf("datum-foo");
        backend.create_cf("datum-bar");
        let db = sakuhiki_core::Db::new(backend);
        let datum = db.cf_handle("datum").await.unwrap();
        // TODO: will need this
        // let index_foo = db.cf_handle("datum-foo").await.unwrap();
        // let index_bar = db.cf_handle("datum-bar").await.unwrap();
        db.rw_transaction(&[&datum], |mut t, [mut datum]| {
            Box::pin(async move {
                let d12 = Datum::new(1, 2);
                let d21 = Datum::new(2, 1);
                t.put(&mut datum, b"12", &d12.to_array()).await.unwrap();
                t.put(&mut datum, b"21", &d21.to_array()).await.unwrap();
                assert_eq!(
                    Datum::from_slice(t.get(&mut datum, b"12").await.unwrap().unwrap()).unwrap(),
                    d12
                );
                assert_eq!(
                    Datum::from_slice(t.get(&mut datum, b"21").await.unwrap().unwrap()).unwrap(),
                    d21
                );
            })
        })
        .await
        .unwrap();
        // TODO
    }
}

use sakuhiki_core::{Backend, Datum, Index, backend::RwTransaction};

pub trait FixedLenKeyExtractor: waaa::Send + waaa::Sync {
    type Datum: Datum;

    const LEN: usize;

    // TODO(blocked): should take &mut [u8; Self::LEN], but const generics are not there yet
    /// Returns `true` iff `datum` must be part of the index.
    fn extract_key(&self, datum: &Self::Datum, key: &mut [u8]) -> bool;

    /// Returns `true` iff `datum` must be part of the index.
    fn extract_key_from_slice(
        &self,
        datum: &[u8],
        key: &mut [u8],
    ) -> Result<(), <Self::Datum as Datum>::Error> {
        let datum = Self::Datum::from_slice(datum)?;
        self.extract_key(&datum, key);
        Ok(())
    }
}

// TODO: add sub-index, instead of forcing the object key
pub struct FixedLenBTreeIndex<K> {
    cfs: &'static [&'static str; 1],
    key_extractor: K,
}

impl<K> FixedLenBTreeIndex<K> {
    pub const fn new(cfs: &'static [&'static str; 1], key_extractor: K) -> Self {
        Self { cfs, key_extractor }
    }
}

impl<B, K> Index<B> for FixedLenBTreeIndex<K>
where
    B: Backend,
    K: FixedLenKeyExtractor,
{
    type Datum = K::Datum;

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
            let mut index_key = Vec::with_capacity(K::LEN + key.len());
            index_key.resize(K::LEN, 0);
            let do_index = self
                .key_extractor
                .extract_key(datum, &mut index_key[0..K::LEN]);
            if do_index {
                index_key.extend(key);
                transaction.put(cf, &index_key, &[]).await?;
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
            let mut index_key = Vec::with_capacity(K::LEN + key.len());
            index_key.resize(K::LEN, 0);
            let do_unindex = self
                .key_extractor
                .extract_key(datum, &mut index_key[0..K::LEN]);
            if do_unindex {
                index_key.extend(key);
                transaction.delete(cf, &index_key).await?;
            }
            Ok(())
        })
    }

    // TODO: implement (un)index_from_slice with the KeyExtractor-specific method
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

    struct KeyFoo;
    impl FixedLenKeyExtractor for KeyFoo {
        type Datum = Datum;
        const LEN: usize = 4;
        fn extract_key(&self, datum: &Self::Datum, key: &mut [u8]) -> bool {
            key.copy_from_slice(&datum.foo.to_be_bytes());
            true
        }
    }

    struct KeyBar;
    impl FixedLenKeyExtractor for KeyBar {
        type Datum = Datum;
        const LEN: usize = 4;
        fn extract_key(&self, datum: &Self::Datum, key: &mut [u8]) -> bool {
            key.copy_from_slice(&datum.bar.to_be_bytes());
            true
        }
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

    impl Datum {
        const INDEX_FOO: &'static FixedLenBTreeIndex<KeyFoo> =
            &FixedLenBTreeIndex::new(&["datum-foo"], KeyFoo);
        const INDEX_BAR: &'static FixedLenBTreeIndex<KeyBar> =
            &FixedLenBTreeIndex::new(&["datum-bar"], KeyBar);
    }

    impl<B: Backend> sakuhiki_core::IndexedDatum<B> for Datum {
        const INDICES: &'static [&'static dyn Index<B, Datum = Self>] =
            &[Self::INDEX_FOO, Self::INDEX_BAR];
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

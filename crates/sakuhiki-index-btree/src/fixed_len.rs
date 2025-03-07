use std::ops::{Bound, RangeBounds};

use sakuhiki_core::{Backend, Datum, Index, Indexer};

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

pub struct FixedLenBTreeIndex<K, I> {
    key_extractor: K,
    sub_index: I,
}

impl<K, I> FixedLenBTreeIndex<K, I> {
    pub const fn new(key_extractor: K, sub_index: I) -> Self {
        Self {
            key_extractor,
            sub_index,
        }
    }
}

impl<B, K, I> Indexer<B> for FixedLenBTreeIndex<K, I>
where
    B: Backend,
    K: FixedLenKeyExtractor,
    I: Indexer<B, Datum = K::Datum>,
{
    type Datum = K::Datum;

    fn cfs(&self) -> &'static [&'static str] {
        self.sub_index.cfs()
    }

    fn index_key_len(&self, object_key: &[u8], datum: &Self::Datum) -> usize {
        K::LEN + self.sub_index.index_key_len(object_key, datum)
    }

    fn index<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
        index_key_prefix: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>> {
        Box::pin(async move {
            let prev_len = index_key_prefix.len();
            index_key_prefix.resize(prev_len + K::LEN, 0);
            let do_index = self
                .key_extractor
                .extract_key(datum, &mut index_key_prefix[prev_len..]);
            if do_index {
                self.sub_index
                    .index(object_key, datum, transaction, cf, index_key_prefix)
                    .await?;
            }
            Ok(())
        })
    }

    fn unindex<'fut, 't>(
        &'fut self,
        object_key: &'fut [u8],
        datum: &'fut Self::Datum,
        transaction: &'fut mut B::RwTransaction<'t>,
        cf: &'fut mut B::RwTransactionCf<'t>,
        index_key_prefix: &'fut mut Vec<u8>,
    ) -> waaa::BoxFuture<'fut, Result<(), B::Error>> {
        Box::pin(async move {
            let prev_len = index_key_prefix.len();
            index_key_prefix.resize(prev_len + K::LEN, 0);
            let do_unindex = self
                .key_extractor
                .extract_key(datum, &mut index_key_prefix[prev_len..]);
            if do_unindex {
                self.sub_index
                    .unindex(object_key, datum, transaction, cf, index_key_prefix)
                    .await?;
            }
            Ok(())
        })
    }

    // TODO: implement _from_slice variants with the KeyExtractor-specific method
}

impl<B, K, I> Index<B> for FixedLenBTreeIndex<K, I>
where
    B: Backend,
    K: FixedLenKeyExtractor,
    I: Index<B, Datum = K::Datum>,
{
    type Query<'q> = FixedLenQuery<'q, B, I>;
}

pub enum FixedLenQuery<'q, B, I>
where
    B: Backend,
    I: Index<B>,
{
    Equal(&'q [u8], I::Query<'q>),
    Prefix(&'q [u8], I::Query<'q>),
    Range {
        start: Bound<&'q [u8]>,
        end: Bound<&'q [u8]>,
    },
}

impl<'q, B, I> FixedLenQuery<'q, B, I>
where
    B: Backend,
    I: Index<B>,
{
    pub fn range(range: impl RangeBounds<&'q [u8]>) -> Self {
        Self::Range {
            start: range.start_bound().cloned(),
            end: range.end_bound().cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use sakuhiki_core::{Datum as _, EndIndex, Indexer};

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
        const INDEX_FOO: &'static FixedLenBTreeIndex<KeyFoo, EndIndex<Self>> =
            &FixedLenBTreeIndex::new(KeyFoo, EndIndex::new(&["datum-foo"]));
        const INDEX_BAR: &'static FixedLenBTreeIndex<KeyBar, EndIndex<Self>> =
            &FixedLenBTreeIndex::new(KeyBar, EndIndex::new(&["datum-bar"]));
    }

    impl<B: Backend> sakuhiki_core::IndexedDatum<B> for Datum {
        const INDICES: &'static [&'static dyn Indexer<B, Datum = Self>] =
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

use std::io;

use sakuhiki_core::{Backend, Datum as _, Indexer};

use crate::*;

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

impl Datum {
    const INDEX_FOO: &'static BTreeIndex<FixedLenKey<Datum>> = &BTreeIndex::new(
        &["datum-foo"],
        FixedLenKey::new(4, |d, key| {
            key.copy_from_slice(&d.foo.to_be_bytes());
            true
        }),
    );
    const INDEX_BAR: &'static BTreeIndex<FixedLenKey<Datum>> = &BTreeIndex::new(
        &["datum-bar"],
        FixedLenKey::new(4, |d, key| {
            key.copy_from_slice(&d.bar.to_be_bytes());
            true
        }),
    );
}

impl<B: Backend> sakuhiki_core::IndexedDatum<B> for Datum {
    const INDEXES: &'static [&'static dyn Indexer<B, Datum = Self>] =
        &[Self::INDEX_FOO, Self::INDEX_BAR];
}

#[tokio::test]
async fn test_index() {
    // TODO: Should have a better migration story for adding/removing indexes
    // Maybe just have create_cf be added to backend and auto-rebuilding?
    let mut backend = sakuhiki_memdb::MemDb::new();
    backend.create_cf("datum");
    backend.create_cf("datum-foo");
    backend.create_cf("datum-bar");
    let db = sakuhiki_core::Db::new(backend);
    let datum = db.cf_handle::<Datum>().await.unwrap();
    // TODO: will need this
    // let index_foo = db.cf_handle("datum-foo").await.unwrap();
    // let index_bar = db.cf_handle("datum-bar").await.unwrap();
    db.rw_transaction(&[&datum], |mut t, [mut datum]| {
        Box::pin(async move {
            let d12 = Datum::new(1, 2);
            let d21 = Datum::new(2, 1);
            t.put::<Datum>(&mut datum, b"12", &d12.to_array())
                .await
                .unwrap();
            t.put::<Datum>(&mut datum, b"21", &d21.to_array())
                .await
                .unwrap();
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

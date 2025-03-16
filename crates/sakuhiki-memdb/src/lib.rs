use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::{Ready, ready},
    ops::RangeBounds,
};

use async_lock::RwLock;
use futures_util::stream;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Column family does not exist in memory database")]
    NonExistentColumnFamily,
}

type ColumnFamily = BTreeMap<Vec<u8>, Vec<u8>>;
type RoCf<'t> = &'t ColumnFamily;
type RwCf<'t> = &'t mut ColumnFamily;

pub struct MemDb {
    db: BTreeMap<String, RwLock<ColumnFamily>>,
}

impl Default for MemDb {
    fn default() -> Self {
        Self::new()
    }
}

impl MemDb {
    pub fn new() -> Self {
        Self {
            db: BTreeMap::new(),
        }
    }

    pub fn create_cf(&mut self, cf: &'static str) {
        self.db
            .insert(cf.to_string(), RwLock::new(ColumnFamily::new()));
    }
}

macro_rules! transaction_impl {
    ($fn:ident, $iter:ident, $locker:ident, $mapper:expr, $cf:ident, $transac:ident) => {
        type $transac<'t> = Transaction;

        fn $fn<'fut, 'db, F, Ret>(
            &'fut self,
            cfs: &'fut [&'fut Self::Cf<'db>],
            actions: F,
        ) -> waaa::BoxFuture<'fut, Result<Ret, Self::Error>>
        where
            F: 'fut
                + waaa::Send
                + for<'t> FnOnce(
                    &'t mut Self::$transac<'t>,
                    &'t mut [$cf<'t>],
                ) -> waaa::BoxFuture<'t, Ret>,
        {
            Box::pin(async {
                let mut t = Transaction { _private: () };
                let mut cfs = cfs.iter().enumerate().collect::<Vec<_>>();
                cfs.sort_by_key(|e| e.1);
                let mut transaction_cfs = Vec::with_capacity(cfs.len());
                for (i, &cf) in cfs {
                    let cf = self.db.get(cf).ok_or(Error::NonExistentColumnFamily)?;
                    transaction_cfs.push((i, cf.$locker().await));
                }
                transaction_cfs.sort_by_key(|e| e.0);
                let mut transaction_cfs = transaction_cfs.$iter().map($mapper).collect::<Vec<_>>();
                Ok(actions(&mut t, &mut transaction_cfs).await)
            })
        }
    };
}

impl sakuhiki_core::Backend for MemDb {
    type Error = Error;

    type Key<'op> = &'op [u8];

    // TODO(med): once rocksdb & indexed-db implemented, check it makes sense for put/delete to return Value
    type Value<'op> = Cow<'op, [u8]>;

    type Cf<'db> = String;
    type RoTransactionCf<'t> = RoCf<'t>;
    type RwTransactionCf<'t> = RwCf<'t>;

    type CfHandleFuture<'op> = Ready<Result<Self::Cf<'op>, Self::Error>>;

    fn cf_handle<'db>(&'db self, name: &str) -> Self::CfHandleFuture<'db> {
        ready(Ok(name.to_string()))
    }

    transaction_impl!(
        ro_transaction,
        iter,
        read,
        |(_, cf)| &**cf,
        RoCf,
        RoTransaction
    );

    transaction_impl!(
        rw_transaction,
        iter_mut,
        write,
        |(_, cf)| &mut **cf,
        RwCf,
        RwTransaction
    );
}

pub struct Transaction {
    _private: (),
}

macro_rules! ro_transaction_methods {
    ($cf:ident) => {
        fn get<'op, 'key>(
            &'op mut self,
            cf: &'op mut $cf<'t>,
            key: &'key [u8],
        ) -> waaa::BoxFuture<'key, Result<Option<Cow<'op, [u8]>>, Error>>
        where
            'op: 'key,
        {
            Box::pin(ready(Ok(cf.get(key).map(|v| Cow::Borrowed(v.as_slice())))))
        }

        fn scan<'op, 'keys>(
            &'op mut self,
            cf: &'op mut $cf<'t>,
            keys: impl 'keys + RangeBounds<[u8]>,
        ) -> waaa::BoxStream<'keys, Result<(&'op [u8], Cow<'op, [u8]>), Error>>
        where
            't: 'op,
            'op: 'keys,
        {
            Box::pin(stream::iter(
                cf.range(keys)
                    .map(|(k, v)| Ok((k.as_slice(), Cow::Borrowed(v.as_slice())))),
            ))
        }
    };
}

impl<'t> sakuhiki_core::backend::RoTransaction<'t, MemDb> for Transaction {
    ro_transaction_methods!(RoCf);
}

impl<'t> sakuhiki_core::backend::RwTransaction<'t, MemDb> for Transaction {
    ro_transaction_methods!(RwCf);

    fn put<'op, 'kv>(
        &'op mut self,
        cf: &'op mut RwCf<'t>,
        key: &'kv [u8],
        value: &'kv [u8],
    ) -> waaa::BoxFuture<'kv, Result<Option<Cow<'op, [u8]>>, Error>>
    where
        't: 'op,
        'op: 'kv,
    {
        let data = cf.insert(key.to_vec(), value.to_vec());
        Box::pin(ready(Ok(data.map(Cow::Owned))))
    }

    fn delete<'op, 'key>(
        &'op mut self,
        cf: &'op mut RwCf<'t>,
        key: &'key [u8],
    ) -> waaa::BoxFuture<'key, Result<Option<Cow<'op, [u8]>>, Error>>
    where
        't: 'op,
        'op: 'key,
    {
        let data = cf.remove(key);
        Box::pin(ready(Ok(data.map(Cow::Owned))))
    }
}

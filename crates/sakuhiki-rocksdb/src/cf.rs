use rocksdb::ColumnFamily;
use sakuhiki_core::backend::BackendCf;

pub struct TransactionCf<'t> {
    cf: &'t ColumnFamily,
    name: &'static str,
}

impl BackendCf for TransactionCf<'_> {
    fn name(&self) -> &'static str {
        self.name
    }
}

impl TransactionCf<'_> {
    pub(crate) async fn open(db: &rocksdb::TransactionDB, name: &str) -> crate::Result<Self> {
        todo!() // TODO(high)
    }
}

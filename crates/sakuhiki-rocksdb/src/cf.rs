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

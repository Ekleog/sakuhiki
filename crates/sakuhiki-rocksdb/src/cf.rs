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

impl<'t> TransactionCf<'t> {
    pub(crate) fn new(name: &'static str, cf: &'t ColumnFamily) -> Self {
        TransactionCf { cf, name }
    }
}

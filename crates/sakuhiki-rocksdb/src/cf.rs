use rocksdb::ColumnFamily;
use sakuhiki_core::backend::BackendCf;

#[derive(Clone)]
pub struct Cf<'t> {
    cf: &'t ColumnFamily,
    name: &'static str,
}

impl BackendCf for Cf<'_> {
    fn name(&self) -> &'static str {
        self.name
    }
}

impl<'t> Cf<'t> {
    pub(crate) fn new(name: &'static str, cf: &'t ColumnFamily) -> Self {
        Cf { cf, name }
    }
}

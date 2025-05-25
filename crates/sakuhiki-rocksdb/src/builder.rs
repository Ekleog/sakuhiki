use sakuhiki_core::{Backend, BackendBuilder, Datum, Db, IndexError};

use crate::{Error, RocksDb};

// TODO(high)
pub struct RocksDbBuilder {}

impl RocksDbBuilder {
    pub fn new() -> RocksDbBuilder {
        RocksDbBuilder {}
    }
}

impl Default for RocksDbBuilder {
    fn default() -> RocksDbBuilder {
        RocksDbBuilder::new()
    }
}

impl BackendBuilder for RocksDbBuilder {
    type Target = RocksDb;

    fn build_datum_cf(
        mut self,
        cf: &'static str,
    ) -> waaa::BoxFuture<'static, Result<Self, <Self::Target as Backend>::Error>> {
        todo!() // TODO(high)
    }

    fn build_index_cf<I: ?Sized + sakuhiki_core::Indexer<Self::Target>>(
        mut self,
        index: &I,
    ) -> waaa::BoxFuture<
        '_,
        Result<Self, IndexError<<Self::Target as Backend>::Error, <I::Datum as Datum>::Error>>,
    > {
        todo!() // TODO(high)
    }

    fn drop_unknown_cfs(self) -> waaa::BoxFuture<'static, Result<RocksDbBuilder, Error>> {
        todo!() // TODO(high)
    }

    type BuildFuture =
        waaa::BoxFuture<'static, Result<Db<Self::Target>, IndexError<Error, anyhow::Error>>>;

    fn build(self) -> Self::BuildFuture {
        todo!() // TODO(high)
    }
}

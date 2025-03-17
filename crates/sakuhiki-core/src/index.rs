use crate::{Backend, CfError, Indexer};

pub trait Index<B: Backend>: 'static + Indexer<B> {
    type Query<'q>;
    type QueryKey<'k>: AsRef<[u8]>;

    fn query<'q, 'op: 'q, 't: 'op>(
        &'q self,
        query: &'q Self::Query<'q>,
        transaction: &'op B::Transaction<'t>,
        object_cf: &'op B::TransactionCf<'t>,
        cfs: &'op [B::TransactionCf<'t>],
    ) -> waaa::BoxStream<'q, Result<(Self::QueryKey<'op>, B::Value<'op>), CfError<B::Error>>>;
}

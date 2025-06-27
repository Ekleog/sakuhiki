use crate::{Backend, CfError, Indexer};

// TODO(med): add FTS with eg. sakuhiki-index-tantivy?
// See https://github.com/quickwit-oss/tantivy/issues/2659 for the doability
// Unfortunately that might require having not only KV but also an object store
pub trait Index<B: Backend>: 'static + Indexer<B> {
    type Query<'q>;
    type QueryKey<'k>: AsRef<[u8]>;

    #[allow(clippy::type_complexity)] // No meaningful way to split the type
    fn query<'q, 'op: 'q, 't: 'op>(
        &'q self,
        query: &'q Self::Query<'q>,
        transaction: &'op B::Transaction<'t>,
        object_cf: &'op B::TransactionCf<'t>,
        cfs: &'op [B::TransactionCf<'t>],
    ) -> waaa::BoxStream<'q, Result<(Self::QueryKey<'op>, B::Value<'op>), CfError<B::Error>>>;
}

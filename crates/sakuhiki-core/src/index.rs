use crate::{Backend, CfError, Indexer};

macro_rules! query_fn {
    ($fn:ident, $query:ident, $transac:ident, $cf:ident) => {
        fn $fn<'q, 'op: 'q, 't: 'op>(
            &'q self,
            query: &'q Self::Query<'q>,
            transaction: &'op B::$transac<'t>,
            object_cf: &'op B::$cf<'t>,
            cfs: &'op [B::$cf<'t>],
        ) -> waaa::BoxStream<'q, Result<(Self::QueryKey<'op>, B::Value<'op>), CfError<B::Error>>>;
    };
}

pub trait Index<B: Backend>: 'static + Indexer<B> {
    type Query<'q>;
    type QueryKey<'k>: AsRef<[u8]>;

    query_fn!(query_ro, Query, RoTransaction, RoTransactionCf);
    query_fn!(query_rw, Query, RwTransaction, RwTransactionCf);
}

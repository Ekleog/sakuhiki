use crate::{Backend, Indexer};

macro_rules! query_fn {
    ($fn:ident, $query:ident, $transac:ident, $cf:ident) => {
        fn $fn<'op, 'q, 't>(
            &self,
            query: &'q Self::Query<'q>,
            transaction: &'op B::$transac<'t>,
            cfs: &'op [B::$cf<'t>],
        ) -> waaa::BoxStream<'q, Option<(B::Key<'op>, B::Value<'op>)>>;
    };
}

pub trait Index<B: Backend>: Indexer<B> {
    type Query<'q>;

    query_fn!(query_ro, Query, RoTransaction, RoTransactionCf);
    query_fn!(query_rw, Query, RwTransaction, RwTransactionCf);
}

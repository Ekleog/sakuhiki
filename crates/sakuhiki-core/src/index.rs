use crate::{Backend, Indexer};

pub trait Index<B: Backend>: Indexer<B> {
    type Query<'q>;
}

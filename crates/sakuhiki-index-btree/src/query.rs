use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use crate::Key;

pub struct BTreeQuery<'q, K>
where
    K: Key,
{
    pub(crate) query: Query<'q>,
    _phantom: PhantomData<fn(K::Datum)>,
}

pub(crate) enum Query<'q> {
    Equal(&'q [u8]),
    Prefix(&'q [u8]),
    Range {
        start: Bound<&'q [u8]>,
        end: Bound<&'q [u8]>,
    },
}

impl<'q, K> BTreeQuery<'q, K>
where
    K: Key,
{
    pub fn equal(key: &'q [u8]) -> Self {
        Self {
            query: Query::Equal(key),
            _phantom: PhantomData,
        }
    }

    pub fn prefix(prefix: &'q [u8]) -> Self {
        Self {
            query: Query::Prefix(prefix),
            _phantom: PhantomData,
        }
    }

    pub fn range(range: impl RangeBounds<&'q [u8]>) -> Self {
        Self {
            query: Query::Range {
                start: range.start_bound().cloned(),
                end: range.end_bound().cloned(),
            },
            _phantom: PhantomData,
        }
    }
}

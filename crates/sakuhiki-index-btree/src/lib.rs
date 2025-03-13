mod fixed_len;
pub use fixed_len::FixedLenKey;

mod index;
pub use index::BTreeIndex;
// TODO(med): add escaped key: 0 for the end, 10 for 0, 11 for 1. Document that shorter strings collate as smaller.
// TODO(med): add composite keys: a tuple of keys.

mod key;
pub use key::Key;

mod query;
pub use query::BTreeQuery;

#[cfg(test)]
mod tests;

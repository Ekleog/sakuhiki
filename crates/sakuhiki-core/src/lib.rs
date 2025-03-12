// TODO: look once more at removing all the Pin<Box<...>> when possible

pub mod backend;
pub use backend::Backend;

mod datum;
pub use datum::{Datum, IndexedDatum};

mod db;
pub use db::{Db, RoTransaction, RwTransaction};

mod errors;
pub use errors::IndexError;

mod index;
pub use index::Index;

mod indexer;
pub use indexer::Indexer;

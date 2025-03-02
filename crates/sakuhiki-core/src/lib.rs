pub mod backend;
pub use backend::Backend;

mod datum;
pub use datum::Datum;

mod db;
pub use db::{Db, RoTransaction, RwTransaction};

mod errors;
pub use errors::IndexError;

mod index;
pub use index::Index;

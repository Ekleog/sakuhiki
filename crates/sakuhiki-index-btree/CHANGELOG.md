# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1-alpha.0](https://github.com/Ekleog/sakuhiki/releases/tag/sakuhiki-index-btree-v0.0.1-alpha.0) - 2025-06-21

### Fixed

- fix send/sync story
- fix clippy lints

### Other

- adjust readme and version numbers
- improve on cferror type for rocksdb transaction handling
- share options handling across backends
- finish polishing up backend builder api
- get fixedlen key ready to deal with key nesting
- implement missing members for fixedlen key too
- finish implementing all default methods in btreeindex
- implement default methods in btreeindex
- handle most clippy lints
- adjust api, use from test
- avoid reallocating all object keys when iterating on index
- have ro/rw transactions for backend, even though the types are not distinct
- simplify code now we no longer need to filter
- equal and prefix query are actually the same because it can't work with nested keys otherwise
- implement equal and prefix matching
- implement prefix query
- implement scan_prefix generically
- enable clippy lints for unimplemented trait methods
- remove done todo
- kill rw/ro transaction split
- implement index-btree's query function
- introduce query_ro/rw functions
- remember cf name as part of cf struct
- set cf for errors in the right place
- take & instead of &mut everywhere, both indexeddb and rocksdb should be able to do it natively
- make backend's put/delete return the old value
- make a pass over all todos
- update indexes in put
- rename indices into indexes
- carry index cfs alongside datum cf
- log todos
- pass all cfs to the (un)index functions
- kill index nesting idea, just have key abstraction for index-btree
- introduce Query concept, rename Index to Indexer for dyn safety
- allow index nesting
- introduce index_key_prefix to help with future nested indices
- rename key into object_key for clarity
- cleanup datum vs indexeddatum; introduce keyextractor concept
- kill delimiter idea
- move btreeindex to module
- add helper index getters
- allow multi-cf indices
- simplify code
- improve on naming
- expand a bit on the test
- use dynamic dispatch everywhere for now so long as we're in heavy development mode and rustc's paint isn't dry yet
- add the strict minimum to get a test compiling
- index-btree does not actually own a Datum
- also implement unindex
- implement btree::index
- initialize index-btree crate
- initial commit

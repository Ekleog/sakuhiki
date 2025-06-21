# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1-alpha.0](https://github.com/Ekleog/sakuhiki/releases/tag/sakuhiki-memdb-v0.0.1-alpha.0) - 2025-06-21

### Fixed

- fix send/sync story
- fix clippy lints
- fix clippy lints

### Other

- adjust readme and version numbers
- improve on cferror type for rocksdb transaction handling
- simplify builder error handling; start implementing rocksdb opening
- share options handling across backends
- implement exclusive locking for index rebuilding in CFs
- finish polishing up backend builder api
- ignore some clippy lints
- add mechanics to drop all unknown cfs
- implement index rebuild
- finish implementing backend creation
- implement memdb cf creation
- start implementing memdb builder
- adjust api, use from test
- sketch impl on the memdb side
- have ro/rw transactions for backend, even though the types are not distinct
- implement scan_prefix generically
- enable clippy lints for unimplemented trait methods
- kill rw/ro transaction split
- implement index-btree's query function
- remember cf name as part of cf struct
- slight code cleanup
- take & instead of &mut everywhere, both indexeddb and rocksdb should be able to do it natively
- make backend's put/delete return the old value
- make a pass over all todos
- carry index cfs alongside datum cf
- implement Default for MemDb
- expand a bit on the test
- use dynamic dispatch everywhere for now so long as we're in heavy development mode and rustc's paint isn't dry yet
- add the strict minimum to get a test compiling
- enable waaa on backend
- rename sakuhiki into sakuhiki-core
- deduplicate code
- use better lifetime names
- cleanup bounds
- refactor the backend traits
- have a single error type per backend
- make transaction actually possible to use
- handle clippy lints
- implement column families for memdb
- introduce column family concept at least for indexed-db and rocksdb
- cleanup memdb code
- implement delete for memdb
- make transaction methods take &mut, implement put for memdb
- record todo
- implement scan for memdb
- implement get for memdb
- introduce sakuhiki-memdb for tests
- initial commit

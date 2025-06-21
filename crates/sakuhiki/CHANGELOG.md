# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1-alpha.0](https://github.com/Ekleog/sakuhiki/releases/tag/sakuhiki-v0.0.1-alpha.0) - 2025-06-21

### Other

- adjust readme and version numbers
- implement index-btree's query function
- make a pass over all todos
- initialize index-btree crate
- also do not have the features
- actually do not reexport all databases, that's going to be version-bumping too often
- reexport sub-crates from sakuhiki
- introduce wrap-everything-up sakuhiki crate
- rename sakuhiki into sakuhiki-core
- introduce the datum and index concepts
- enforce static backends
- use better lifetime names
- implement delete
- implement put
- also implement ro transaction methods for rwtransaction
- deduplicate code
- remove bound
- cleanup bounds
- refactor the backend traits
- implement scan
- implement get
- have a single error type per backend
- make transaction actually possible to use
- start introducing an actual sakuhiki database
- implement column families for memdb
- introduce column family concept at least for indexed-db and rocksdb
- make transaction methods take &mut, implement put for memdb
- implement scan for memdb
- make Backend possible to implement
- make lifetimes explicit
- improve on api
- have a proper range for scanning
- define backend api for kv stores
- initial commit

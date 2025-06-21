# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1-alpha.0](https://github.com/Ekleog/sakuhiki/releases/tag/sakuhiki-rocksdb-v0.0.1-alpha.0) - 2025-06-21

### Other

- adjust readme and version numbers
- improve on cferror type for rocksdb transaction handling
- also finish implementing transaction starting
- unify impl of transaction fns
- implement cf handle creation
- remove no-longer-needed error variant
- avoid blocking on transaction starting
- implement transaction starting
- adjust todos and define transaction type
- remove duplicate fn
- move transaction and cf opening to the right place
- finish building rocksdb backend
- make progress building rocksdb backend
- simplify builder error handling; start implementing rocksdb opening
- still allow configuring the backend itself
- simplify rocksdb backend accordingly
- build the proper builder for rocksdb
- share options handling across backends
- start implementing cf building for rocksdb
- push opening the database back to the build step
- allow configuring cf options
- allow creating a rocksdb builder
- add rocksdb skeleton
- add missing todos
- implement index-btree's query function
- make a pass over all todos
- reexport sub-crates from sakuhiki
- rename sakuhiki into sakuhiki-core
- introduce the two first users
- initial commit

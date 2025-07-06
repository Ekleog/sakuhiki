# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1-alpha.1](https://github.com/Ekleog/sakuhiki/compare/sakuhiki-core-v0.0.1-alpha.0...sakuhiki-core-v0.0.1-alpha.1) - 2025-07-06

### Added

- reserve CFs named __sakuhiki* for sakuhiki itself
- replace custom errors with eyre, making all the types much simpler

### Other

- use a new mode rather than a take_exclusive_lock function
- take Borrow to avoid needless clones
- clean up transaction functions
- clippy
- expand on todo
- log todos

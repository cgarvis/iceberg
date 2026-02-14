# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-02-14

### Added
- Initial release of Apache Iceberg v2 table format library
- Pure Elixir implementation with zero runtime dependencies
- Schema DSL (Ecto-like) for defining table schemas
- Support for create, insert overwrite, and file registration operations
- Avro encoding for manifest and manifest-list files
- SQL injection protection in Parquet stats extraction
- Comprehensive test suite with 123 tests
- DuckDB compute backend for data operations
- Memory and S3-compatible storage backends
- Compile-time schema validation
- Lazy logger evaluation for performance
- Centralized error handling
- Full Dialyzer and Credo compliance

[Unreleased]: https://github.com/cgarvis/iceberg/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cgarvis/iceberg/releases/tag/v0.1.0

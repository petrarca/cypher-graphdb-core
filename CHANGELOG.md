# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure
- Core functionality for Cypher query language support
- ORM-like interface for graph databases
- CLI tool for interactive sessions
- Documentation system with MkDocs

### Changed
- Memgraph and the new Neo4j backend share a Bolt foundation (`backends/bolt`): each is a dialect on it.
  Bolt goes through a transport chosen by the connection: for Memgraph pymgclient (default; faster on
  large reads) or the Neo4j Python driver with `neo4j-rust-ext` (`?transport=neo4j`; per-transaction
  timeouts, metadata, pooling, cluster routing); Neo4j always through the Neo4j driver. Each transport turns its
  values into cypher-graphdb objects in one pass. pymgclient now runs in auto-commit mode when the
  connection does (no BEGIN/COMMIT round trips per statement).

### Added
- Neo4j backend (`backend="neo4j"`): the graph name selects the database; named range indexes.

### Fixed

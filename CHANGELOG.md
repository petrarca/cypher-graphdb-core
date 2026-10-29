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
- Neo4j backend (`backend="neo4j"`): the graph name selects the database; named range indexes.
- Query timeouts per statement: `execute(..., timeout_s=)`, `execute_with_stats(..., timeout_s=)` and
  `with db.query_timeout(...)` override the connection's default; a cancelled query raises
  `QueryTimeoutError` (a `QueryExecutionError`) on every backend. Memgraph through pymgclient cannot
  apply one per statement and logs a warning.
- Path length limits: the parser reports a query's longest path (`ParsedCypherQuery.max_path_length`,
  None when unbounded); `execute`, `execute_with_stats` and `execute_cypher_page` take `max_path_length=`
  and reject a longer query with `PathLengthExceededError` before it runs (no limit by default).

### Changed
- Memgraph and the new Neo4j backend share a Bolt foundation (`backends/bolt`): each is a dialect on it.
  Bolt goes through a transport chosen by the connection: for Memgraph pymgclient (default; faster on
  large reads) or the Neo4j Python driver with `neo4j-rust-ext` (`?transport=neo4j`; per-transaction
  timeouts, metadata, pooling, cluster routing); Neo4j always through the Neo4j driver. Each transport
  turns its values into cypher-graphdb objects in one pass. pymgclient now runs in auto-commit mode when
  the connection does (no BEGIN/COMMIT round trips per statement).
- The default query timeout (`query_timeout_s`) is 600 s instead of 30 s; with the Neo4j driver it is
  sent per transaction instead of as Memgraph's server-wide setting.
- AGE: with autocommit, a failed statement is rolled back, so the connection is usable for the next.

### Fixed

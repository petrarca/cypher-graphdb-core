# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0] - 2026-11-11

### Added
- `create_labels(node_labels, edge_labels)`: creates the labels that do not exist yet, idempotently, and
  returns them. On AGE the label tables, which AGE otherwise creates on a label's first write -- a schema
  provisioned ahead of any write needs them before its indexes; a no-op on Memgraph, Neo4j and FalkorDB.
- `indexed_properties(object_type=NODE)`: the `(label, property)` pairs an equality lookup is index-backed
  for, the same answer on every backend; `IndexInfo.lookup_property` for one index.
- `IndexInfo.object_type` (node or edge) and `IndexInfo.details`, the backend's own information as reported
  (Memgraph's index type and count, Neo4j's type, state and owning constraint, AGE's table, definition and
  access method).
- `IndexType.OTHER` for an index kind not normalized (a Memgraph or Neo4j point index, a Neo4j text index, an
  AGE GiST index, ...): reported, its own type in `details`, and never counted as serving an equality lookup.
  These were reported as property indexes before.

### Changed
- Neo4j: `create_property_index(label, *props)` creates one range index per property, as the other
  backends do, instead of one composite index over all of them -- Neo4j does not use a composite index for a
  lookup on one of its properties, so it served none. An existing composite index is left as it is.
- Neo4j: `list_indexes()` also lists relationship indexes (as `object_type` edge); token-lookup indexes
  only with `include_internal`.
- Memgraph: `list_indexes()` reports edge-type indexes as edge indexes, and a property reported as a bare
  string as one property (it was split into its characters).
- AGE: `list_indexes()` reports the indexes of edge label tables as edge indexes.

## [0.8.1] - 2026-11-09

### Added
- List filters in the bulk deletes: a filter value of `bulk_delete_nodes` and `bulk_delete_orphans` may be a
  list, matching any of its values (`IN` in Cypher, `IN (...)` in SQL on AGE): one call deletes the nodes of
  many files, say, instead of one call per value. A single value matches equality as before; an empty list
  raises `ValueError`.

### Fixed
- AGE: the bulk deletes' property filters use the property indexes. The filter cast the property access to
  `text`, a different expression than the one `create_property_index` indexes, so every bulk delete and
  orphan delete scanned the whole label table (a label of 1 M nodes: 4.8 s for a delete matching nothing,
  0.02 s now). Values are compared as agtype string literals.
- Filter values of the bulk deletes on Memgraph and Neo4j are passed as query parameters instead of being
  interpolated, so any string works -- a file path with `/` or spaces was rejected before (only
  `[A-Za-z0-9_.:-]` passed). AGE already escaped them as SQL literals.
- AGE: a missing label table in a bulk delete rolls back a savepoint, not the connection, so the deletes
  made before it in the same transaction survive.

## [0.8.0] - 2026-11-08

### Added
- Neo4j backend (`backend="neo4j"`): the graph name selects the database; named range indexes.
- Query timeouts per statement: `execute(..., timeout_s=)`, `execute_with_stats(..., timeout_s=)` and
  `with db.query_timeout(...)` override the connection's default; a cancelled query raises
  `QueryTimeoutError` (a `QueryExecutionError`) on every backend. Memgraph through pymgclient cannot
  apply one per statement and logs a warning.
- Path length limits: the parser reports a query's longest path (`ParsedCypherQuery.max_path_length`,
  None when unbounded); `execute`, `execute_with_stats` and `execute_cypher_page` take `max_path_length=`
  and reject a longer query with `PathLengthExceededError` before it runs (no limit by default).
- Batched bulk deletes: `bulk_delete_nodes(..., batch_size=)` and `bulk_delete_orphans(..., batch_size=)`
  delete in statements of at most that many nodes and their edges, each committed: on Memgraph and Neo4j
  their recommendation for large deletes (memory stays flat), on AGE short transactions in SQL (brief row
  locks, vacuum not held back, edges deleted through the start_id/end_id indexes). Each statement stays
  far below a statement timeout. Without it, one statement as before.
- `bulk_delete_edges` and `bulk_update_nodes`, with a direct SQL delete path on AGE.

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
- AGE: graphids are looked up with one `ANY` condition per key property instead of a per-tuple `OR`
  chain, which made composite-key edge writes take seconds per batch.
- AGE: the session setup is committed, so a rollback after a reconnect keeps `ag_catalog` on the
  search path.
- An undecorated base class is no longer treated as a parent in a model's schema (it raised
  `AttributeError`).
- A paged (`execute_cypher_page`) or streamed (`execute_cypher_stream`) query that the database refuses
  raises `QueryExecutionError` (`QueryTimeoutError` when cancelled), the driver's error its cause -- as a
  whole query always did. The native paths let the backend's own exception through (AGE's
  `AGEExecutionError`, Memgraph's and Neo4j's driver errors), which a caller catching the library's types
  did not catch.
- The parser no longer trips an assert on a pattern outside a clause, such as a pattern comprehension
  in `RETURN`.

[Unreleased]: https://github.com/petrarca/cypher-graphdb-core/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/petrarca/cypher-graphdb-core/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/petrarca/cypher-graphdb-core/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/petrarca/cypher-graphdb-core/compare/v0.7.3...v0.8.0

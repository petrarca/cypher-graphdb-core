---
type: Design Overview
title: cypher-graphdb — backend-agnostic access to Cypher graph databases
description: Overview and concept map for the cypher-graphdb library design — the backend seam, the layer stack, typed models, and the documents describing each concept.
tags: [cypher-graphdb, design, overview, backend, architecture, layering]
status: stable
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# cypher-graphdb — design overview

`cypher-graphdb` is a backend-agnostic Python library for Cypher graph
databases. One API drives Apache AGE (a PostgreSQL extension) and Memgraph
(Bolt), with optional Pydantic typing on top and a CLI over the same surface.

For *how to use* it, see the [usage guides](../usage/index.md).

## Problem

Cypher is close to a standard; the databases that speak it are not. AGE stores
properties as a single `agtype` JSON column per label table and cannot bind
parameters inside `UNWIND`; Memgraph is a native graph store over Bolt with
per-property indexes. Code written against one does not run against the other,
and the differences surface at exactly the wrong moments — bulk loads, indexing,
streaming.

Two failure modes follow, and the design targets both:

1. **Leaky portability.** An abstraction that pretends the backends are the same
   works until it doesn't, then fails at runtime deep inside a load.
2. **A lowest common denominator.** An abstraction that only exposes what *both*
   backends do gives up the reason you chose a graph database.

## Design principles

1. **The backend is a seam, not a secret.** `CypherBackend` is an ABC with a
   small required core; everything optional is opt-in and *declared*. Consumers
   ask `has_capability(...)` rather than discovering limits by crashing.
2. **Never hide the database.** Every layer above raw Cypher is optional, and
   `execute()` / `execute_sql()` stay reachable. Dropping a level for one
   awkward query is a supported move, not an escape hatch.
3. **Typing is opt-in and explicit.** Models register on a `ModelProvider` you
   construct. No global registry, so importing a module has no process-wide
   effect and two graphs can share a label without colliding.
4. **Parameterized by construction.** Query building returns `(query, params)`.
   Where a backend cannot bind (AGE inside `UNWIND`), the serializer owns the
   escaping in one place rather than at every call site.
5. **Immutable results.** A query returns its rows *and* its statistics as one
   value, instead of leaving them on the connection for a caller to read back —
   which is what makes pooling and concurrency safe.

## The layer stack

```
TYPED OBJECTS     ModelProvider + your GraphNode / GraphEdge subclasses
GRAPH OBJECTS     GraphNode / GraphEdge / GraphPath / Graph + match criteria
CYPHER            execute(cypher, params=…) → QueryResult
BACKEND SQL       execute_sql() — PostgreSQL/AGE directly
```

`CypherGraphDB` is a facade composed of mixins, one per concern — connection,
batch, indexing, schema, search, SQL, streaming, pagination — so every layer is
reachable from one object. `graphops` sits alongside for client-side analysis of
a materialized `Graph`.

## Concept map

| Concept | Doc | What it covers | State |
|---|---|---|---|
| Model isolation | [model-provider-isolation.md](model-provider-isolation.md) | Per-graph model separation; no global registry, no import side effects | implemented |
| Label collision | [model-provider-label-collision.md](model-provider-label-collision.md) | The bug that motivated isolation: label-as-sole-key let one graph overwrite another | implemented |
| Parameterized queries | [parameterized-queries-for-typed-models.md](parameterized-queries-for-typed-models.md) | The `(query, params)` contract; the AGE `UNWIND` exception | implemented |
| Query results | [query-result-immutable-design.md](query-result-immutable-design.md) | Immutable `QueryResult`; why statistics travel with the rows | implemented |
| Fluent builder | [fluent-cypher-query-builder.md](fluent-cypher-query-builder.md) | The opt-in `cypherquery` builder terminating in `(cypher, params)` | implemented |
| Read-only mode | [read-only-mode.md](read-only-mode.md) | Blocking write clauses in the parser, before the backend | implemented |
| Export format | [hierarchical-export-format.md](hierarchical-export-format.md) | JSON/YAML round-trip with `gid_` deduplication | implemented |
| Format registry | [format-registry.md](format-registry.md) | Formats resolved through a registry; metadata / probe / resolve separated; consumers can register their own | implemented |
| Packaging | [packaging-and-extras.md](packaging-and-extras.md) | Lean core, `[cli]` and `[excel]` extras, and the invariant that importing the library pulls in neither | implemented |
| CLI prompt parsing | [cli-prompt-parsing.md](cli-prompt-parsing.md) | Grammar-based line and argument parsing; why token matching stays hand-written; the pipeline position rules | implemented |

### Designs not built

Kept for the analysis, not as a roadmap. Each says so at the top.

| Doc | Why it still matters | State |
|---|---|---|
| [duckdb-migration.md](duckdb-migration.md) | The case for dropping pandas — which happened, via lightweight row streaming rather than DuckDB | superseded |
| [stateless-multi-graph-support.md](stateless-multi-graph-support.md) | Analysis of graph-name-as-backend-state; multi-graph is served by prefixed settings + per-graph providers instead | proposed |
| [multiple-statement-execution.md](multiple-statement-execution.md) | Semicolon handling and the trade-offs; splitting stays at the CLI edge | proposed |
| [falkordb-integration.md](falkordb-integration.md) | What a third backend would cost — a useful test of the backend seam | proposed |

## Backend notes

**Apache AGE.** A graph is a PostgreSQL schema; a label is a table within it,
created lazily on first insert. Properties live in one `agtype` column.
Indexing therefore means *two* things: a GIN index on the whole column, plus a
btree expression index per property — without the latter, `WHERE n.prop = …`
compiles to `agtype_access_operator` and scans sequentially. Parameters cannot
be bound inside `UNWIND`, so that path serializes inline literals.

**Memgraph.** Native graph storage over Bolt, with per-property indexes,
multiple labels per node, and native streaming.

The asymmetry is why `BackendCapability` exists rather than a uniform interface.

## Reading order

New to the codebase: this page, then
[model-provider-isolation.md](model-provider-isolation.md) (the most
consequential design decision), then
[query-result-immutable-design.md](query-result-immutable-design.md) (why
results look the way they do).

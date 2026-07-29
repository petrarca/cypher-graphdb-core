---
type: Usage Overview
title: Using cypher-graphdb — for consumers of the library
description: Task-oriented documentation for working with Cypher graph databases through cypher-graphdb, across its four layers from typed objects down to backend SQL.
tags: [cypher-graphdb, usage, guide, getting-started, layering, consumer-documentation]
status: stable
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Using cypher-graphdb

How to talk to a Cypher graph database — Apache AGE or Memgraph — through one
API, and how to drive it from the CLI. For *why* it is built this way, see the
[design docs](../design/index.md).

## Guides

| Guide | Read this when… |
|---|---|
| [getting-started.md](getting-started.md) | Starting from zero: install, connect, run your first query. |
| [connecting.md](connecting.md) | Wiring up a backend: connection strings, settings precedence, multiple graphs, pooling, read-only mode. |
| [queries.md](queries.md) | Running Cypher and working with results; creating, fetching and deleting nodes and edges. |
| [typed-models.md](typed-models.md) | Mapping labels onto your own Pydantic classes with a `ModelProvider`. |
| [bulk-and-indexes.md](bulk-and-indexes.md) | Loading data at volume, and creating property indexes — including what your backend actually supports. |
| [import-export.md](import-export.md) | Moving data in and out as CSV/Excel/JSON/YAML, and registering a format of your own. |
| [cli.md](cli.md) | Using `cgdb-cli`, interactively or in a script. |

## Layering (high level → low level)

Four layers, each usable on its own. You are never locked into the one you
started at — they operate on the same connection and the same transaction, so
dropping a level for one awkward query costs nothing.

```
TYPED OBJECTS     your own Pydantic classes, registered on a ModelProvider
  │ hydrates      results come back as Product / UsesTechnology, not dicts
GRAPH OBJECTS     GraphNode / GraphEdge / GraphPath / Graph + match criteria
  │ builds        create_or_merge(), fetch_nodes(), fetch_edges(), delete()
CYPHER            execute(cypher, params=…) — you write the query
  │ dispatches    QueryResult / TabularResult; streaming, pagination, statistics
BACKEND SQL       execute_sql() — straight to PostgreSQL/AGE, no Cypher at all
```

`CypherGraphDB` is a facade composed from mixins, one per concern — connection,
batch, indexing, schema, search, SQL, streaming, pagination — so every layer is
reachable from the same object.

**Pick a layer by how much the database should get out of your way.**

- **Typed objects** when the graph has a schema you own and you want your editor
  and Pydantic to enforce it. See [typed-models.md](typed-models.md).
- **Graph objects** when you want structure without declaring classes — generic
  nodes and edges, matched by criteria. See [queries.md](queries.md).
- **Cypher** when the query is the point: aggregation, variable-length paths,
  anything a criteria object would only obscure. Still parameterized, still
  hydrated. See [queries.md](queries.md).
- **Backend SQL** when you need the backend's own capabilities — a PostgreSQL
  index, a `COPY`, a join against a non-graph table. `execute_sql()` is there on
  purpose; using it is not cheating.

The library does not try to hide the database. An ORM that made the bottom two
layers unreachable would be a downgrade for exactly the work graph databases are
good at.

### Alongside the layers

Some capabilities are orthogonal to the stack rather than a rung on it:

| | What it does |
|---|---|
| `graphops` | Client-side analysis over a materialized `Graph` — `root_nodes`, `incoming_nodes` / `outgoing_nodes`, `build_tree`, `has_cycles`, `density`. No database round-trip; you fetch a subgraph, then reason about it in memory. |
| Schema introspection | `graphs()`, `labels()` — what exists, and how much of it. |
| Full-text search | `search` over graph content, where the backend supports it. |
| Indexing & bulk writes | `create_property_index()`, `bulk_create_nodes()` / `bulk_create_edges()` — see [bulk-and-indexes.md](bulk-and-indexes.md). |
| Import / export | A format registry (CSV, JSON, YAML built in; Excel via `[excel]`), extensible with your own — see [import-export.md](import-export.md). |

## The 30-second example

```python
from cypher_graphdb import CypherGraphDB

with CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687") as db:
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    db.commit()

    count = db.execute("MATCH (n:Person) RETURN count(n)", unnest_result=True)
    print(f"{count} people")
```

Swap `backend="age"` and a `postgresql://` URL and the rest of the program is
unchanged.

## Where backends differ

Backends are not uniform, and the library does not pretend otherwise. Anything a
backend may not support is guarded by an explicit capability check rather than
discovered through a runtime failure:

```python
from cypher_graphdb import BackendCapability

if db.has_capability(BackendCapability.PROPERTY_INDEX):
    db.create_property_index("Person", "name")
```

See [bulk-and-indexes.md](bulk-and-indexes.md) for the full list.

## What is not here

There is no generated API reference. The guides above cover the surface you use;
for the exact signature and behaviour of something like `fetch_nodes`, read its
docstring.

Generating a reference site from those same docstrings would add a build step
and a publishing surface without adding information — your editor already shows
you the docstring at the call site. That is the whole argument. Docstrings are
**not** immune to drifting from the code; they are simply the closest place to
it, and the place a reader is most likely to fix. If you find one that lies,
correct it in the same commit as the code.

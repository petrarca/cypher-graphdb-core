---
type: Guide
title: Connecting and configuring
description: Connection methods for Apache AGE and Memgraph, settings precedence and env vars, prefixed settings for multiple graphs, pooling, and read-only mode.
tags: [cypher-graphdb, connection, settings, configuration, pool, read-only]
status: stable
---

# Connecting and configuring

## The three backends

| Backend | Transport | Connection string | Graph means |
|---|---|---|---|
| `age` | PostgreSQL (`psycopg`) | `postgresql://user:pass@host:5432/db` | a PostgreSQL **schema**; labels are tables inside it |
| `memgraph` | Bolt: pymgclient (default), or the Neo4j Python driver | `bolt://[user:pass@]host:7687[?transport=neo4j]` | the single active database |
| `neo4j` | Bolt: the Neo4j Python driver (with its Rust extension) | `bolt://` or `neo4j://[user:pass@]host:7687[/database]` | a Neo4j **database** (`neo4j` by default) |

All drivers are regular dependencies. Memgraph speaks Bolt through pymgclient by default (faster
on large reads) or through the Neo4j driver (`?transport=neo4j`, or `connect_params={"transport":
...}`: per-transaction timeouts, metadata, cluster routing); the URL's scheme goes to the
transport -- `bolt+s://` for TLS, `neo4j://` for cluster routing.

Memgraph and Neo4j share the Bolt foundation (statements, pages, streams, bulk writes); each is a
dialect on it -- index DDL, catalog queries, how a graph is selected.

Because an AGE graph is a schema, AGE connections take a **graph name** and can
create it on demand. Memgraph connections generally do not; a Neo4j connection's graph name is
its database.

## Ways to connect

### `connect_url` in the constructor

The shortest form. The connection is opened immediately.

```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")
```

### Construct, then `connect()`

Use this when you need graph selection, auto-creation, or explicit transaction
behaviour:

```python
db = CypherGraphDB(backend="age")
db.connect(
    cinfo="postgresql://postgres:postgres@localhost:8432/graphdb",
    graph_name="codegraph",
    create_graph=True,     # create the graph if it does not exist (AGE)
    autocommit=False,      # keep writes in a transaction until you commit
)
```

`autocommit=False` is the right default for bulk work — see
[bulk-and-indexes.md](bulk-and-indexes.md).

### As a context manager

```python
with CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687") as db:
    ...
```

The context manager closes the connection. It does **not** commit — an
uncommitted transaction is rolled back on close.

### From settings alone

With `CGDB_BACKEND` and `CGDB_CINFO` in the environment or a `.env` file, no
arguments are needed:

```python
db = CypherGraphDB()      # backend and connection taken from settings
```

## Settings

Configuration is read by a pydantic-settings `Settings` model.

| Setting | Env var | CLI flag | Default | Meaning |
|---|---|---|---|---|
| `backend` | `CGDB_BACKEND` | `-b / --backend` | – | `age` or `memgraph` |
| `cinfo` | `CGDB_CINFO` | `-c / --cinfo` | – | Connection string / DSN |
| `graph` | `CGDB_GRAPH` | `-g / --graph` | – | Graph name (backend-specific) |
| `read_only` | `CGDB_READ_ONLY` | `-r / --read-only` | `False` | Reject write clauses |
| `create_graph` | `CGDB_CREATE_GRAPH_IF_NOT_EXISTS` | – | `False` | Auto-create the graph (AGE) |
| `query_timeout_s` | `CGDB_QUERY_TIMEOUT_S` | – | – | Per-query timeout in seconds |

**Precedence:** CLI arguments → environment variables → `.env` → constructor
arguments → defaults.

`get_settings()` returns the active settings. `cinfo` is redacted whenever
settings are logged or serialized, so credentials do not leak into logs.

A `.env` file in the working directory is loaded automatically. The repo ships
`.env.age.example` and `.env.memgraph.example` to copy.

## Several graphs in one process

Settings are prefixable, so independent connections can be configured
independently without clashing on `CGDB_*`:

```python
from cypher_graphdb import CypherGraphDB, Settings

tech = Settings.with_prefix("CGDB_TS_")   # reads CGDB_TS_BACKEND, CGDB_TS_CINFO, …
code = Settings.with_prefix("CGDB_CG_")   # reads CGDB_CG_BACKEND, CGDB_CG_CINFO, …

tech_db = CypherGraphDB(tech.backend)
tech_db.connect(cinfo=tech.cinfo, graph_name=tech.graph, create_graph=tech.create_graph)
```

The prefix must end with `_`. This is how a single service hosts several graphs
— a technology graph and a code graph, say — from one environment.

Pair it with a per-graph `ModelProvider` so the two graphs can use the same label
for different classes; see [typed-models.md](typed-models.md).

## Pooling

`CypherGraphDBPool` keeps a bounded set of connections alive, with an optional
idle TTL:

```python
from cypher_graphdb import CypherGraphDBPool

pool = CypherGraphDBPool(pool_size=5, ttl=300)
```

It is thread-safe, purges idle connections opportunistically on acquire (no
background thread), and can be used as a context manager to close everything.

For a long-running server, prefer a pool over one connection per request.

## Read-only mode

Read-only mode parses each statement and rejects writing clauses before they
reach the database:

```python
from cypher_graphdb import ReadOnlyModeError

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")
db.settings.read_only = True

try:
    db.execute("CREATE (:Person {name: 'Mallory'})")
except ReadOnlyModeError:
    ...
```

This is a guard against accidental writes, enforced client-side. It is not a
substitute for database-level permissions for untrusted input.

`install_connection_guard()` / `clear_connection_guard()` additionally let you
veto connections process-wide — useful in test suites that must not touch a real
database.

## Disconnecting

`disconnect()` closes the connection. Any open transaction is rolled back, so
commit first if you meant to keep the work.

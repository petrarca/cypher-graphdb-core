---
type: Guide
title: Getting started with cypher-graphdb
description: Install the library, start a backend, connect, and run your first queries against Apache AGE or Memgraph.
tags: [cypher-graphdb, getting-started, tutorial, install, connect]
status: stable
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Getting started

From nothing to a working query, in four steps.

## 1. Install

Requires **Python 3.14+**. Both backend drivers (`psycopg` for Apache AGE,
`pymgclient` for Memgraph) ship as regular dependencies — there is nothing extra
to install per backend.

```bash
pip install cypher-graphdb          # the library
pip install 'cypher-graphdb[cli]'   # + the cypher-graphdb command
# or
uv add 'cypher-graphdb[cli]'
```

The distribution is `cypher-graphdb`; the import name is `cypher_graphdb`.

| Extra | Adds | Use when |
|---|---|---|
| *(none)* | core library, both backends | embedding the library in your own program |
| `[excel]` | openpyxl | you need Excel import/export (CSV/JSON/YAML need nothing) |
| `[cli]` | typer, rich, prompt_toolkit, art, lark (+ `[excel]`) | you want [`cypher-graphdb`](cli.md) |

The CLI's dependencies are opt-in so that a service embedding the library does
not drag a terminal UI stack into its image. The core install is 22 packages
and carries no numpy. Running `cypher-graphdb` without the
extra tells you which install you need.

> Working *on* the library rather than with it? Clone the repo and run
> `task install`; see [CONTRIBUTING.md](../../CONTRIBUTING.md).

## 2. Get a database running

Either backend works. Memgraph is the quicker start:

```bash
docker run -p 7687:7687 memgraph/memgraph
```

For Apache AGE you need PostgreSQL with the AGE extension loaded; the repo ships
compose files and matching `.env.age.example` / `.env.memgraph.example` files to
copy to `.env`.

## 3. Connect

`connect_url` is the simplest route, and a context manager cleans up for you:

```python
from cypher_graphdb import CypherGraphDB

with CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687") as db:
    print(db.execute("RETURN 1 AS value", unnest_result=True))  # 1
```

For Apache AGE the URL is a PostgreSQL DSN, and AGE additionally needs a **graph
name** — an AGE graph is a PostgreSQL schema:

```python
db = CypherGraphDB(backend="age")
db.connect(cinfo="postgresql://postgres:secret@localhost:5432/graphdb",
           graph_name="demo", create_graph=True)
```

Other ways to configure the connection — environment variables, `.env` files,
individual parameters — are in [connecting.md](connecting.md).

## 4. Write and read

```python
from cypher_graphdb import CypherGraphDB, GraphNode, GraphEdge

with CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687") as db:
    alice = db.create_or_merge(GraphNode(label_="Person",
                                         properties_={"name": "Alice", "age": 30}))
    acme = db.create_or_merge(GraphNode(label_="Company",
                                        properties_={"name": "TechCorp"}))

    db.create_or_merge(
        GraphEdge.build(alice, acme, label_="WORKS_FOR", properties_={"since": 2020})
    )
    db.commit()

    for name, company in db.execute(
        "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name"
    ):
        print(f"{name} works for {company}")
```

Two things to notice:

- **`create_or_merge` returns the object with its ID assigned** — use the return
  value, not the one you passed in.
- **Writes are transactional.** Call `commit()` (or `rollback()`); leaving the
  context manager does not commit for you.

## Where to go next

| You want to… | Guide |
|---|---|
| Understand connection options and settings precedence | [connecting.md](connecting.md) |
| Work with results, criteria, pagination | [queries.md](queries.md) |
| Get your own classes back instead of `GraphNode` | [typed-models.md](typed-models.md) |
| Load a lot of data, or add indexes | [bulk-and-indexes.md](bulk-and-indexes.md) |
| Explore interactively | [cli.md](cli.md) |

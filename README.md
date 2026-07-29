# cypher-graphdb

One Python API for **Cypher graph databases** — Apache AGE (a PostgreSQL
extension) and Memgraph (Bolt) today, others behind the same seam. Plus optional
Pydantic typing and an interactive CLI over the same surface.

Cypher is close to a standard; the databases that speak it are not. AGE keeps
properties in a single `agtype` column and cannot bind parameters inside
`UNWIND`; Memgraph is a native graph store with per-property indexes and real
streaming. `cypher-graphdb` absorbs those differences **without pretending they
do not exist**.

## Why cypher-graphdb

1. **Portable, honestly.** One API across backends, with the differences
   *declared* rather than discovered. Anything a backend may not support sits
   behind `has_capability(...)`, so you branch on a fact instead of catching a
   failure mid-load.

2. **Never in your way.** Every layer above raw Cypher is optional and the
   database stays reachable — down to `execute_sql()` against PostgreSQL/AGE
   directly. Dropping a level for one awkward query is a supported move.

3. **Typing you opt into.** Register Pydantic models on a `ModelProvider` you
   construct and results hydrate into your own classes. No global registry, so
   importing a module has no process-wide effect and two graphs can use the same
   label for different things.

4. **Built for bulk.** `bulk_create_nodes` / `bulk_create_edges` batch through
   `UNWIND` (or a direct SQL path), and property indexing is first-class —
   because a graph load that ignores indexes is a graph load that never
   finishes.

5. **Safe by construction.** Query building returns `(query, params)`;
   read-only mode rejects writing clauses in the parser before they reach the
   backend; results and their statistics come back as one immutable value, which
   is what makes pooling and concurrency safe.

## Feature shortlist

- **Backends** — `age` (Apache AGE on PostgreSQL) and `memgraph` (Bolt). Both
  drivers ship as regular dependencies; there is no per-backend extra. Add
  another by implementing the `CypherBackend` ABC.
- **Capability model** — `BackendCapability` covers property indexes, unique
  constraints, full-text and vector indexes, streaming, pagination, multiple
  labels and more. Optional methods raise `NotImplementedError`; the check is
  how you avoid that.
- **Typed models** — `mp.node()` / `mp.edge()` / `mp.relation()` on an explicit
  `ModelProvider`, with `Cardinality` on declared relationships and JSON-schema
  generation from the models.
- **Queries** — parameterized `execute(cypher, params=…)`, optional result
  unnesting, `QueryResult` with execution statistics, chunked streaming, and
  pagination via `Page`.
- **Graph objects** — `GraphNode`, `GraphEdge`, `GraphPath`, `Graph`, with match
  criteria (`MatchNodeCriteria`, `MatchEdgeCriteria`, `MatchNodeById`, …) that
  build parameterized Cypher rather than concatenated strings.
- **Bulk + indexes** — batched node/edge creation, `create_property_index`,
  `drop_index`, `list_indexes` returning normalized `IndexInfo`.
- **Client-side analysis** — `graphops` over a materialized `Graph`:
  `root_nodes`, `incoming_nodes` / `outgoing_nodes`, `build_tree`, `has_cycles`,
  `density`.
- **Import / export** — a format registry: CSV, JSON and YAML built in, Excel via
  `[excel]`, and your own formats registrable with `@data_format`. Includes a hierarchical
  round-trip format with `gid_` deduplication.
- **Safety** — read-only mode, connection guards, credential-redacting settings.
- **Pooling** — `CypherGraphDBPool` with a size bound and idle TTL.
- **CLI** — `cgdb-cli`, interactive or scripted, over every backend.

## Levels of abstraction

Four layers, each usable on its own, all on the same connection and transaction.

```
TYPED OBJECTS     your Pydantic classes, registered on a ModelProvider
GRAPH OBJECTS     GraphNode / GraphEdge / GraphPath / Graph + match criteria
CYPHER            execute(cypher, params=…) → QueryResult
BACKEND SQL       execute_sql() — straight to PostgreSQL/AGE
```

`CypherGraphDB` is a facade composed of mixins — connection, batch, indexing,
schema, search, SQL, streaming, pagination — so every layer is reachable from
one object. Details in
[`docs/usage/index.md`](docs/usage/index.md#layering-high-level--low-level).

## Example

```python
from cypher_graphdb import CypherGraphDB, GraphNode, GraphEdge

with CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687") as db:
    alice = db.create_or_merge(GraphNode(label_="Person",
                                         properties_={"name": "Alice", "age": 30}))
    acme  = db.create_or_merge(GraphNode(label_="Company",
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

Swap `backend="age"` and a `postgresql://` URL and the rest is unchanged. Note
that nothing commits implicitly — not `create_or_merge`, not leaving the `with`
block.

### With typed models

Models register on a provider you construct, and the provider is wired to the
connection:

```python
from cypher_graphdb import CypherGraphDB, GraphNode, GraphEdge, ModelProvider

mp = ModelProvider()

@mp.node(label="Product")
@mp.relation(rel_type="USES_TECHNOLOGY", to_type="Technology")
class Product(GraphNode):
    name: str
    multi_tenancy: bool | None = None

@mp.edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    version: str | None = None

db = CypherGraphDB(backend="age", model_provider=mp)
```

There is no global registry — importing this module has no process-wide effect,
so two graphs may define the same label without colliding. See
[`docs/adr/0001-explicit-model-providers.md`](docs/adr/0001-explicit-model-providers.md).

### Loading data in bulk

```python
db.bulk_create_nodes(rows, label="Component", batch_size=200)
db.create_property_index("Component", "symbol", "name")   # before the edges
db.bulk_create_edges(edges, src_refs=…, dst_refs=…,
                     src_label="Component", dst_label="Component",
                     src_ref_prop="symbol", dst_ref_prop="symbol")
```

Order matters: nodes, then the indexes the edge load matches on, then edges.
[`docs/usage/bulk-and-indexes.md`](docs/usage/bulk-and-indexes.md) explains why,
and what each backend actually does with an index.

## Install

Requires **Python 3.14+**.

**Lean core, optional CLI.** The default install carries only what the library
needs — both backend drivers included, so there is no per-backend extra. The
interactive CLI's dependencies (typer, rich, prompt_toolkit, art, lark) and
Excel support (openpyxl) are opt-in. The core has no numpy and no terminal-UI stack.

| Install | Adds | Use when |
|---|---|---|
| `cypher-graphdb` | core library, both backends | embedding the library in your own program |
| `cypher-graphdb[excel]` | openpyxl | you need Excel import/export |
| `cypher-graphdb[cli]` | typer, rich, prompt_toolkit, art, lark (+ `[excel]`) | you want the `cgdb-cli` command |
| `cypher-graphdb[dev]` | cli + toolchain | development (implies `[cli]`) |

```bash
pip install cypher-graphdb          # library only
pip install 'cypher-graphdb[cli]'   # + the cgdb-cli command
# or
uv add 'cypher-graphdb[cli]'
```

The distribution is `cypher-graphdb`; the import name is `cypher_graphdb`.
`cgdb-cli` appears on your `PATH` with the `[cli]` extra; invoking it without
that extra reports which install you need rather than a bare import error.

Connection details come from arguments, the environment, or a `.env` file:

```bash
export CGDB_BACKEND=age
export CGDB_CINFO=postgresql://postgres:postgres@localhost:5432/graphdb
export CGDB_GRAPH=my_graph
```

Then start with [`docs/usage/getting-started.md`](docs/usage/getting-started.md).

## The CLI

```bash
cgdb-cli --graph my_graph                    # interactive REPL
cgdb-cli --graph my_graph -e "labels"        # execute and exit
cgdb-cli --graph my_graph --json -e "indexes"
cgdb-cli schema generate -m ./graph_models/ -o ./schemas/   # no database needed
```

Anything that is not a recognised command runs as Cypher. Output is a table
interactively and JSON when scripted. Full command set in
[`docs/usage/cli.md`](docs/usage/cli.md).

## Develop

Requires [`uv`](https://docs.astral.sh/uv/) and [`task`](https://taskfile.dev/);
integration tests need Docker.

```bash
task install           # venv + editable install
task fct               # format + check + unit tests — the local loop
task test:all          # unit + integration
task run:cli           # run cgdb-cli from the venv
```

Conventions and the release flow are in
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## Layout

```
src/cypher_graphdb/
  backend.py            the CypherBackend ABC + BackendCapability
  cyphergraphdb/        the facade and its mixins (connection, batch, indexing,
                        schema, search, sql, streaming, pagination, criteria)
  backends/             age/ and memgraph/ implementations
  cypherquery/          the opt-in fluent query builder
  cypherbuilder.py      parameterized Cypher construction
  cypherparser.py       query parsing (drives read-only mode)
  modelprovider.py      explicit model registries
  models.py             GraphNode / GraphEdge / GraphPath / Graph
  graphops.py           client-side analysis over a materialized Graph
  tools/                import / export (CSV, Excel, JSON, YAML) -- needs [cli]
  cli/                  cgdb-cli — REPL, commands, rendering
docs/                   usage/, design/, adr/
```

## Documentation

- **Using the library** — install, connect, query, type your models, bulk-load,
  and the CLI: [`docs/usage/index.md`](docs/usage/index.md).
- **Design** — the backend seam, the layer stack, and one document per concept,
  each declaring whether it is built:
  [`docs/design/index.md`](docs/design/index.md).
- **Decisions** — [`docs/adr/index.md`](docs/adr/index.md).

Plain markdown with OKF frontmatter; there is no doc build and no generated API
reference. Docstrings in the code are the API reference.

## Contributing & License

Contributions welcome — see [`CONTRIBUTING.md`](CONTRIBUTING.md) (and
[`AGENTS.md`](AGENTS.md) if you use an AI coding assistant). Licensed under the
Apache License 2.0 — see [`LICENSE.md`](LICENSE.md).

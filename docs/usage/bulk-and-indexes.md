---
type: Guide
title: Bulk writes, indexes and backend capabilities
description: Loading data at volume with bulk_create_nodes/bulk_create_edges, creating property indexes, and checking what a backend actually supports.
tags: [cypher-graphdb, bulk, indexes, capabilities, performance, age, memgraph]
status: stable
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Bulk writes, indexes and capabilities

Loading a graph of any size through `create_or_merge` one object at a time is
slow. This guide covers the fast path.

## Check the capability first

Not every backend supports every operation. Ask before you call:

```python
from cypher_graphdb import BackendCapability

if db.has_capability(BackendCapability.PROPERTY_INDEX):
    db.create_property_index("Person", "name")
```

| Capability | Meaning |
|---|---|
| `PROPERTY_INDEX` | `create_property_index` / `drop_index` / `list_indexes` |
| `UNIQUE_CONSTRAINT` | Uniqueness constraints |
| `FULLTEXT_INDEX` | Full-text indexing |
| `VECTOR_INDEX` | Vector indexing |
| `STREAMING_SUPPORT` | Native server-side streaming |
| `LABEL_FUNCTION` | A `label()` function in Cypher |
| `SUPPORT_MULTIPLE_LABELS` | More than one label per node |

Methods behind a capability raise `NotImplementedError` when unsupported — the
check is how you avoid that.

## Bulk creating nodes

`bulk_create_nodes` writes in batches using `UNWIND`:

```python
db.bulk_create_nodes(
    [{"id": "1", "name": "alpha"}, {"id": "2", "name": "beta"}],
    label="Component",
    batch_size=200,
)
```

It accepts either plain property dicts (with an explicit `label=`) or typed
`GraphNode` instances, in which case the label comes from the model:

```python
db.bulk_create_nodes([Component(symbol="a", name="alpha"), ...])
```

It returns the number of nodes written.

## Bulk creating edges

Edges need to identify their endpoints. Supply the edge objects plus parallel
arrays of source and destination references:

```python
db.bulk_create_edges(
    edges,                              # list of GraphEdge / dicts
    src_refs=[r[0] for r in rows],      # values identifying each source node
    dst_refs=[r[1] for r in rows],      # values identifying each destination
    src_label="File",
    dst_label="Import",
    src_ref_prop="symbol",              # property to match src_refs against
    dst_ref_prop="symbol",
    batch_size=500,
)
```

`src_label` / `dst_label` are not optional decoration — they let the backend
target a single label table instead of scanning the whole graph, which is the
difference between a fast load and an unusable one. `src_ref_prop` /
`dst_ref_prop` default to `"id"`.

## Property indexes

```python
db.create_property_index("Component", "symbol", "name", "qualified_name")
db.list_indexes()                       # user indexes
db.list_indexes(include_internal=True)  # + backend internals
db.drop_index("Component", "symbol")
```

`list_indexes()` returns `IndexInfo` objects carrying the label, properties and
`IndexType`.

**Always pass the property names.** They are not decoration on any backend:

- **Apache AGE** creates *two* kinds of index — a GIN index on the whole
  `properties` column, plus a **btree expression index per named property**. The
  GIN index alone does not help a Cypher `WHERE n.prop = …` lookup, which AGE
  compiles to an `agtype_access_operator` expression; without the named
  properties every such lookup is a sequential scan. Calling
  `create_property_index("Component")` with no properties is close to useless.
- **Memgraph, Neo4j, FalkorDB** create one index per property.

So `create_property_index("Component", "symbol", "name")` produces three indexes
on AGE (one GIN + two btree) and two on Memgraph — and is the correct call on
both.

By default `list_indexes()` hides backend-internal indexes — AGE's `_pkey`,
`_start_id_idx`, `_end_id_idx` and friends — so you see what you created.

## Ordering a bulk load

Order matters, and getting it wrong is the usual cause of a load that never
finishes:

1. **Write the nodes.** Nothing to match against yet, so this is pure insert.
2. **Create the property indexes** — on the properties the edge load will match
   on (`src_ref_prop` / `dst_ref_prop`).
3. **Write the edges.** Every edge does two lookups; without step 2 each one is
   a sequential scan.

```python
write_nodes(db)
for label in node_labels:
    db.create_property_index(label, "symbol", "name")
write_edges(db)
```

Creating the indexes *before* the nodes would mean paying to maintain them
during the insert; creating them after the edges is too late to help.

## Transactions and commit cadence

Connect with `autocommit=False` for bulk work and commit periodically — around
every 1000 objects is a reasonable starting point:

```python
db.connect(cinfo=..., graph_name=..., create_graph=True, autocommit=False)
```

Committing per object destroys throughput. Never committing grows the
transaction until the server struggles, and loses everything on failure.

## Reading a lot back

For results too large to materialize, stream them in chunks:

```python
for chunk in db.execute_cypher_stream("MATCH (n:Component) RETURN n", chunk_size=1000):
    handle(chunk)
```

Backends advertising `STREAMING_SUPPORT` stream natively; the others fall back to
a chunked implementation, so the loop above works either way. Pagination via
`Page` is the alternative when you need addressable pages rather than a cursor.

## Why bulk helpers exist at all

Apache AGE cannot bind query parameters inside `UNWIND`. A portable
`UNWIND $rows` write therefore has to be rewritten as inline Cypher literals for
AGE, with all the escaping that implies. `bulk_create_nodes` and
`bulk_create_edges` own that difference — and take the direct SQL path where the
backend allows it — so consumer code stays the same across backends. See
[queries.md](queries.md) for the parameter caveat in its general form.

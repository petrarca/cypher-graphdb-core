---
type: Guide
title: Bulk writes, indexes and backend capabilities
description: Loading and maintaining data at volume with bulk_create_nodes/bulk_create_edges, bulk_update_nodes and bulk_delete_edges, creating property indexes, and checking what a backend actually supports.
tags: [cypher-graphdb, bulk, indexes, capabilities, performance, age, memgraph]
status: stable
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

A ref prop can also be a sequence of properties forming a composite key, e.g.
`("symbol", "source_key")` when a symbol is only unique within one source of a
shared graph; each ref is then a tuple of values in the same order.

## Bulk updating nodes

Setting properties on many existing nodes takes the nodes' references and,
parallel to them, the properties to set — every entry sets the same keys:

```python
db.bulk_update_nodes(
    "Class",
    refs=[("cls:app.Foo", "svc-a"), ("cls:app.Bar", "svc-a")],
    properties=[{"community": 3}, {"community": 5}],
    ref_prop=("symbol", "source_key"),
)
```

Nodes no ref matches are skipped; the return value counts the nodes updated.

## Bulk deleting edges

Deleting many edges takes the same parallel references as creating them. Every
edge of the label from a matched source to its matched destination is deleted,
parallel edges included:

```python
db.bulk_delete_edges(
    "CALLS",
    src_refs=[("mod.foo.bar", "svc-a")],
    dst_refs=[("mod.target.a", "svc-a")],
    src_label="Method",
    dst_label="Method",
    src_ref_prop=("qualified_name", "source_key"),
    dst_ref_prop=("qualified_name", "source_key"),
)
```

On AGE this resolves the endpoints with one SQL query per label and deletes by
`(start_id, end_id)`, without a Cypher `MATCH ... DELETE` — the form that gets
slow on AGE for large sets. Pairs that match no node, and labels that do not
exist yet, delete nothing.

## Property indexes

```python
db.create_property_index("Component", "symbol", "name", "qualified_name")
db.list_indexes()                       # user indexes
db.list_indexes(include_internal=True)  # + backend internals
db.drop_index("Component", "symbol")
```

`list_indexes()` returns `IndexInfo` objects, normalized across backends: the
label, whether the index is on nodes or edges (`object_type`), the properties it
is keyed on in order, the `IndexType`, and in `details` what the backend itself
reports -- Memgraph's index type and entry count, Neo4j's type, state and owning
constraint, AGE's table, definition and access method -- so nothing is lost.

`IndexInfo.lookup_property` names the one property an equality lookup
(`MATCH (n:Label {prop: $v})`) uses the index for, or None: only an index keyed
on exactly that property qualifies. `indexed_properties()` returns those
`(label, property)` pairs for the whole graph -- the backend-neutral answer to
"is this lookup index-backed":

```python
if ("ExternalCall", "symbol") not in db.indexed_properties():
    ...
```

**Always pass the property names.** They are not decoration on any backend:

- **Apache AGE** creates *two* kinds of index — a GIN index on the whole
  `properties` column, plus a **btree expression index per named property**. The
  GIN index alone does not help a Cypher `WHERE n.prop = …` lookup, which AGE
  compiles to an `agtype_access_operator` expression; without the named
  properties every such lookup is a sequential scan. Calling
  `create_property_index("Component")` with no properties is close to useless.
- **Memgraph, Neo4j, FalkorDB** create one index per property. (Neo4j created
  one composite index over all of them up to 0.8.1; Neo4j plans a label scan for
  a lookup on one property of a composite index, so it served none.)

So `create_property_index("Component", "symbol", "name")` produces three indexes
on AGE (one GIN + two btree) and two on Memgraph — and is the correct call on
both.

By default `list_indexes()` hides backend-internal indexes — AGE's `_pkey`,
`_start_id_idx`, `_end_id_idx` and friends, Neo4j's token lookups — so you see
what you created.

## Provisioning a schema ahead of the writes

On a graph that several writers share, create labels and indexes once, ahead of
any write, and only check them afterwards:

```python
db.create_labels(node_labels, edge_labels)   # AGE: the label tables; others: nothing to do
for label, props in indexes.items():
    db.create_property_index(label, *props)
```

`create_labels` is idempotent. AGE otherwise creates a label's table on its
first write, and a label without a table cannot be indexed. Index DDL is not
safe next to concurrent writes: Memgraph refuses it while any other transaction
is open (`Cannot get read-only access to the storage`), and AGE's
`CREATE INDEX IF NOT EXISTS` waits for every open write on the table -- even
when the index exists. Reading `list_indexes()` / `indexed_properties()` is safe
at any time.

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
during the insert; creating them after the edges is too late to help. This
ordering is for a load that has the graph to itself; on a shared graph, provision
the indexes ahead instead (above).

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
`bulk_create_edges`, `bulk_update_nodes` and `bulk_delete_edges` own that
difference — and take the direct SQL path where the backend allows it — so
consumer code stays the same across backends. See
[queries.md](queries.md) for the parameter caveat in its general form.

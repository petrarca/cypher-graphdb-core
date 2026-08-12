---
type: Guide
title: Queries, results and graph objects
description: Running Cypher with parameters, shaping results, creating/fetching/deleting nodes and edges, transactions, and error handling.
tags: [cypher-graphdb, queries, results, nodes, edges, transactions]
status: stable
---

# Queries, results and graph objects

## Running Cypher

```python
rows = db.execute("MATCH (n:Person) RETURN n.name, n.age LIMIT 5")
for name, age in rows:
    print(name, age)
```

`execute` takes four shaping arguments:

| Argument | Effect |
|---|---|
| `params` | Bind values for `$name` placeholders — **always prefer this to string formatting** |
| `unnest_result` | Collapse trivial nesting: a single scalar comes back as the scalar |
| `fetch_one` | Return the first row only |
| `raw_data` | Skip hydration and return the backend's raw rows |

### Parameters

Use `$name` placeholders and pass `params`. This is safe against injection and
lets the backend cache plans:

```python
rows = db.execute(
    "MATCH (s:Source) WHERE s.source_key = $src RETURN DISTINCT s.lang AS lang",
    params={"src": source_key},
)
```

Parameters also carry whole collections, which is the basis of efficient
multi-row writes:

```python
db.execute(
    "UNWIND $rows AS row MERGE (t:Technology {name: row.name}) SET t.category = row.category",
    params={"rows": rows},
)
```

> **AGE caveat:** Apache AGE cannot bind parameters inside `UNWIND`. The library
> works around this by serializing values into inline Cypher literals for that
> case, with proper escaping. You write the same code either way — but this is
> why very large `UNWIND` payloads behave differently per backend, and why
> [`bulk_create_nodes` / `bulk_create_edges`](bulk-and-indexes.md) exist.

### Shaping results

```python
count = db.execute("MATCH (n) RETURN count(n)", unnest_result=True)   # 42
row   = db.execute("RETURN 1 AS x, 2 AS y", fetch_one=True, unnest_result=True)  # (1, 2)
```

Without `unnest_result` the same scalar query returns `[(42,)]`.

### Unnesting collapses aggressively

`unnest_result=True` removes *every* redundant level, and that includes the list
itself when only one row came back:

| Rows matched | `unnest_result=True` returns |
|---|---|
| 3 | a `list` of 3 objects |
| 1 | **the object** — not a one-element list |
| 0 | `None` |

So this is a latent bug:

```python
for p in db.fetch_nodes({"label_": Product}, unnest_result=True):   # ✗
    print(p.name)
```

It works during development with several products and breaks the day exactly one
matches, because it starts iterating the model object instead of a list.

Two safe shapes:

```python
# many: leave unnesting off and unpack the row
for (p,) in db.fetch_nodes({"label_": Product}):
    print(p.name)

# exactly one: ask for one
p = db.fetch_nodes({"label_": Product}, unnest_result=True, fetch_one=True)
```

Use unnesting for the "one value" and "one row" cases; leave it off whenever the
result count is variable and you intend to iterate.

`QueryResult` carries the rows plus execution metadata; `TabularResult` is the
column-oriented form used for tabular rendering. `db.statistics` exposes timing
and counters for the last query.

## Creating and merging

`create_or_merge` writes a node or edge and returns it **with its ID assigned** —
use the returned object:

```python
from cypher_graphdb import GraphNode, GraphEdge

alice = db.create_or_merge(GraphNode(label_="Person",
                                     properties_={"name": "Alice", "age": 30}))
acme  = db.create_or_merge(GraphNode(label_="Company",
                                     properties_={"name": "TechCorp"}))

db.create_or_merge(
    GraphEdge.build(alice, acme, label_="WORKS_FOR", properties_={"since": 2020})
)
```

Two options are worth knowing:

- `strategy="merge"` (default) matches an existing object before creating one;
  `strategy="force_create"` always creates.
- `validate=True` checks properties against the registered model for the label.
  It is **off by default** — turn it on when you want a typo in a property name
  to fail loudly rather than silently create a new property.

## Fetching

`fetch_nodes` and `fetch_edges` accept several forms of criteria:

```python
node  = db.fetch_nodes(12345, unnest_result=True)              # by internal ID
node  = db.fetch_nodes("6f628f1e7tFZHfis", unnest_result=True) # by GID
nodes = db.fetch_nodes({"label_": "Product", "name": "CypherGraph"})
nodes = db.fetch_nodes({"label_": Product})                    # by model class
```

For anything richer, use the criteria objects — they build parameterized Cypher
rather than string-concatenated queries:

```python
from cypher_graphdb import MatchNodeCriteria, MatchEdgeCriteria, MatchNodeById

db.fetch_nodes(MatchNodeCriteria(label_="Person", properties_={"age": 30}))
db.fetch_edges(MatchEdgeCriteria(label_="WORKS_FOR"))
db.fetch_nodes(MatchNodeById(id_=12345))
```

Large result sets can be paged with `Page`, or streamed where the backend
advertises `STREAMING_SUPPORT` — see
[bulk-and-indexes.md](bulk-and-indexes.md) for capability checks.

## Deleting

```python
db.delete(node)                     # delete one object
db.delete(node, detach=True)        # DETACH DELETE — drop its relationships too
db.delete(MatchNodeCriteria(label_="Person"))   # delete everything matching
```

Deleting a node that still has edges fails unless `detach=True`.

## Transactions

Writes are transactional unless the backend connected with autocommit.

```python
db.execute("CREATE (:Person {name: 'Alice'})")
db.commit()      # or db.rollback()
```

Nothing commits implicitly — not `create_or_merge`, not leaving a `with` block.
For long write runs, commit periodically rather than at the very end; a batch
size around 1000 objects is a reasonable starting point.

## Errors

| Exception | Raised when |
|---|---|
| `QueryExecutionError` | The backend rejected or failed the query |
| `ReadOnlyModeError` | A write was attempted on a read-only connection |
| `LabelNotFoundError` | The referenced label does not exist |
| `ConnectionGuardError` | An installed connection guard vetoed the connection |

```python
from cypher_graphdb import QueryExecutionError

try:
    db.execute("MATCH (n RETURN n")
except QueryExecutionError as exc:
    logger.error("query failed: {}", exc)
    db.rollback()
```

Roll back after a failed write — the transaction stays open and poisoned
otherwise.

## Inspecting a query without running it

`parse_cypher_query()` returns a `ParsedCypherQuery` describing the operations
and labels a statement touches. This is what read-only mode uses, and it is
available for your own checks — per-label authorization, for instance.

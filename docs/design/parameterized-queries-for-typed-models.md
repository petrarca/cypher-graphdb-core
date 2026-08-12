---
type: Design
title: Parameterized queries for typed models
description: Why CRUD on typed models builds parameterized Cypher instead of embedding property values in query strings, and how the (query, params) contract works.
tags: [cypher-graphdb, parameterized-queries, typed-models, cypherbuilder, escaping]
status: stable
implementation: implemented
---

# Parameterized queries for typed models

> **Implemented.** `src/cypher_graphdb/cypherbuilder.py` returns a
> `(query, params)` tuple from every method that would otherwise inline
> property values.
>
> One backend caveat outlives the change: Apache AGE cannot bind parameters
> inside `UNWIND`, so that path serializes inline Cypher literals via
> `backends/age/ageserializer.py`. That is the reason the bulk helpers exist.

## Executive summary

The library uses parameterized queries for CRUD operations on typed models. Building Cypher by embedding property values directly into query strings fails when values contain special characters like quotes (`"`, `'`), newlines (`\n`), or backslashes (`\`).

## Problem statement

### Current behavior

When using `create_or_merge()` with a typed model, the library:

1. Calls `CypherBuilder.merge_node_by_id()` to generate a Cypher query
2. Uses `dict_to_value_pairs()` → `convert_to_str()` to embed values in the query string
3. Executes the query via the backend

The `convert_to_str()` function in `utils/string_utils.py` (lines 39-40) does **not escape special characters**:

```python
if isinstance(value, str):
    return f'"{value}"'  # No escaping!
```

### Failure example

Given a ProductProfile with:
```python
description = 'Setting the standard with "pioneering" solutions'
```

The generated Cypher becomes:
```cypher
SET n.description = "Setting the standard with "pioneering" solutions"
```

This is **invalid Cypher syntax** and causes a `DatabaseError`.

### Impact

- Any string field containing `"`, `'`, `\n`, or `\` will fail on `create_or_merge()`
- Affects all typed model operations: `_create_node()`, `_merge_node()`, `_create_edge()`, `_merge_edge()`
- Users must work around this by using raw `execute()` with manual parameterized queries

## Solution

### Approach: parameterized queries

Instead of embedding values in query strings, use Cypher's native parameter binding (`$param` syntax). The backends already support this via the `params` argument to `execute_cypher()`.

### Design principles

1. **Security**: Parameterized queries prevent Cypher injection attacks
2. **Correctness**: All characters are handled correctly by the database driver
3. **Performance**: Database can cache and reuse query plans
4. **Backward Compatibility**: Minimize breaking changes to public API

## The design

### CypherBuilder (`cypherbuilder.py`)

Change CRUD methods to return `tuple[str, dict]` instead of `str`.

#### Current implementation

```python
@classmethod
def merge_node_by_id(cls, node_id, properties) -> str:
    values = dict_to_value_pairs(properties, "n.")
    return f"""
        MATCH (n)
        WHERE id(n) = {node_id}
        SET {values}
        RETURN n
    """
```

#### Proposed implementation

```python
@classmethod
def merge_node_by_id(cls, node_id, properties) -> tuple[str, dict]:
    """Generate parameterized MERGE query for node update.
    
    Args:
        node_id: Database ID of the node to update
        properties: Dictionary of properties to set
        
    Returns:
        Tuple of (cypher_query, params_dict)
    """
    # Build SET clause with parameter placeholders
    set_parts = [f"n.{key} = $p_{key}" for key in properties.keys()]
    
    query = f"""
        MATCH (n)
        WHERE id(n) = {node_id}
        SET {', '.join(set_parts)}
        RETURN n
    """
    
    # Prefix params to avoid conflicts with user params
    params = {f"p_{key}": value for key, value in properties.items()}
    
    return query, params
```

#### Methods to update

| Method | Current Return | New Return |
|--------|---------------|------------|
| `create_node()` | `str` | `tuple[str, dict]` |
| `merge_node_by_id()` | `str` | `tuple[str, dict]` |
| `create_edge()` | `str` | `tuple[str, dict]` |
| `merge_edge_by_id()` | `str` | `tuple[str, dict]` |

### CypherGraphDB (`cyphergraphdb/cyphergraphdb.py`)

Update internal methods to unpack the tuple and pass params.

#### Current implementation

```python
def _merge_node(self, obj) -> GraphNode:
    cypher_cmd = CypherBuilder.merge_node_by_id(obj.id_, obj.flatten_properties())
    result = self._parse_and_execute(cypher_cmd, True)
    # ...
```

#### Proposed implementation

```python
def _merge_node(self, obj) -> GraphNode:
    cypher_cmd, params = CypherBuilder.merge_node_by_id(obj.id_, obj.flatten_properties())
    result = self._parse_and_execute(cypher_cmd, fetch_one=True, params=params)
    # ...
```

#### Methods to update

| Method | Change |
|--------|--------|
| `_create_node()` | Unpack tuple, pass `params` |
| `_merge_node()` | Unpack tuple, pass `params` |
| `_create_edge()` | Unpack tuple, pass `params` |
| `_merge_edge()` | Unpack tuple, pass `params` |

### `_parse_and_execute()` method

Ensure params are forwarded to the backend.

#### Current signature

```python
def _parse_and_execute(self, cypher_cmd, fetch_one=False, raw_data=False):
```

#### Proposed signature

```python
def _parse_and_execute(self, cypher_cmd, fetch_one=False, raw_data=False, params=None):
```

#### Implementation

```python
def _parse_and_execute(self, cypher_cmd, fetch_one=False, raw_data=False, params=None):
    """Parse and execute a Cypher command with optional parameters.
    
    Args:
        cypher_cmd: Cypher query string or ParsedCypherQuery
        fetch_one: If True, return only first result
        raw_data: If True, return raw data without transformation
        params: Optional dict of parameter values to bind
        
    Returns:
        Query results
    """
    parsed_query = self._parse_cypher(cypher_cmd)
    result, _ = self._backend.execute_cypher(
        parsed_query, 
        fetch_one=fetch_one, 
        raw_data=raw_data, 
        params=params
    )
    return result
```

### Backend support (already implemented)

Both backends already support parameterized queries:

#### Memgraph backend (`backends/memgraph/memgraphdb.py`)

```python
def _execute_query(self, query, fetch_one=False, raw_data=False, params=None):
    # ...
    cursor.execute(query, params or {})  # Line 493
```

#### AGE backend (`backends/age/agegraphdb.py`)

```python
def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False, params=None):
    if params:
        # Use prepared statement for parameterized queries
        (result, execute_stats, _) = self._execute_prepared(cypher_query, fetch_one, raw_data, params)
```

**No changes needed to backends.**


## Where it does not apply

Apache AGE cannot bind parameters inside `UNWIND`, so that path serializes
inline Cypher literals through `backends/age/ageserializer.py`, which owns the
escaping. This is why the bulk helpers exist rather than callers writing their
own `UNWIND $rows`; see [the usage guide](../usage/bulk-and-indexes.md).

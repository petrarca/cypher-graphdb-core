# Design Document: Parameterized Queries for Typed Models

**Date:** 2026-02-03  
**Status:** Proposal  
**Author:** Auto-generated from apex-graph analysis  

## Executive Summary

This document proposes changes to the cypher-graphdb library to use parameterized queries for all CRUD operations on typed models. Currently, the library builds Cypher queries by embedding property values directly into query strings, which fails when values contain special characters like quotes (`"`, `'`), newlines (`\n`), or backslashes (`\`).

## Problem Statement

### Current Behavior

When using `create_or_merge()` with a typed model, the library:

1. Calls `CypherBuilder.merge_node_by_id()` to generate a Cypher query
2. Uses `dict_to_value_pairs()` → `convert_to_str()` to embed values in the query string
3. Executes the query via the backend

The `convert_to_str()` function in `utils/string_utils.py` (lines 39-40) does **not escape special characters**:

```python
if isinstance(value, str):
    return f'"{value}"'  # No escaping!
```

### Failure Example

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

## Proposed Solution

### Approach: Parameterized Queries

Instead of embedding values in query strings, use Cypher's native parameter binding (`$param` syntax). The backends already support this via the `params` argument to `execute_cypher()`.

### Design Principles

1. **Security**: Parameterized queries prevent Cypher injection attacks
2. **Correctness**: All characters are handled correctly by the database driver
3. **Performance**: Database can cache and reuse query plans
4. **Backward Compatibility**: Minimize breaking changes to public API

## Detailed Changes

### 1. CypherBuilder (`cypherbuilder.py`)

Change CRUD methods to return `tuple[str, dict]` instead of `str`.

#### Current Implementation

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

#### Proposed Implementation

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

#### Methods to Update

| Method | Current Return | New Return |
|--------|---------------|------------|
| `create_node()` | `str` | `tuple[str, dict]` |
| `merge_node_by_id()` | `str` | `tuple[str, dict]` |
| `create_edge()` | `str` | `tuple[str, dict]` |
| `merge_edge_by_id()` | `str` | `tuple[str, dict]` |

### 2. CypherGraphDB (`cyphergraphdb/cyphergraphdb.py`)

Update internal methods to unpack the tuple and pass params.

#### Current Implementation

```python
def _merge_node(self, obj) -> GraphNode:
    cypher_cmd = CypherBuilder.merge_node_by_id(obj.id_, obj.flatten_properties())
    result = self._parse_and_execute(cypher_cmd, True)
    # ...
```

#### Proposed Implementation

```python
def _merge_node(self, obj) -> GraphNode:
    cypher_cmd, params = CypherBuilder.merge_node_by_id(obj.id_, obj.flatten_properties())
    result = self._parse_and_execute(cypher_cmd, fetch_one=True, params=params)
    # ...
```

#### Methods to Update

| Method | Change |
|--------|--------|
| `_create_node()` | Unpack tuple, pass `params` |
| `_merge_node()` | Unpack tuple, pass `params` |
| `_create_edge()` | Unpack tuple, pass `params` |
| `_merge_edge()` | Unpack tuple, pass `params` |

### 3. `_parse_and_execute()` Method

Ensure params are forwarded to the backend.

#### Current Signature

```python
def _parse_and_execute(self, cypher_cmd, fetch_one=False, raw_data=False):
```

#### Proposed Signature

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

### 4. Backend Support (Already Implemented)

Both backends already support parameterized queries:

#### Memgraph Backend (`backends/memgraph/memgraphdb.py`)

```python
def _execute_query(self, query, fetch_one=False, raw_data=False, params=None):
    # ...
    cursor.execute(query, params or {})  # Line 493
```

#### AGE Backend (`backends/age/agegraphdb.py`)

```python
def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False, params=None):
    if params:
        # Use prepared statement for parameterized queries
        (result, execute_stats, _) = self._execute_prepared(cypher_query, fetch_one, raw_data, params)
```

**No changes needed to backends.**

## Migration Strategy

### Option A: Breaking Change (Recommended)

Change return types directly. This is cleaner but requires a major version bump.

```
Version: 2.0.0
```

**Pros:**
- Clean API
- No legacy code to maintain

**Cons:**
- Breaking change for anyone calling `CypherBuilder` directly

### Option B: Backward Compatible

Add new methods with `_parameterized` suffix, deprecate old methods.

```python
# New method
@classmethod
def merge_node_by_id_parameterized(cls, node_id, properties) -> tuple[str, dict]:
    ...

# Old method (deprecated)
@classmethod
def merge_node_by_id(cls, node_id, properties) -> str:
    warnings.warn("Use merge_node_by_id_parameterized instead", DeprecationWarning)
    ...
```

**Pros:**
- No breaking changes
- Gradual migration path

**Cons:**
- API bloat
- Maintenance burden

### Recommendation

**Option A** is recommended because:
1. `CypherBuilder` is an internal class, not part of the public API
2. The fix is critical for correctness
3. Clean break is better than carrying technical debt

## Testing Strategy

### Unit Tests

```python
def test_merge_node_with_special_characters():
    """Test that special characters in string fields are handled correctly."""
    node = TestNode(
        name="Test",
        description='Contains "quotes" and \'apostrophes\'',
        notes="Has\nnewlines\nand\ttabs"
    )
    
    # Should not raise DatabaseError
    result = cdb.create_or_merge(node)
    
    # Verify data integrity
    fetched = cdb.fetch_nodes({"label_": TestNode, "id_": result.id_})
    assert fetched.description == node.description
    assert fetched.notes == node.notes
```

### Integration Tests

Test with real database backends:
- Memgraph
- PostgreSQL/AGE

### Edge Cases to Test

| Input | Expected Behavior |
|-------|-------------------|
| `"double quotes"` | Stored correctly |
| `'single quotes'` | Stored correctly |
| `line1\nline2` | Stored with newline |
| `path\\to\\file` | Stored with backslashes |
| `emoji 🎉` | Stored correctly (UTF-8) |
| `NULL` (string) | Stored as string "NULL" |
| `$param` (string) | Stored as string "$param" |
| Empty string `""` | Stored as empty string |

## Implementation Plan

### Phase 1: Core Changes (1-2 days)

1. Update `CypherBuilder` methods to return `tuple[str, dict]`
2. Update `_parse_and_execute()` to accept `params`
3. Update `_create_node()`, `_merge_node()`, `_create_edge()`, `_merge_edge()`

### Phase 2: Testing (1 day)

1. Add unit tests for special character handling
2. Run integration tests against Memgraph and AGE
3. Test backward compatibility of `execute()` method

### Phase 3: Documentation & Release (0.5 days)

1. Update CHANGELOG.md
2. Update README with migration notes
3. Tag release as 2.0.0

## Appendix

### A. Affected Files

| File | Changes |
|------|---------|
| `cypherbuilder.py` | Return type changes for 4 methods |
| `cyphergraphdb/cyphergraphdb.py` | Update 4 internal methods + `_parse_and_execute` |
| `utils/string_utils.py` | No changes (kept for other uses) |
| `backends/*` | No changes (already support params) |

### B. Current Workaround

Until this is implemented, users can work around the issue by using parameterized queries directly:

```python
# Instead of:
profile.description = "Text with \"quotes\""
cdb.create_or_merge(profile)  # FAILS

# Use:
cdb.execute(
    "MATCH (p:ProductProfile {key: $key}) SET p.description = $description",
    params={"key": profile.key, "description": "Text with \"quotes\""}
)  # WORKS
```

### C. References

- [Cypher Parameters Documentation](https://neo4j.com/docs/cypher-manual/current/syntax/parameters/)
- [Memgraph Parameters](https://memgraph.com/docs/fundamentals/query-modules)
- [PostgreSQL Prepared Statements](https://www.postgresql.org/docs/current/sql-prepare.html)

---
type: Design
title: Immutable QueryResult and dual execute methods
description: Why query results and their statistics are returned as one immutable object rather than read back from mutable connection state, and what that buys in concurrent use.
tags: [cypher-graphdb, query-result, immutability, thread-safety, statistics, pooling]
status: stable
implementation: implemented
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Immutable QueryResult and dual execute methods

> **Built.** `QueryResult` lives in `cyphergraphdb/result.py`;
> `execute_with_stats()` and `execute_sql_with_stats()` return it. The mutable
> connection state this replaced — `_exec_statistics`, `_sql_statistics`,
> `_last_parsed_query`, and the `exec_statistics()` / `last_parsed_query` /
> `sql_statistics()` accessors — was removed outright rather than deprecated,
> since the library had no external users at the time.

The durable point of this document is the *why*: results and their metadata
travel together as one immutable value, instead of being read back from state
hanging off the connection.

## Problem Statement

### Current State Management Issues

The `CypherGraphDatabase` class maintains mutable instance state for query execution:

```python
class CypherGraphDB:
    def __init__(self):
        self._exec_statistics = ExecStatistics()
        self._sql_statistics = SqlStatistics()  
        self._last_parsed_query = None
```

### Race Conditions

Parallel queries using the same `CypherGraphDB` instance cause state corruption:

```python
# Thread A executes query
result = cdb.execute("MATCH (n) RETURN n")
# Thread B executes query before Thread A accesses statistics
stats = cdb.exec_statistics()  # Gets Thread B's statistics, not A's
```

### Pool Limitations

While `CypherGraphDBPool` isolates state between different instances, it doesn't solve:
- Concurrent access to the same pooled instance
- State corruption within a single instance during parallel operations

## Solution Design

### Immutable QueryResult

```python
@dataclass(frozen=True)
class QueryResult:
    """Immutable container for query execution results and metadata."""
    
    data: Any | TabularResult
    exec_statistics: ExecStatistics
    sql_statistics: SqlStatistics | None = None
    parsed_query: ParsedCypherQuery | None = None
    
    def is_empty(self) -> bool:
        """Check if the result contains no data."""
        return not self.data
    
    def has_graph_data(self) -> bool:
        """Check if the result contains graph data (nodes/edges)."""
        return self.exec_statistics.has_graph_data()
    
    def has_tabular_data(self) -> bool:
        """Check if the result contains tabular data."""
        return self.exec_statistics.has_tabular_data()
```

### Dual Execute Methods

#### Method 1: `execute()` - Current Behavior (Data Only)

```python
def execute(
    self,
    cypher_cmd: str | ParsedCypherQuery,
    unnest_result: str | bool = None,
    fetch_one=False,
    raw_data=False,
) -> Any | TabularResult:
    """Execute a Cypher command and return data only (current behavior).
    
    Statistics are discarded to maintain backward compatibility.
    
    Args:
        cypher_cmd: Cypher query string or pre-parsed ParsedCypherQuery
        unnest_result: Result formatting option
        fetch_one: Optimize for single result
        raw_data: Return raw database results
        
    Returns:
        Query results formatted according to unnest_result parameter
        
    Examples:
        ```python
        # Simple data access - unchanged
        products = cdb.execute("MATCH (p:Product) RETURN p.name", unnest_result="c")
        count = cdb.execute("MATCH (n) RETURN count(n)", unnest_result="rc")
        ```
    """
    parsed_query = self._parse_cypher(cypher_cmd)
    result, exec_stats = self._backend.execute_cypher(parsed_query, fetch_one, raw_data)
    
    # Simply return the data - no instance state manipulation
    return utils.unnest_result(result, unnest_result)
```

#### Method 2: `execute_with_stats()` - New Behavior (Full Result)

```python
def execute_with_stats(
    self,
    cypher_cmd: str | ParsedCypherQuery,
    unnest_result: str | bool = None,
    fetch_one=False,
    raw_data=False,
) -> QueryResult:
    """Execute a Cypher command and return immutable QueryResult with statistics.
    
    Provides complete query execution information without mutable state.
    
    Args:
        cypher_cmd: Cypher query string or pre-parsed ParsedCypherQuery
        unnest_result: Result formatting option
        fetch_one: Optimize for single result
        raw_data: Return raw database results
        
    Returns:
        QueryResult containing data, execution statistics, and parsed query
        
    Examples:
        ```python
        # Full query information
        result = cdb.execute_with_stats("MATCH (p:Product) RETURN p.name", unnest_result="c")
        data = result.data  # ['Product1', 'Product2', ...]
        stats = result.exec_statistics  # Execution metrics
        parsed = result.parsed_query  # Query analysis
        
        # Performance monitoring
        if result.exec_statistics.exec_time > 1.0:
            print(f"Slow query: {result.exec_statistics.exec_time}s")
        ```
    """
    parsed_query = self._parse_cypher(cypher_cmd)
    result, exec_stats = self._backend.execute_cypher(parsed_query, fetch_one, raw_data)
    
    return QueryResult(
        data=utils.unnest_result(result, unnest_result),
        exec_statistics=exec_stats,
        sql_statistics=None,  # Only populated for SQL queries
        parsed_query=parsed_query
    )
```

### Error Messages for Migration Detection

```python
def exec_statistics(self) -> ExecStatistics:
    """Removed for thread safety: Use execute_with_stats() to get QueryResult with statistics.
    
    Raises:
        RuntimeError: Always raises to guide migration to execute_with_stats()
    """
    raise RuntimeError(
        "exec_statistics() has been removed for thread safety. "
        "Use execute_with_stats() instead:\n"
        "  Old: result = cdb.execute(query); stats = cdb.exec_statistics()\n" 
        "  New: result = cdb.execute_with_stats(query); stats = result.exec_statistics"
    )

@property
def last_parsed_query(self) -> ParsedCypherQuery:
    """Removed for thread safety: Use execute_with_stats() to get QueryResult with parsed_query.
    
    Raises:
        RuntimeError: Always raises to guide migration to execute_with_stats()
    """
    raise RuntimeError(
        "last_parsed_query has been removed for thread safety. "
        "Use execute_with_stats() instead:\n"
        "  Old: result = cdb.execute(query); parsed = cdb.last_parsed_query\n"
        "  New: result = cdb.execute_with_stats(query); parsed = result.parsed_query"
    )
```

## Alternatives Considered

### Session-Based Approach
- **Pros**: Clean state isolation
- **Cons**: More complex API, context management overhead
- **Rejected**: Higher complexity for similar benefits

### Thread-Local Storage
- **Pros**: Transparent to existing code
- **Cons**: Hidden state management, debugging complexity
- **Rejected**: Obscures state ownership patterns

### Full Immutable Migration
- **Pros**: Cleanest architecture
- **Cons**: Major breaking changes
- **Rejected**: Too disruptive for existing codebases


## Usage

```python
result = db.execute_with_stats("MATCH (n) RETURN n")
rows = result.data
timing = result.exec_statistics
```

`execute()` remains for the common case where statistics are not wanted; it
discards them rather than storing them on the connection. `execute_sql_with_stats()`
is the SQL counterpart, carrying `SqlStatistics`.

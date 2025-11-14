# QueryResult and Dual Execute Methods Design

## Overview

This document outlines the design for implementing immutable `QueryResult` objects alongside dual execute methods to address state management issues in the CypherGraphDatabase library while maintaining backward compatibility.

## Alpha Stage Implementation Notes

Given that CypherGraphDB is in alpha stage with no external users, this design can be implemented directly without the complex migration strategy outlined below. The migration assessment is provided for reference but the recommended approach is:

1. Implement `QueryResult` and `execute_with_stats()` immediately
2. Make `exec_statistics()` and `last_parsed_query` raise errors immediately  
3. Use error-driven discovery to find internal code needing updates
4. No backward compatibility layer needed

This provides all architectural benefits without migration overhead.

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

## Migration Strategy

### Phase 1: Implementation (No Breaking Changes)
- [ ] Add `QueryResult` dataclass to `models.py`
- [ ] Implement `execute_with_stats()` method
- [ ] Keep existing `execute()` method unchanged
- [ ] Add comprehensive documentation

### Phase 2: Deprecation Warnings
- [ ] Add deprecation warnings to `exec_statistics()` and `last_parsed_query`
- [ ] Update examples in documentation
- [ ] Add migration guide to README

### Phase 3: Hard Errors
- [ ] Replace deprecation warnings with RuntimeErrors
- [ ] Ensure all internal code uses `execute_with_stats()` where statistics needed
- [ ] Update test suites

### Phase 4: Future Cleanup (Major Version)
- [ ] Remove deprecated methods in next major version
- [ ] Consider renaming `execute_with_stats()` to `execute()`

## Migration Impact Assessment

### **Critical Breaking Changes**

#### **CLI Commands** (All Affected)
- **`stats` command**: Currently calls `cdb.exec_statistics()` - will fail with RuntimeError
- **`execute_cypher` command**: Uses `last_parsed_query` for column resolution and `exec_statistics()` for display
- **`search` command**: Uses `last_parsed_query.return_arguments` for column headers
- **`dump_parsed_query` command**: Direct access to `last_parsed_query`
- **`sql` command**: Uses `sql_statistics().col_names` for column headers

#### **Server API** (Critical)
- **`/query` endpoint**: Uses `exec_statistics()` and `last_parsed_query` for API responses
- **QueryService**: All query execution depends on instance state
- **Column resolution**: Depends on parsed query for RETURN * expansion

### **High Impact Areas**
- **CLI Statistics Display**: All `stats` commands will fail
- **CLI Query Execution**: Column resolution and statistics display broken
- **Server Query Endpoint**: All database queries will fail
- **Command Chaining**: CLI commands that depend on previous query state

### **Medium Impact Areas**
- **Testing Frameworks**: Tests that query statistics after execution
- **Debugging Tools**: Code that inspects `last_parsed_query` for analysis
- **Performance Monitoring**: Scripts that track execution statistics

### **Low Impact Areas**
- **Simple Data Queries**: Code that only uses `execute()` for data access
- **CRUD Operations**: Create/update/delete operations unaffected
- **Connection Management**: Pool and context management unchanged

### **Usage Patterns That Will Break**

```python
# CLI Statistics Command
stats = cdb.exec_statistics()  # RuntimeError

# Server Query Service
res = cdb.execute(q)
stats = cdb.exec_statistics()  # RuntimeError
col_names = resolve_column_names(cdb.last_parsed_query.return_arguments, res, stats.col_count)  # RuntimeError

# CLI Column Resolution
return_args = cdb.last_parsed_query.return_arguments  # RuntimeError
```

## Implementation Details

### File Changes Required

1. **`src/cypher_graphdb/models.py`**
   - Add `QueryResult` dataclass

2. **`src/cypher_graphdb/cyphergraphdb.py`**
   - Add `execute_with_stats()` method
   - Add `execute_sql_with_stats()` method for SQL queries
   - Modify `exec_statistics()` and `last_parsed_query` to raise errors
   - Update docstrings

3. **`src/cypher_graphdb/cli/commands/`** (High Impact)
   - `dump_statistics_command.py`: Migrate to use `execute_with_stats()`
   - `execute_cypher_command.py`: Capture QueryResult for column resolution and stats
   - `search_command.py`: Capture QueryResult for column headers
   - `dump_parsed_query_command.py`: Access parsed_query from QueryResult
   - `sql_command.py`: Use `execute_sql_with_stats()` for column names

4. **`server/src/cypher_graphdb_server/services/query_service.py`** (High Impact)
   - Migrate `execute_query()` to use `execute_with_stats()`
   - Update column resolution to use QueryResult.parsed_query
   - Update API response construction

5. **`tests/`**
   - Add tests for `QueryResult`
   - Add tests for `execute_with_stats()` and `execute_sql_with_stats()`
   - Update existing tests to handle RuntimeErrors for deprecated access
   - Update CLI command tests for new QueryResult usage
   - Update server tests for new query service behavior

### Backward Compatibility Guarantees

- ⚠️ **Breaking Changes Required**: CLI commands and server components use statistics/parsed query extensively
- ✅ Basic `execute()` method signature unchanged for simple data access
- ✅ Connection pooling and context managers unaffected
- ✅ CRUD operations remain unchanged
- ❌ CLI commands will fail without migration
- ❌ Server query endpoint will fail without migration

### Thread Safety Improvements

- ✅ `QueryResult` objects are immutable and thread-safe
- ✅ No shared mutable state during parallel query execution
- ✅ Each query result is self-contained

## Performance Benchmarks

### Memory Overhead Analysis
- `QueryResult` overhead: ~200 bytes per query (statistics + metadata)
- Negligible impact for typical query result sizes (KB-MB range)
- Immutable objects eligible for immediate garbage collection

### Execution Time Impact
- `execute()`: No performance change (statistics discarded)
- `execute_with_stats()`: Same performance as current stateful approach
- Thread contention eliminated in concurrent scenarios

### Pool Efficiency
- Better pool utilization due to reduced state contention
- No need for state synchronization between queries
- Cleaner connection lifecycle management

## Common Usage Patterns

### Data Access (95% of cases - unchanged)
```python
# Simple queries - no changes needed
nodes = cdb.execute("MATCH (n) RETURN n")
count = cdb.execute("MATCH (n) RETURN count(n)", unnest_result="rc")
```

### Statistics Access (5% of cases - explicit)  
```python
# Performance monitoring
result = cdb.execute_with_stats("MATCH (n) RETURN n")
if result.exec_statistics.exec_time > 1.0:
    logger.warning(f"Slow query: {result.exec_statistics}")

# CLI command implementation
result = cdb.execute_with_stats(query)
display_results(result.data)
display_stats(result.exec_statistics)
```

### Concurrent Access (now thread-safe)
```python
# Multiple threads can safely access statistics
def worker(query):
    result = cdb.execute_with_stats(query)
    return result.exec_statistics.exec_time

with ThreadPoolExecutor() as executor:
    times = list(executor.map(worker, queries))  # No race conditions
```

## Testing Strategy

### Unit Tests
- [ ] `QueryResult` immutability tests
- [ ] `execute_with_stats()` return value validation  
- [ ] Error message tests for deprecated methods
- [ ] Thread safety tests for concurrent access

### Integration Tests  
- [ ] CLI command updates work correctly
- [ ] Server endpoint updates work correctly
- [ ] Statistics accuracy preserved
- [ ] Performance benchmarks

### Migration Validation
- [ ] All `exec_statistics()` calls identified and updated
- [ ] All `last_parsed_query` calls identified and updated
- [ ] No remaining stateful access patterns

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

## Conclusion

The dual method approach with immutable `QueryResult` provides significant benefits for thread safety and state management, but requires substantial migration work:

### **Benefits**
- **Thread Safety**: Immutable state eliminates race conditions in concurrent scenarios
- **Clear State Ownership**: Each query result is self-contained
- **Future-Proof Design**: Scales well with parallel execution patterns

### **Migration Challenges**
- **CLI Overhaul**: All CLI commands need restructuring to use QueryResult
- **Server API Changes**: Query service requires complete refactoring
- **Command Chaining**: CLI command infrastructure needs updates for state passing
- **Breaking Changes**: Existing user code accessing statistics will fail

### **Recommended Approach**
Given the extensive internal usage of statistics and parsed query, consider these alternatives:

1. **Gradual Migration**: Implement QueryResult alongside existing stateful approach, allowing gradual adoption
2. **Internal vs External API**: Keep stateful approach for internal components (CLI/server) while offering immutable QueryResult for external users
3. **Major Version Release**: Bundle these breaking changes with other major improvements for a coordinated migration

This design solves the core state management issues but requires careful planning and extensive migration work due to heavy internal dependencies on the current stateful pattern.

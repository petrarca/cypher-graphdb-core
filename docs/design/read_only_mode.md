# Read-Only Mode Design

## Overview

This document describes the design and implementation of read-only mode for the cypher-graphdb library. Read-only mode prevents any write operations (CREATE, DELETE, SET, MERGE, REMOVE) from being executed against the graph database, providing a safe way to connect to production databases for querying and analysis without risk of data modification.

## Goals

- Provide a secure, low-level mechanism to prevent write operations
- Cannot be bypassed by user code or callbacks
- Minimal performance overhead when disabled
- Simple, explicit API
- Support configuration via environment variables
- Works consistently across all backends (AGE, Memgraph, future backends)

## Non-Goals

- Fine-grained permission control (use database-level permissions for that)
- Read-only at database/backend connection level (may be added later as optimization)
- Dynamic toggling of read-only mode after connection establishment

## Architecture

### Core Principle

Read-only validation occurs at the **lowest level** of the execution pipeline - in the backend's `execute_cypher()` method, **before** any user hooks or database communication. This ensures it cannot be bypassed.

### Execution Flow

```
User: db.execute("CREATE (n:Node)")
    ↓
CypherGraphDB.execute()
    ↓
CypherGraphDB._parse_and_execute()
    ↓
ParsedCypherQuery (ANTLR parsing)
    ↓
Backend.execute_cypher(parsed_query)
    ↓
Backend._validate_read_only(parsed_query)  ← VALIDATION (LOW LEVEL)
    ↓ read_only=True + has_updating_clause() → ReadOnlyModeError
    ↓ read_only=False or read-only query → continue
    ↓
[User hooks: on_before_execute]
    ↓
Actual database query execution
```

### Component Changes

#### 1. Backend Base Class (`backend.py`)

Add read-only flag and validation to `CypherBackend`:

```python
class CypherBackend(abc.ABC):
    def __init__(self, _, **kwargs):
        # ... existing initialization ...
        self._read_only = kwargs.get("read_only", False)
    
    @property
    def read_only(self) -> bool:
        """Return True if backend is in read-only mode."""
        return self._read_only
    
    def _validate_read_only(self, parsed_query: ParsedCypherQuery):
        """Validate query is allowed in read-only mode.
        
        Raises:
            ReadOnlyModeError: If query contains write operations
        """
        if self._read_only and parsed_query.has_updating_clause():
            raise ReadOnlyModeError(
                f"Write operation not allowed in read-only mode. "
                f"Query contains updating clause: {parsed_query.parsed_query}"
            )
```

**Key Points:**
- `_read_only` flag stored at backend level
- `_validate_read_only()` method leverages existing `ParsedCypherQuery.has_updating_clause()`
- Validation is protected (underscore prefix) and called internally

#### 2. Backend Implementations

Each backend (`AGEGraphDB`, `MemgraphDB`) calls validation first:

```python
def execute_cypher(
    self,
    cypher_query: ParsedCypherQuery,
    fetch_one: bool = False,
    raw_data: bool = False,
) -> tuple[TabularResult, ExecStatistics]:
    # Validate read-only mode FIRST
    self._validate_read_only(cypher_query)
    
    # Continue with existing execution logic
    # ...
```

#### 3. CypherGraphDB Class (`cyphergraphdb.py`)

Accept read-only parameter and expose it:

```python
class CypherGraphDB:
    def __init__(
        self,
        backend: CypherBackend | str | None = None,
        connect_url: str | None = None,
        connect_params: dict | None = None,
        read_only: bool = False,
    ):
        backend = self._resolve_backend(backend)
        backend._read_only = read_only
        self._backend = backend
        # ... rest of initialization ...
    
    @property
    def read_only(self) -> bool:
        """Return True if connection is in read-only mode."""
        return self._backend.read_only
```

Additional validation in high-level methods:

```python
def create_or_merge(self, obj, strategy=...):
    if self.read_only:
        raise ReadOnlyModeError("create_or_merge not allowed in read-only mode")
    # ... existing code ...

def delete(self, obj, detach=False):
    if self.read_only:
        raise ReadOnlyModeError("delete not allowed in read-only mode")
    # ... existing code ...
```

#### 4. Settings (`settings.py`)

Add environment variable support:

```python
class Settings(BaseSettings):
    # ... existing settings ...
    
    read_only: bool = Field(
        default=False,
        description="Enable read-only mode (prevents write operations)",
        validation_alias="CGDB_READ_ONLY",
    )
```

#### 6. CLI Arguments (`args.py`)

Add `--read-only` / `-r` flag to CLI:

```python
def create_main_app() -> typer.Typer:
    @app.command()
    def main(
        # ... existing parameters ...
        read_only: Annotated[
            bool,
            typer.Option(
                "--read-only",
                "-r",
                help="Enable read-only mode (prevents write operations)",
            ),
        ] = False,
        # ... other parameters ...
    ) -> None:
        """CypherGraph CLI - A command-line interface for graph databases."""
        # Pass read_only in options dict
        options = {
            # ... existing options ...
            "read_only": read_only,
            # ...
        }
```

#### 7. CLI Application (`cli/app.py`)

Pass read-only flag to CypherGraphDB when connecting:

```python
class CypherGraphCLI:
    def connect(self, backend, cinfo, graph_name, read_only=False):
        # Create CypherGraphDB with read_only flag
        self.db = CypherGraphDB(
            backend=backend,
            read_only=read_only
        )
        self.db.connect(cinfo, graph_name=graph_name)
```

### Updating Clause Detection

The implementation leverages existing ANTLR-based parsing:

- `ParsedCypherQuery.has_updating_clause()` returns `True` if query contains:
  - `CREATE`
  - `DELETE`
  - `SET`
  - `REMOVE`
  - `MERGE`
- Detection happens during parsing via `CypherQueryListener`
- Each clause has `updating_clause: bool` flag set by parser

### SQL Execution Handling

For `execute_sql()` method, implement blanket blocking:

```python
def execute_sql(
    self,
    sql_str: str,
    fetch_one: bool = False,
    raw_data: bool = False,
) -> tuple[TabularResult, ExecStatistics, SqlStatistics]:
    if self._read_only:
        raise ReadOnlyModeError(
            "Direct SQL execution not allowed in read-only mode"
        )
    # ... existing implementation ...
```

**Rationale:** SQL parsing for write detection is complex and error-prone. Blanket blocking provides maximum security. Users who need SQL queries should use Cypher instead.

## API

### Instantiation

```python
# Direct parameter
db = CypherGraphDB(backend="memgraph", read_only=True)

# Via environment variable
# export CGDB_READ_ONLY=true
db = CypherGraphDB()

# With context manager
with CypherGraphDB(backend="memgraph", read_only=True) as db:
    db.connect()
    db.execute("MATCH (n) RETURN n")  # OK
    db.execute("CREATE (n)")          # Raises ReadOnlyModeError
```

### Connection

```python
# Read-only set at instantiation, applies to connection
db = CypherGraphDB(read_only=True)
db.connect("bolt://localhost:7687")

# Check mode
if db.read_only:
    print("Connected in read-only mode")
```

### CLI Support

```bash
# Via flag
cypher-graphdb --backend memgraph --read-only

# Via environment variable
export CGDB_READ_ONLY=true
cypher-graphdb --backend memgraph

# Short form
cypher-graphdb -b memgraph -r
```

## Error Handling

### Read-Only Violations

```python
db = CypherGraphDB(read_only=True)
db.connect()

try:
    db.execute("CREATE (n:Person {name: 'Alice'})")
except ReadOnlyModeError as e:
    print(f"Operation blocked: {e}")
    # Output: "Write operation not allowed in read-only mode. 
    #          Query contains updating clause: CREATE (n:Person {name: 'Alice'})"
```

### Method-Level Blocking

```python
db = CypherGraphDB(read_only=True)
db.connect()

node = Product(name="Test")
try:
    db.create_or_merge(node)
except ReadOnlyModeError as e:
    print(f"Operation blocked: {e}")
    # Output: "create_or_merge not allowed in read-only mode"
```

## Security Considerations

### Why Low-Level?

1. **Cannot Be Bypassed**: User callbacks (`on_before_execute`) execute after validation
2. **Single Source of Truth**: Validation in backend base class, inherited by all implementations
3. **Early Failure**: Blocks before network communication with database
4. **Protected Methods**: Validation methods use underscore prefix, signaling internal use

### What's Protected?

- `execute()` with write operations (CREATE, DELETE, SET, MERGE, REMOVE)
- `create_or_merge()` method
- `delete()` method
- `execute_sql()` method (all SQL blocked)

### What's Allowed?

- `execute()` with read-only queries (MATCH, RETURN, WITH, etc.)
- `fetch()`, `fetch_nodes()`, `fetch_edges()` methods
- `parse()` method (no execution)
- `labels()`, `graphs()`, `exec_statistics()` methods

## Performance

- **Zero overhead** when `read_only=False` (default)
- **Minimal overhead** when `read_only=True`:
  - Single boolean check
  - Leverages existing `has_updating_clause()` (already computed during parsing)
  - No additional parsing or string manipulation

## Testing Strategy

### Unit Tests

1. Test read-only flag propagation from `CypherGraphDB` to backend
2. Test `_validate_read_only()` method directly
3. Test each updating clause type (CREATE, DELETE, SET, MERGE, REMOVE)
4. Test read-only queries pass validation
5. Test high-level method blocking (`create_or_merge`, `delete`)
6. Test SQL execution blocking

### Integration Tests

1. Test with real Memgraph connection
2. Test with real AGE connection
3. Test environment variable loading
4. Test context manager behavior

### Test Cases

```python
def test_read_only_blocks_create():
    db = CypherGraphDB(backend="memgraph", read_only=True)
    db.connect()
    with pytest.raises(ReadOnlyModeError):
        db.execute("CREATE (n:Test)")

def test_read_only_allows_match():
    db = CypherGraphDB(backend="memgraph", read_only=True)
    db.connect()
    result = db.execute("MATCH (n) RETURN count(n)")
    assert result is not None

def test_read_only_blocks_high_level_methods():
    db = CypherGraphDB(backend="memgraph", read_only=True)
    db.connect()
    node = TestNode(name="test")
    with pytest.raises(ReadOnlyModeError):
        db.create_or_merge(node)
```

## Future Enhancements

### Phase 2 (Optional)

1. **Backend Connection Read-Only**: Configure backend database connection as read-only
   - PostgreSQL (AGE): `SET default_transaction_read_only = on`
   - Memgraph: Use read-only replica connections
   
2. **Audit Logging**: Log attempted write operations in read-only mode
   
3. **Granular Control**: Allow specific operations (e.g., allow SET but block CREATE)

### Non-Features

These will **not** be implemented:

- Dynamic toggling of read-only mode (security risk)
- Transaction-level read-only control (use database features)
- Fine-grained ACL (use database-level permissions)

## Migration Path

This is a **new feature** with no breaking changes:

1. Default behavior unchanged (`read_only=False`)
2. Existing code works without modification
3. Opt-in via explicit parameter or environment variable
4. No changes to existing method signatures (except optional parameter)

## Documentation Updates

Required documentation changes:

1. **README.md**: Add read-only mode to "Quick Start" section
2. **Usage Guide**: Add dedicated section on read-only connections
3. **API Reference**: Document `read_only` parameter and property
4. **CLI Help**: Document `--read-only` flag
5. **Examples**: Add example script demonstrating read-only mode

## Implementation Checklist

- [ ] Add `ReadOnlyModeError` exception
- [ ] Add `read_only` field to `Settings`
- [ ] Add `_read_only` flag to `CypherBackend.__init__()`
- [ ] Add `read_only` property to `CypherBackend`
- [ ] Add `_validate_read_only()` method to `CypherBackend`
- [ ] Update `AGEGraphDB.execute_cypher()` with validation call
- [ ] Update `MemgraphDB.execute_cypher()` with validation call
- [ ] Update `CypherBackend.execute_sql()` with read-only check
- [ ] Add `read_only` parameter to `CypherGraphDB.__init__()`
- [ ] Add `read_only` property to `CypherGraphDB`
- [ ] Add read-only checks to `create_or_merge()` method
- [ ] Add read-only checks to `delete()` method
- [ ] Add `--read-only` / `-r` flag to CLI (`args.py`)
- [ ] Update CLI to pass `read_only` to `CypherGraphDB` (`cli/graphdb.py` or `cli/app.py`)
- [ ] Display read-only mode indicator in CLI prompt/banner
- [ ] Write unit tests for validation logic
- [ ] Write integration tests with real backends
- [ ] Test CLI with `--read-only` flag
- [ ] Update documentation (README, usage guide, API reference, CLI help)
- [ ] Add example scripts

## Questions & Decisions

### Resolved

1. **Q**: Should read-only be enforced via hooks or low-level?
   - **A**: Low-level in backend, cannot be bypassed

2. **Q**: How to handle SQL execution?
   - **A**: Blanket block all SQL in read-only mode

3. **Q**: Should read-only be changeable after connection?
   - **A**: No, immutable after initialization (security)

### Open

1. **Q**: Should we display a read-only indicator in the CLI prompt?
   - **Recommendation**: Yes, something like `[READ-ONLY] cypher-graphdb>` or similar

2. **Q**: Should we also set backend DB connection to read-only?
   - **Recommendation**: Phase 2, backend-specific implementation

3. **Q**: Should `commit()` be blocked in read-only mode?
   - **Recommendation**: No, harmless if no writes occurred

## References

- **Cypher Parser**: `lib/src/cypher_graphdb/cypherparser.py`
- **Backend Base**: `lib/src/cypher_graphdb/backend.py`
- **CypherGraphDB**: `lib/src/cypher_graphdb/cyphergraphdb.py`
- **Settings**: `lib/src/cypher_graphdb/settings.py`
- **AGE Backend**: `lib/src/cypher_graphdb/backends/age/agegraphdb.py`
- **Memgraph Backend**: `lib/src/cypher_graphdb/backends/memgraph/memgraphdb.py`

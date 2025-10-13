# Read-Only Mode Design

## Overview

Read-only mode prevents write operations (CREATE, DELETE, SET, MERGE, REMOVE) from being executed against the graph database. This provides a safe way to connect to production databases for querying without risk of data modification.

## Design Goals

- **Secure**: Cannot be bypassed by user code or callbacks
- **Simple**: Single boolean flag with clear semantics
- **Flexible**: Multiple configuration methods (code, CLI, environment)
- **Performant**: Zero overhead when disabled, minimal when enabled
- **Universal**: Works across all backends (AGE, Memgraph, future)

## Core Architecture

### Validation Strategy

Read-only validation occurs at the **lowest level** - in the backend's `execute_cypher()` method, before any user hooks or database communication.

```
User Query → Parse → Backend Validation → [If valid] → User Hooks → Database
                           ↓
                    [If write in RO mode]
                           ↓
                   ReadOnlyModeError
```

**Why Low-Level?**
- Cannot be bypassed by user code
- Single validation point for all backends
- Fails fast before database communication
- Consistent behavior across the library

### Detection Mechanism

Uses existing ANTLR parser's `ParsedCypherQuery.has_updating_clause()`:
- Detects: CREATE, DELETE, SET, MERGE, REMOVE
- Already computed during query parsing
- Zero additional parsing overhead

### State Management

The `read_only` flag is stored at the backend level:
- Set via `connect(read_only=True)` or `db.read_only = True`
- Can be toggled at runtime: `db.read_only = True/False`
- Merges with environment variable (`CGDB_READ_ONLY`)
- Explicit parameters override environment variables

## Usage Patterns

### 1. Connection Time
```python
db = CypherGraphDB()
db.connect(read_only=True)
```

### 2. Runtime Toggle
```python
db = CypherGraphDB()
db.connect()

db.read_only = True   # Enable read-only
db.read_only = False  # Disable read-only
```

### 3. Environment Variable
```bash
export CGDB_READ_ONLY=true
```

### 4. CLI
```bash
cypher-graphdb --read-only
cypher-graphdb -r  # short form
```

## Component Responsibilities

### Backend (`backend.py`)
- Stores `_read_only` flag
- Provides `read_only` property (getter/setter)
- Implements `_validate_read_only(parsed_query)` method
- Calls validation before query execution

### CypherGraphDB (`cyphergraphdb.py`)
- Passes `read_only` parameter to backend during connect
- Exposes `read_only` property (delegates to backend)
- Additional checks in `create_or_merge()` and `delete()` methods

### Settings (`settings.py`)
- Defines `read_only` field with `CGDB_READ_ONLY` environment variable
- Provides default value (False)

### CLI (`args.py`, `cli/app.py`)
- Adds `--read-only/-r` flag
- Passes flag through to CypherGraphDB

### Exceptions (`exceptions.py`)
- Defines `ReadOnlyModeError` exception
- Raised when write operations attempted in read-only mode

## Security Model

### Protected Operations
- `execute()` with write queries
- `create_or_merge()` method
- `delete()` method  
- `execute_sql()` (all SQL blocked)

### Allowed Operations
- `execute()` with read-only queries (MATCH, RETURN, WITH)
- `fetch()`, `fetch_nodes()`, `fetch_edges()`
- All metadata queries

### Defense Layers
1. **Primary**: Backend validation in `execute_cypher()`
2. **Secondary**: Explicit checks in high-level methods
3. **Conservative**: Blanket SQL blocking (hard to parse for writes)

## Design Decisions

### Why Runtime Toggle?

**Decision**: Allow `db.read_only = True/False` at runtime

**Rationale**:
- Flexibility for applications that switch between modes
- Simpler API than requiring reconnection
- No security compromise (validation still enforced)
- Common pattern in database drivers

### Why Block All SQL?

**Decision**: Block all `execute_sql()` in read-only mode

**Rationale**:
- SQL write detection is complex and error-prone
- Conservative approach maximizes security
- Users can use Cypher for queries instead
- Avoids maintaining SQL parser

### Why Connection-Level Parameter?

**Decision**: Accept `read_only` as parameter to `connect()`

**Rationale**:
- Consistent with existing patterns (`cinfo`, `graph_name`)
- Natural merge with environment variables
- Industry standard (PostgreSQL, Neo4j use session-level)
- Clear ownership (backend property)

## Performance

| Mode | Overhead |
|------|----------|
| `read_only=False` (default) | None |
| `read_only=True` | Single boolean check + existing parse flag |

No additional parsing, string manipulation, or network overhead.

## Future Enhancements

### Potential Additions
- Backend-native read-only connections (PostgreSQL, Memgraph replicas)
- Audit logging of blocked operations
- Granular operation control (allow some writes, block others)

### Non-Goals
- Fine-grained ACL (use database permissions)
- Transaction-level control (use database features)
- Automatic read-only detection

## Migration

**No breaking changes:**
- Default is `read_only=False` (existing behavior)
- Opt-in feature via explicit parameter or environment variable
- All existing code continues to work unchanged

## References

**Core Files:**
- `src/cypher_graphdb/backend.py` - Base validation logic
- `src/cypher_graphdb/cyphergraphdb.py` - Main API
- `src/cypher_graphdb/exceptions.py` - ReadOnlyModeError
- `src/cypher_graphdb/settings.py` - Environment variable support
- `src/cypher_graphdb/args.py` - CLI flag definition

**Backend Implementations:**
- `src/cypher_graphdb/backends/age/agegraphdb.py`
- `src/cypher_graphdb/backends/memgraph/memgraphdb.py`

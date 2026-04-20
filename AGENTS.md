# Agent Instructions

This file contains project-specific rules and conventions for AI coding assistants working on cypher-graphdb-core.

## Project Overview

cypher-graphdb-core is the backend-agnostic Python library for working with Cypher-based graph databases. It provides:

- **CypherGraphDB**: Facade class composing multiple mixins for graph operations
- **Backend abstraction**: `CypherBackend` ABC with pluggable backends (AGE, Memgraph)
- **cgdb-cli**: Interactive REPL and non-interactive CLI for graph database operations
- **Bulk operations**: `bulk_create_nodes`, `bulk_create_edges` for high-throughput writes
- **Index management**: `create_property_index`, `drop_index`, `list_indexes` with backend capability detection

### Related Projects

- **cypher-graphdb** (parent repo): FastAPI server + React explorer frontend
- **cypher-graphdb/server**: Depends on this core library via editable install
- **coco-codegraph-poc**: AST-based code graph builder using this library

## Project Structure

```
src/cypher_graphdb/
  __init__.py              # Public API exports (CypherGraphDB, BackendCapability, IndexInfo, etc.)
  backend.py               # CypherBackend ABC, BackendCapability enum
  backendprovider.py       # Backend discovery and registration
  settings.py              # PydanticSettings configuration
  statistics.py            # GraphStatistics, LabelStatistics, IndexType, IndexInfo models
  cypherbuilder.py         # Parameterized Cypher query builder
  cypherparser.py          # Cypher query parser
  models.py                # GraphNode, GraphEdge, GraphPath, Graph models

  cyphergraphdb/           # CypherGraphDB facade (composed of mixins)
    cyphergraphdb.py        # Main class: CypherGraphDB(ConnectionMixin, BatchMixin, IndexingMixin, ...)
    connection.py           # Connection management mixin
    batch.py                # BatchMixin ($params-based batch operations)
    indexing.py             # IndexingMixin (create_property_index, bulk_create_*, list_indexes)
    schema.py               # SchemaMixin (graphs, labels, create/drop graph)
    search.py               # SearchMixin (fulltext search)
    sql.py                  # SqlMixin (raw SQL execution)
    stream_mixin.py         # StreamMixin (streaming query results)
    criteria.py             # MatchNodeCriteria, MatchEdgeCriteria

  backends/
    age/                    # Apache AGE (PostgreSQL) backend
      agegraphdb.py          # AGEGraphDB: CypherBackend implementation
      agesqlbuilder.py       # SQL builder for AGE-specific queries
      ageserializer.py       # Inline Cypher literal serializer (UNWIND workaround)
      agerowfactories.py     # Row factory for AGE agtype parsing
      agesearch.py           # Full-text search via PostgreSQL tsvector
      agtype.py              # agtype data type loader
    memgraph/               # Memgraph backend

  cli/                      # cgdb-cli interactive REPL and command system
    app.py                   # CypherGraphCLI main application
    graphdb.py               # CLIGraphDB wrapper (connect, create/drop/use graph)
    commands/                # Command implementations (one file per command)
      __init__.py             # Command registry (all commands registered here)
      base_command.py         # BaseCommand ABC
      dump_graphs_command.py  # 'graphs' command
      dump_indexes_command.py # 'indexes' command
      dump_labels_command.py  # 'labels' command
      create_graph_command.py # 'create graph' command
      drop_graph_command.py   # 'drop graph' command
      use_graph_command.py    # 'use graph' / 'use' command
      graph_exists_command.py # 'graph exists' command
      ...                     # ~40 total commands
    help/                    # Markdown help files for commands
      _overview.md            # Help overview (shown by 'help' command)
    renderer.py              # Output rendering (json, table, list formats)
    completer.py             # Tab completion for interactive mode
    promptparser.py          # Command parsing (PromptParserCmd model)
    command_registry.py      # CommandRegistry singleton
```

## Build & Test

```bash
task fct          # Format + check + test (always run before committing)
task format       # ruff format
task check        # ruff check --fix
task test         # pytest
```

## CLI Commands (cgdb-cli)

### Running the CLI

```bash
# Interactive REPL
cgdb-cli --graph my_graph

# Non-interactive (execute and exit)
cgdb-cli --graph my_graph -e "labels"
cgdb-cli --graph my_graph --table -e "graphs"
cgdb-cli --graph my_graph --json -e "indexes"

# With explicit connection
CGDB_CINFO=postgresql://user:pass@localhost:5432/db CGDB_BACKEND=age cgdb-cli --graph my_graph
```

### Available CLI Commands

**Graph management:**
- `graphs` -- List all graphs in the database
- `create graph <name>` -- Create a new graph (requires `--yes` in non-interactive mode)
- `drop graph <name>` -- Drop a graph (requires `--yes` in non-interactive mode)
- `use graph <name>` / `use <name>` -- Switch to a different graph
- `graph exists <name>` -- Check if a graph exists

**Inspection:**
- `labels` -- Show all node/edge labels with counts
- `indexes` -- List user-created property indexes
- `indexes all` -- List all indexes including backend internals
- `stats` / `statistics` -- Show query execution statistics
- `backends` -- List available backends

**Output format:**
- `--json` / `-j` -- JSON output (default for non-interactive)
- `--table` / `-t` -- Table output (default for interactive)
- `json`, `table`, `list` -- Change format at runtime

**Cypher queries:**
Any input not matching a command is executed as a Cypher statement.

### Adding a New CLI Command

1. Create `src/cypher_graphdb/cli/commands/my_command.py`:
```python
from cypher_graphdb.cli.commands.base_command import BaseCommand

class MyCommand(BaseCommand):
    command_name = "my_command"
    command_map_entry = BaseCommand.create_command_map_entry(
        pattern="[[my_command", tokens=["my command"]
    )

    def execute(self, parsed_cmd):
        result = ...  # Your logic
        return self._post_processing_cmd(
            parsed_cmd, result,
            render_kwargs={"col_headers": ["Col1", "Col2"]},
        )
```

2. Register in `cli/commands/__init__.py`:
```python
from cypher_graphdb.cli.commands.my_command import MyCommand
registry.register(MyCommand)
```

3. Add tab-completion via the `completion` class attribute (no completer.py changes needed):
```python
# Static list
completion = ["all", "verbose"]
# Dynamic provider
completion = "graphs"       # or "variables", "config"
# Label+property completion
completion = "label_props"  # or "label_only"
# Fine-grained control
completion = {"type": "label_props", "complete_mandatory_props": True}
```
4. Update `cli/help/_overview.md` with the new command.

## Backend Architecture

### CypherBackend ABC (`backend.py`)

All backends implement `CypherBackend`. Methods are either abstract (required) or opt-in (default raises `NotImplementedError`):

**Abstract (required):** `connect`, `disconnect`, `execute_cypher`, `create_graph`, `drop_graph`, `graphs`, `labels`, `commit`, `rollback`

**Opt-in (check with `has_capability`):**
- `create_property_index`, `drop_index`, `list_indexes` -- requires `BackendCapability.PROPERTY_INDEX`
- `bulk_create_nodes`, `bulk_create_edges` -- backend-specific bulk write

### BackendCapability Enum

```python
from cypher_graphdb import BackendCapability

if cdb.has_capability(BackendCapability.PROPERTY_INDEX):
    cdb.create_property_index("Label", "prop1", "prop2")
```

Capabilities: `LABEL_FUNCTION`, `SUPPORT_MULTIPLE_LABELS`, `STREAMING_SUPPORT`, `PROPERTY_INDEX`, `UNIQUE_CONSTRAINT`, `FULLTEXT_INDEX`, `VECTOR_INDEX`

### AGE Backend Specifics

- AGE stores properties in a single `agtype` JSON column per label table
- GIN indexes cover all properties (property_names parameter is ignored)
- UNWIND requires inline Cypher literals (no `$params`), handled by `ageserializer.py`
- Label tables are created lazily on first node insert
- Graph = PostgreSQL schema; label = table within that schema

## Code Quality

### SQL Safety

- Use `psycopg.sql.SQL/Identifier/Placeholder` for parameterized queries (no f-string SQL)
- `cypherbuilder.py` uses parameterized queries for all node/edge criteria
- AGE serializer (`ageserializer.py`) handles escaping for inline literals

### Logging

Use loguru's `{}` placeholder format: `logger.debug("Found {} files", count)`, not f-strings.

### Complexity

McCabe max-complexity = 10. Refactor complex functions into helpers.

## Git Conventions

- Commit prefixes: `feat:`, `fix:`, `refactor:`, `chore:`, `docs:`, `test:`
- Short commit messages, max two sentences
- Current feature branch: `feat/bulk-write-and-property-indexes`

## Smoke Testing

```bash
# Test CLI against live AGE
CGDB_CINFO=postgresql://postgres:postgres@localhost:8432/graphdb CGDB_BACKEND=age \
  cgdb-cli --graph my_graph --table -e "labels"

# Test bulk operations via Python
from cypher_graphdb import CypherGraphDB
cdb = CypherGraphDB("age")
cdb.connect(cinfo="...", graph_name="test", create_graph=True)
cdb.bulk_create_nodes("Label", [{"id": "1", "name": "foo"}])
cdb.create_property_index("Label", "id", "name")
print(cdb.list_indexes())
```

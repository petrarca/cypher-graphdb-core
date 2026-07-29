# Agent Instructions

> **Org-wide standards:** this repo follows the Petrarca Labs engineering
> handbook. Before changing releases, tests, dependencies, Taskfile tasks,
> commits, or tsconfig/`SONNET_UI_LOCAL`, read
> [`../engineering-handbook/INDEX.md`](../engineering-handbook/INDEX.md) and
> prefer those standards over local defaults. See `ENGINEERING-STANDARDS.md`.

This file contains project-specific rules and conventions for AI coding assistants working on cypher-graphdb-core.

## Project Overview

cypher-graphdb-core is the backend-agnostic Python library for working with Cypher-based graph databases. It provides:

- **CypherGraphDB**: Facade class composing multiple mixins for graph operations
- **Backend abstraction**: `CypherBackend` ABC with pluggable backends (AGE, Memgraph)
- **cypher-graphdb**: Interactive REPL and non-interactive CLI for graph database operations
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

  cli/                      # cypher-graphdb interactive REPL and command system
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

## CLI Commands (cypher-graphdb)

### Running the CLI

```bash
# Interactive REPL
cypher-graphdb --graph my_graph

# Non-interactive (execute and exit)
cypher-graphdb --graph my_graph -e "labels"
cypher-graphdb --graph my_graph --table -e "graphs"
cypher-graphdb --graph my_graph --json -e "indexes"

# With explicit connection
CGDB_CINFO=postgresql://user:pass@localhost:5432/db CGDB_BACKEND=age cypher-graphdb --graph my_graph
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

### Declaring a command's behaviour

Three flags on `create_command_map_entry`, all explicit — they used to be
punctuation around the action name (`"[[format_output]]_"`), which nothing
type-checked and four call sites had to decode:

| Flag | Default | Meaning |
|---|---|---|
| `position` | `CommandPosition.FIRST` | Where it may sit in a `a \| b \| c` pipeline. `FIRST` produces data, `LAST` consumes it, `ANY` either, `FIRST_OR_LAST` both ends. A lone command satisfies all of them. |
| `requires_backend` | `True` | Whether a connection must exist first. |
| `splits_args` | `True` | `False` keeps the remainder as one opaque string (raw SQL). |

`CommandPosition` lives in its own module (`cli/commandposition.py`) because
`promptparser` enforces it and `commands/base_command` declares it — importing
either from the other is circular.

### How the prompt is parsed

`cli/promptgrammar.py` holds two Lark grammars: one splits the line into
pipeline segments plus a `||` shell command, the other parses a command's
argument list. Quoting, escapes and nested collections are grammar rules, not
scanner state.

**Matching the leading command tokens stays hand-written** (`starts_with` +
`_parse_cmd_with_map`). The completer parses half-typed input, where a grammar
cannot yet lex a command name that is not finished.

Two behaviours are load-bearing and easy to break — both were regressions during
the Lark migration, and neither was caught by unit tests:

- **Positional arguments stay raw strings.** `create edge WORKS_FOR, <id>, <id>`
  passes internal ids positionally and `PromptParserCmd.args` is `list[str]`, so
  literal-evaluating them fails the command. Only keyword values — and the
  elements of a collection inside one — are evaluated.
- **A kwarg with no value yet is dropped, not bound to `""`.** Otherwise
  `fetch nodes Person, empty=` becomes a filter matching nothing while the user
  is still typing.

When changing this, diff the CLI's output against the previous build on a live
graph. The unit tests did not catch either regression.

## Graph Models (`ModelProvider`)

Typed models are registered on an **explicit** `ModelProvider`. There is no
global provider and no implicit registration -- importing a model module has no
process-wide side effect.

```python
from cypher_graphdb import GraphEdge, GraphNode, ModelProvider

mp = ModelProvider()

@mp.node(label="Product")
@mp.relation(rel_type="USES_TECHNOLOGY", to_type="Technology")
class Product(GraphNode):
    key: str

@mp.edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    version: str | None = None
```

Rules:

- **Always declare a module-level `mp = ModelProvider()`** in a `graph_models`
  module; never expect models to land somewhere by import side effect.
- Registration API is `mp.node()`, `mp.edge()`, `mp.relation()`,
  `mp.extend_relation()`, `mp.extend_relations()` -- all instance methods.
- Wire the provider to the connection/pool so results hydrate typed:
  `CypherGraphDB(..., model_provider=mp)` or `cdb.model_provider = mp`.
  Without one, a connection returns generic `GraphNode`/`GraphEdge`.
- Two graphs may reuse the same label -- separate providers cannot collide.
- Loading from a path (`--model-path`, `SchemaGenerator`,
  `generate_schemas_from_path`) **discovers** providers declared in the loaded
  modules and adopts their models. A module may declare several providers;
  packages are walked recursively. This is import-order independent.

See `docs/design/model-provider-isolation.md`.

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
- `create_property_index` creates **two** kinds of index, and `property_names` is
  **not** ignored: a GIN index on the whole `properties` column (covers the `@>`
  containment operator), **plus a btree expression index per named property**.
  Cypher `WHERE n.prop = ...` compiles to `agtype_access_operator`, which the GIN
  index does not serve — without the per-property btree indexes every lookup is a
  sequential scan. Always pass property names.
- `list_indexes()` parses `pg_indexes`; AGE property indexes are *expression*
  indexes, so property names come from the `'"name"'::agtype` literal, not from a
  column list
- UNWIND requires inline Cypher literals (no `$params`), handled by `ageserializer.py`
- Label tables are created lazily on first node insert
- Graph = PostgreSQL schema; label = table within that schema

## Packaging: the core/CLI split

The library core must stay free of the terminal-UI stack. `[excel]` carries
`openpyxl`; `[cli]` carries `typer`, `rich`, `prompt_toolkit`, `art`, `lark` and
pulls in `[excel]`.

**Invariant: `import cypher_graphdb` must not import any of them.** A regression
here is invisible locally (an editable dev install always has them) and only
breaks a published core-only install. Check with:

```python
import sys; before = set(sys.modules); import cypher_graphdb
assert not {"rich", "typer", "prompt_toolkit", "art"} & {m.split(".")[0] for m in set(sys.modules) - before}
```

Rules:

- `cli/` may import CLI packages freely; nothing outside `cli/` and `args.py` may.
- `tools/` (tabular import/export) re-exports the openpyxl-backed symbols
  lazily, so CSV/JSON/YAML work without `[excel]`. Resolve formats through
  `format_provider` rather than importing importer/exporter classes directly.
- Formats register on the global `format_provider` (like backends, unlike
  models, which are per-graph). `format_name_for_file()` is metadata and must
  never import or raise; `probe()` reports availability without raising;
  `resolve()` is the throwing counterpart.
- `cli/__init__.py` re-exports `CypherGraphCLI` **lazily** (PEP 562). Do not make
  it eager: several downstream projects import `cypher_graphdb.cli.renderer`
  alone, which needs only `rich`, and an eager re-export would drag the whole
  app in.
- Modules on the consumer-facing surface guard their imports with
  `_extras.require_extra("cli", exc)` so the failure names the install rather
  than a package the user never heard of.
- Prefer the stdlib over a heavyweight dependency: `numpy` was removed when the
  only use was one `math.isnan` equivalent.

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

## Documentation

Docs are plain markdown with [OKF](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
frontmatter. **There is no doc build** — no mkdocs, no generated API reference,
no site. Entry point: `docs/README.md`.

| Directory | Contents |
|---|---|
| `docs/usage/` | Task-oriented guides for consumers |
| `docs/design/` | Architecture and the *why*, one doc per concept |
| `docs/adr/` | Decision records (`NNNN-title.md`) |
| `docs/records/` | *(optional, absent today)* point-in-time artifacts; not maintained |

To add a guide: create `docs/usage/my-guide.md` with OKF frontmatter and add a
row to the table in `docs/usage/index.md`. Nothing else to register.

Frontmatter carries **two independent lifecycle axes** — do not conflate them:

- `status`: `draft` | `stable` | `deprecated` — is the *document* trustworthy?
- `implementation`: `proposed` | `in-progress` | `implemented` | `superseded` |
  `abandoned` — does the *thing* exist? (`design/` only.)

A design doc that ships moves to `implementation: implemented` and sheds
whatever the code now owns. **Verify a doc's claimed status against the code
before trusting it** — several were stale by years.

Because there is no generated reference, **docstrings are the API
documentation**. See `CONTRIBUTING.md` for the docstring conventions, and the
org standard in `../engineering-handbook/standards/sdlc/documentation.md`.

## Smoke Testing

```bash
# Test CLI against live AGE
CGDB_CINFO=postgresql://postgres:postgres@localhost:8432/graphdb CGDB_BACKEND=age \
  cypher-graphdb --graph my_graph --table -e "labels"

# Test bulk operations via Python
from cypher_graphdb import CypherGraphDB
cdb = CypherGraphDB("age")
cdb.connect(cinfo="...", graph_name="test", create_graph=True)
cdb.bulk_create_nodes([{"id": "1", "name": "foo"}], label="Label")
cdb.create_property_index("Label", "id", "name")
print(cdb.list_indexes())
```

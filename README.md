# CypherGraphDB Library

## Overview

CypherGraphDB is a unified Python toolkit for working with Cypher-capable graph databases. It provides three core capabilities:

### 1. **Generalized API** - Vendor-Neutral Graph Database Access

Work with multiple graph database backends through a unified, consistent interface:

- **Vendor Neutrality** – Same API works across Apache AGE, Memgraph, and other Cypher databases
- **Extensible Architecture** – Add new backends by implementing the `CypherBackend` abstract class
- **Connection Management** – Pooled connections with automatic reconnection and configuration
- **Query Execution** – Direct Cypher queries with result processing and statistics
- **Data Exchange** – Built-in import/export tools for CSV, Excel, JSON formats
- **Schema Operations** – Graph introspection, schema dumping, and analysis tools

### 2. **Typed ORM-Like Access Layer**

Optional Pydantic-based object mapping for type-safe graph operations:

- **Model Decorators** – `@node`, `@edge`, `@relation` for defining graph schemas
- **Type Safety** – Automatic validation and IDE support with autocompletion
- **Custom Methods** – Add business logic directly to your graph models
- **Schema Generation** – Automatic JSON schema generation from Python models

### 3. **Interactive CLI** - Vendor-Neutral Graph Database Operations

Comprehensive command-line interface that works identically across all supported backends:

- **44+ Commands** – Full suite of graph operations, schema management, and analysis tools
- **Rich Formatting** – Colorized output with tables, trees, and JSON highlighting
- **Cross-Platform** – Same commands work with Apache AGE, Memgraph, or any supported backend
- **Interactive Exploration** – Real-time graph querying and visualization

## Supported Backends

- **age** – Apache AGE extension on PostgreSQL
- **memgraph** – Memgraph database via Bolt protocol

**Extensible:** Add new Cypher-compatible backends by implementing the `CypherBackend` abstract class.

## Prerequisites

| Tool | Purpose | Install |
|------|---------|---------|
| Python 3.13+ | Runtime | `uv python install 3.13` |
| [uv](https://github.com/astral-sh/uv) | Dependency & venv manager | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [Task](https://taskfile.dev) | Automation | `brew install go-task` |
| Docker | Integration tests | `brew install --cask docker` |

## Quick Start

```bash
git clone <repo-url>
cd cypher-graphdb/lib
task install    # Create venv + install library
task test:unit  # Run unit tests
task run:cli    # Launch interactive CLI
```

## Backend Configuration

### Apache AGE (backend="age")

Apache AGE runs as a PostgreSQL extension. Connection strings use PostgreSQL format.

**Key Parameters:**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `host` | PostgreSQL server hostname | `localhost` |
| `port` | PostgreSQL server port | `5432` |
| `dbname` | Database name | (required) |
| `user` | Username for authentication | - |
| `password` | Password for authentication | - |
| `graph_name` | AGE graph name to use/create | - |

**Connection Examples:**

```bash
# CLI with connection info
cypher-graphdb --backend age --cinfo "host=localhost port=5432 dbname=postgres"

# Environment variable
export CGDB_CINFO="postgresql://postgres:secret@localhost:5432/postgres"
cypher-graphdb --backend age
```

### Memgraph (backend="memgraph")

Memgraph uses the Bolt protocol for connections.

**Key Parameters:**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `host` | Memgraph server hostname | `127.0.0.1` |
| `port` | Memgraph Bolt port | `7687` |
| `username` | Username for authentication | (empty) |
| `password` | Password for authentication | (empty) |

**Connection Formats:**

- Bolt URI: `bolt://[username:password@]hostname:port`
- Key=value: `host=localhost port=7687 username=user password=secret`
- Partial: `port=1234` or `host=192.168.1.100`

**Connection Examples:**

```bash
# Bolt URI
cypher-graphdb --backend memgraph --cinfo "bolt://localhost:7687"

# Environment variable
export CGDB_CINFO="bolt://user:pass@localhost:7687"
cypher-graphdb --backend memgraph
```

## 1. Generalized API Usage

### Vendor-Neutral Database Access

The unified API provides consistent access to different graph database backends:

#### Connection (Recommended)

The recommended connection method is `connect_url` for simplicity:

```python
from cypher_graphdb import CypherGraphDB

# Memgraph - recommended
db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

# Apache AGE
db = CypherGraphDB(backend="age", connect_url="postgresql://postgres:secret@localhost:5432/postgres")

# Context manager (recommended - auto-cleanup)
with CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687") as db:
   result = db.execute("RETURN 1 AS value", unnest_result=True)
   print(result)  # 1
```

Alternative connection methods (CINFO parameter, individual params, environment variables) are also supported. See documentation for details.

## Configuration

The library reads configuration from environment variables (can be overridden by CLI flags or constructor parameters):

| Setting | Env Var | CLI Flag | Default | Description |
|---------|---------|----------|---------|-------------|
| backend | `CGDB_BACKEND` | `-b / --backend` | `None` | Backend type (`memgraph`, `age`) |
| cinfo | `CGDB_CINFO` | `-c / --cinfo` | `None` | Connection string / DSN |
| graph | `CGDB_GRAPH` | `-g / --graph` | `None` | Graph name (backend-specific) |
| read_only | `CGDB_READ_ONLY` | `-r / --read-only` | `False` | Block write operations |
| create_graph | `CGDB_CREATE_GRAPH_IF_NOT_EXISTS` | - | `False` | Auto-create graph if missing (AGE only) |

**Precedence:** CLI arguments > Environment variables > Constructor parameters > Defaults

**Example .env files:**
- `.env.memgraph.example` - Pre-configured for Memgraph Docker Compose stack
- `.env.age.example` - Pre-configured for Apache AGE Docker Compose stack

Copy the appropriate example file to `.env` and the library will automatically load it:

```bash
# For Memgraph
cp .env.memgraph.example .env

# For Apache AGE
cp .env.age.example .env
```

### Basic Queries

```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

# Execute query with multiple results
rows = db.execute("MATCH (n:Person) RETURN n.name, n.age LIMIT 5")
for name, age in rows:
    print(f"{name}: {age}")

# Unnest single scalar result
count = db.execute("MATCH (n) RETURN count(n)", unnest_result=True)
print(f"Total nodes: {count}")  # 42

# Fetch one row
result = db.execute("RETURN 1 AS x, 2 AS y", fetch_one=True, unnest_result=True)
print(result)  # (1, 2)
```

### Create & Merge Nodes and Edges

```python
from cypher_graphdb import CypherGraphDB, GraphNode, GraphEdge

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

# Create nodes
person = GraphNode(label_="Person", properties_={"name": "Alice", "age": 30})
person = db.create_or_merge(person)  # Returns node with ID assigned
print(f"Created person with ID: {person.id_}")

company = GraphNode(label_="Company", properties_={"name": "TechCorp"})
company = db.create_or_merge(company)

# Create relationship
rel = GraphEdge.build(person, company, label_="WORKS_FOR", properties_={"since": 2020})
rel = db.create_or_merge(rel)
print(f"Created relationship: {person.properties_['name']} WORKS_FOR {company.properties_['name']}")
```

### Fetch Nodes (Multiple Methods)

```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

# Fetch by database ID
node = db.fetch_nodes(12345, unnest_result=True)

# Fetch by GID (string identifier)
node = db.fetch_nodes("6f628f1e7tFZHfis", unnest_result=True)

# Fetch by properties
nodes = db.fetch_nodes({"label_": "Product", "name": "CypherGraph"})

# Fetch all nodes of a type (with typed models)
from my_models import Product
products = db.fetch_nodes({"label_": Product})
```

### Bulk Data Setup

```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

# Create multiple nodes and relationships in one query
db.execute("""
    CREATE (p1:Person {name: 'Alice', age: 30, email: 'alice@example.com'})
    CREATE (p2:Person {name: 'Bob', age: 25, email: 'bob@example.com'})
    CREATE (c:Company {name: 'TechCorp', founded: 2010})
    CREATE (p1)-[:WORKS_FOR {since: 2020, role: 'Engineer'}]->(c)
    CREATE (p2)-[:WORKS_FOR {since: 2021, role: 'Designer'}]->(c)
    CREATE (p1)-[:KNOWS {since: 2019}]->(p2)
""")
```

## 3. Interactive CLI - Vendor-Neutral Graph Operations

The library includes a comprehensive interactive CLI with 44+ commands for exploring graph databases. The CLI provides identical functionality across all supported backends.

### Running the CLI

```bash
# Using task automation
task run:cli

# Direct execution
.venv/bin/cypher-graphdb
```

### Command Categories

**Connection Management:**
- `connect memgraph bolt://localhost:7687` – Connect to Memgraph
- `connect age postgresql://...` – Connect to Apache AGE
- `disconnect` – Close current connection

**Query Execution:**
- `MATCH (n) RETURN n LIMIT 5;` – Execute any Cypher query
- `stats` – Show last query execution statistics
- `format_output` – Configure result display format

**Graph Operations:**
- `create_node` – Create nodes with properties
- `create_edge` – Create relationships between nodes
- `fetch_nodes` – Retrieve nodes by criteria
- `fetch_edges` – Retrieve edges by criteria
- `delete_graphobj` – Remove nodes or edges

**Schema & Introspection:**
- `dump_schema` – Export graph schema definitions
- `dump_labels` – List all node and edge labels
- `dump_models` – Show registered model classes
- `load_models` – Import model definitions

**Import/Export:**
- `import_graph` – Load data from CSV, Excel, JSON files
- `export_graph` – Export graph data to various formats

**Transaction Control:**
- `commit` – Commit current transaction
- `rollback` – Rollback current transaction

**Utilities:**
- `help` – Show all available commands
- `exit` – Exit CLI (aliases: `quit`, `q`, `bye`)
- `search` – Full-text search across graph data
- `graph_to_tree` – Visualize graph structure as tree

> **Vendor Neutrality:** All CLI commands work identically across Apache AGE, Memgraph, and any other supported backend.

## 2. Typed ORM-Like Access Layer

Define typed models with decorators for better type safety and metadata:

```python
from cypher_graphdb import GraphNode, GraphEdge, node, edge, relation

@node(label="Product")
@relation(rel_type="USES_TECHNOLOGY", to_type="Technology")
class Product(GraphNode):
    name: str
    multi_tenancy: bool | None = None

@node(label="Technology")
class Technology(GraphNode):
    name: str

@edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    version: str | None = None
```

### Using Typed Models

```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

# Create typed nodes
demo_product = Product(name="CypherGraph Demo", multi_tenancy=True)
db.create_or_merge(demo_product)

demo_technology = Technology(name="Python")
db.create_or_merge(demo_technology)

# Create typed relationship
demo_relation = UsesTechnology.build(demo_product, demo_technology, version="3.13")
db.create_or_merge(demo_relation)

# Fetch typed nodes
products = db.fetch_nodes({"label_": Product})
for product in products:
    print(f"Product: {product.name}, Multi-tenant: {product.multi_tenancy}")
```

## Result Unnesting

Simplify common query patterns:

```python
# Single scalar
value = db.execute("RETURN 42", unnest_result=True)  # 42 (not [(42,)])

# Single row
row = db.execute("RETURN 1, 2", fetch_one=True, unnest_result=True)  # (1, 2)
```

## Task Automation

| Task | Purpose |
|------|---------|
| `install` | Install library with dependencies |
| `test:unit` | Run unit tests |
| `test:integration` | Integration tests (requires Docker) |
| `format` | Format code with Ruff |
| `check` | Lint and check code quality |
| `run:cli` | Run interactive CLI |
| `fct` | Format + check + test |

## Development Workflow

See root [`README.md`](../README.md) for full project development workflow.

## Documentation

Library documentation is built with MkDocs:

```bash
task build:docs   # Build docs site
```

Output: `site/` directory. API reference auto-generated from docstrings.

## License

See [`LICENSE.md`](../LICENSE.md) (MIT).

---
Happy graph hacking!

# CypherGraphDB Library

## Overview

CypherGraphDB provides a lightweight, model-aware Python interface and CLI for working with Cypher-capable graph databases (Apache AGE, Memgraph). Key capabilities:

- **Unified API** – Work with multiple graph backends through consistent interface
- **Typed models** – Optional Pydantic-based decorators (@node, @edge, @relation)
- **Query execution** – Direct Cypher queries with result unnesting and statistics
- **Interactive CLI** – Explore graph databases with syntax highlighting and formatting
- **Import/Export** – Tools for CSV, Excel, JSON data exchange


## Supported Backends

- **age** – Apache AGE extension on PostgreSQL
- **memgraph** – Memgraph database via Bolt protocol

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

## Using the Library

### Connection (Recommended)

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

## CLI (Interactive Graph Exploration)

The library includes an interactive CLI for exploring graph databases.

### Running the CLI

```bash
# Using task automation
task run:cli

# Direct execution
.venv/bin/cypher-graphdb
```

### Common Commands

| Command | Purpose |
|---------|---------|
| `connect memgraph bolt://localhost:7687` | Connect to Memgraph |
| `connect age postgresql://...` | Connect to Apache AGE |
| `MATCH (n) RETURN n LIMIT 5;` | Execute Cypher query |
| `:help` | Show available commands |
| `:stats` | Show last query statistics |
| `:exit` | Exit CLI |

## Typed Models

Define typed models with decorators for better type safety and metadata:

```python
from cypher_graphdb import GraphNode, GraphEdge, node, edge, relation

@node(metadata={"category": "software"})
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

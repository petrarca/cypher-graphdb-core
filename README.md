# CypherGraphDB Library

## Overview
CypherGraphDB provides a lightweight, model-aware interface and CLI for working with Cypher-capable graph databases (currently Apache AGE + PostgreSQL and Memgraph). It wraps connection management, query execution, typed node/edge models, path handling, simple full-text search integration (backend dependent), and statistics tracking.

Core ideas:
- Unified Python API for working with cypher-based graph databases across supported backends
- Optional typed models via decorators (@node, @edge, @relation)
- Graph objects are based on Pydantic, so full Pydantic feature set is available
- Additional metadata support via decorators to annotate models (e.g. constrain valid relationships)
- Explicit create / merge / fetch / delete operations
- CLI for interactive exploration
- Tools for exporting and importing graph data in various formats (CSV, Excel, JSON)
- Row factories translating backend driver objects into framework GraphNode / GraphEdge / GraphPath instances which are serialize friendly (e.g. for JSON output)


## Supported Backends
- age (Apache AGE extension on PostgreSQL)
- memgraph (Memgraph database via Bolt)

## Prerequisites
- Python 3.13+
- [uv](https://github.com/astral-sh/uv) (dependency & venv manager)
- [Task](https://taskfile.dev) (automation) – install on macOS: `brew install go-task`
- Docker (for running integration tests with Memgraph)

## Quick Start
```bash
git clone <repo-url>
cd cypher-graphdb/lib
# create venv + install library
task install
# run unit tests
task test:unit
# run CLI
task run:cli
```

Activate environment manually if needed:
```bash
source .venv/bin/activate
```

## Backend Configuration

### Apache AGE Backend (backend="age")

Apache AGE runs as an extension on PostgreSQL, so connection strings use PostgreSQL format.

**Key Parameters:**
- `host`: PostgreSQL server hostname (default: localhost)
- `port`: PostgreSQL server port (default: 5432)  
- `dbname`: Database name (required)
- `user`: Username for authentication
- `password`: Password for authentication
- `graph_name`: AGE graph name to use/create

**CLI Usage:**
```bash
# Using CINFO parameter
cypher-graphdb --backend age --cinfo "host=localhost port=5432 dbname=postgres"

# Using environment variable
export CGDB_CINFO="postgresql://postgres:secret@localhost:5432/postgres"
cypher-graphdb --backend age

# Interactive CLI
> connect age host=localhost port=5432 dbname=postgres user=postgres
```

### Memgraph Backend (backend="memgraph")

Memgraph uses the Bolt protocol for connections.

**Key Parameters:**
- `host`: Memgraph server hostname (default: 127.0.0.1)
- `port`: Memgraph Bolt port (default: 7687)
- `username`: Username for authentication (default: empty)
- `password`: Password for authentication (default: empty)

**Supported CINFO Formats:**
- Key=value: `"host=localhost port=7687 username=user password=secret"`
- Bolt URI: `"bolt://[username:password@]hostname:port"`
- Partial: `"port=1234"` or `"host=192.168.1.100"`

**CLI Usage:**
```bash
# Using CINFO parameter
cypher-graphdb --backend memgraph --cinfo "host=localhost port=7687"

# Using Bolt URI
cypher-graphdb --backend memgraph --cinfo "bolt://localhost:7687"

# Using environment variable  
export CGDB_CINFO="bolt://user:pass@localhost:7687"
cypher-graphdb --backend memgraph

# Interactive CLI
> connect memgraph host=localhost port=7687
```

## Using the Library

### Connection Methods

CypherGraphDB supports multiple connection methods. The recommended approach is using `connect_url` for simplicity:

```python
from cypher_graphdb import CypherGraphDB

# Memgraph - Recommended connection method
db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

# Memgraph - With authentication
db = CypherGraphDB(backend="memgraph", connect_url="bolt://user:pass@localhost:7687")

# Apache AGE - PostgreSQL URL
db = CypherGraphDB(backend="age", connect_url="postgresql://postgres:secret@localhost:5432/postgres")

# All alternative connection methods (still supported):
# 1. Manual connect: db = CypherGraphDB(backend="memgraph"); db.connect("bolt://localhost:7687")
# 2. CINFO parameter: db.connect(cinfo="bolt://localhost:7687") 
# 3. Individual params: db.connect(host="localhost", port=7687)
# 4. Connect params: CypherGraphDB(backend="memgraph", connect_params={"cinfo": "bolt://..."})
# 5. Environment var: export CGDB_CINFO="bolt://localhost:7687"; db.connect()
```

### Basic Connection (Context Manager)
```python
from cypher_graphdb import CypherGraphDB

# Memgraph with context manager
with CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687") as db:
   result = db.execute("RETURN 1 AS value", unnest_result=True)
   print(result)  # 1

# Apache AGE with context manager
with CypherGraphDB(backend="age", connect_url="postgresql://postgres:secret@localhost:5432/postgres") as db:
   result = db.execute("RETURN 1 AS value", unnest_result=True) 
   print(result)  # 1
```

### Simple Query Example
```python
from cypher_graphdb import CypherGraphDB

# Connect and execute queries
db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")
rows = db.execute("MATCH (n) RETURN n LIMIT 5")
for row in rows:
    print(row)
```

### Create / Merge Nodes & Edges untyped
```python
from cypher_graphdb import CypherGraphDB, GraphNode, GraphEdge

# Using direct connection
db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

person = GraphNode(label_="Person", properties_={"name": "Alice", "age": 30})
db.create_or_merge(person)  # assigns id_

company = GraphNode(label_="Company", properties_={"name": "TechCorp"})
db.create_or_merge(company)

rel = GraphEdge.build(person, company, label_="WORKS_FOR", properties_={"since": 2020})
db.create_or_merge(rel)

fetched = db.execute("""
MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) RETURN p, r, c
""")
for (p, r, c) in fetched:
    print(p.label_, p.properties_["name"], r.label_, c.properties_["name"])
```

## CLI (Interactive Graph Exploration)

CypherGraphDB includes an interactive CLI for exploring graph databases:

### Running the CLI
```bash
# Using task automation
task run:cli

# Direct execution (with activated venv)
cypher-graphdb

# Without activation
.venv/bin/cypher-graphdb
```

### Key CLI Commands
```bash
# Connect to databases
> connect memgraph bolt://localhost:7687
> connect age postgresql://postgres:secret@localhost:5432/postgres

# Execute Cypher queries
> MATCH (n) RETURN n LIMIT 5;
> CREATE (p:Person {name: 'Alice', age: 30});

# CLI utilities
> :help               # Show available commands  
> :stats              # Show last query statistics
> :exit               # Exit CLI
```

### CLI Features
- **Interactive Cypher execution** with syntax highlighting
- **Connection management** for multiple backends
- **Query result formatting** with tabular display
- **Command history** and auto-completion
- **Built-in help system** and error handling

## Advanced Usage

### Typed Models via Decorators
```python
from cypher_graphdb import GraphNode, GraphEdge, node, edge, relation

@node(metadata={"color": "blue"})
@relation(rel_type="BELONGS_TO", to_type="Category")
class Product(GraphNode):
    product_key: str
    product_family: str | None = None

@node(label="Category")
class Category(GraphNode):
    category_key: str
    name: str
    description: str | None = None

@edge(label="BELONGS_TO")
class BelongsTo(GraphEdge):
    val: str | None = None
```

### Result Unnesting
`unnest_result` simplifies common patterns:
```python
value = db.execute("RETURN 1 AS x", unnest_result=True)  # returns 1 (not [(1,)])
row = db.execute("RETURN 1 AS x, 2 AS y", fetch_one=True, unnest_result=True)  # (1, 2)
```

### Bulk Data Setup
```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph", connect_url="bolt://localhost:7687")

db.execute("""
    CREATE (p1:Person {name: 'Alice', age: 30, email: 'alice@example.com'})
    CREATE (p2:Person {name: 'Bob', age: 25, email: 'bob@example.com'})
    CREATE (c:Company {name: 'TechCorp', founded: 2010})
    CREATE (p1)-[:WORKS_FOR {since: 2020, role: 'Engineer'}]->(c)
    CREATE (p2)-[:WORKS_FOR {since: 2021, role: 'Designer'}]->(c)
    CREATE (p1)-[:KNOWS {since: 2019}]->(p2)
""")
```

## Project Structure (Simplified)
```
cypher-graphdb/
├── src/cypher_graphdb/
│   ├── backends/
│   │   ├── age/              # AGE row factories & backend
│   │   └── memgraph/         # Memgraph backend + row factories
│   ├── cli/                  # CLI application
│   ├── models.py             # GraphNode, GraphEdge, GraphPath, Graph
│   ├── modelprovider.py      # Dynamic typed model registry
│   ├── cyphergraphdb.py      # Core CypherGraphDB class
│   ├── cypherbuilder.py      # Cypher generation helpers
│   ├── decorators.py         # @node/@edge/@relation
│   └── graphops.py           # Utility graph operations
├── tests/
│   ├── unit/
│   └── integration/          # Uses Memgraph test container
├── examples/                 # Example scripts
├── docs/                     # MkDocs site
├── Taskfile.yml              # Automation tasks
└── README.md
```

## Development

### Key Tasks
```bash
task install              # Install library with dependencies
task test:unit            # Run unit tests 
task test:integration     # Integration tests (requires Docker)
task format               # Format code with Ruff
task check                # Lint and check code quality
task run:cli              # Run interactive CLI
```

## Deployment
Tag-based CI/CD pipeline publishes releases to Artifactory. See `Taskfile.yml` for development commands.

Example tag:
```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

## Example Query Patterns
```python
# Unnesting single scalar
val = db.execute("RETURN 42 AS answer", unnest_result=True)  # 42

# Fetch one row
row = db.execute("RETURN 1 AS a, 2 AS b", fetch_one=True, unnest_result=True)  # (1, 2)

# Multiple rows preserved
rows = db.execute("UNWIND range(1,3) AS x RETURN x")
for (x,) in rows:
    print(x)
```

## License
See `LICENSE.md` (MIT).

## Contributing
See `CONTRIBUTING.md` for guidelines.

---
Happy graph hacking!

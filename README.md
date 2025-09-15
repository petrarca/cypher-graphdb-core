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
cd cypher-graphdb
# create venv + install extras
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

## Using the Library
### Basic Connection (Context Manager)
```python
from cypher_graphdb import CypherGraphDB

# Connect to Memgraph (test/integration style)
connect_params = {"host": "localhost", "port": 7687}
with CypherGraphDB(backend="memgraph", connect_params=connect_params) as db:

   result = db.execute("RETURN 1 AS value", unnest_result=True)
   print(result)  # 1
```

### Manual Connect & Simple Query
```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph")
db.connect(host="localhost", port=7687)
rows = db.execute("MATCH (n) RETURN n LIMIT 5")
for row in rows:
    # each row is a tuple of GraphObjects / primitives
    print(row)
```

### Create / Merge Nodes & Edges untyped
```python
from cypher_graphdb import CypherGraphDB, GraphNode, GraphEdge

db = CypherGraphDB(backend="memgraph", connect_params={"host": "localhost", "port": 7687})

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

### Create / Merge with Typed Models
```python
# sample_model.py (ensure it's imported before using typed resolution)
from cypher_graphdb import CypherGraphDB
from examples.sample_model import Product, Category, BelongsTo  # adjust path as needed

# Connect (Memgraph example)
db = CypherGraphDB(backend="memgraph", connect_params={"host": "localhost", "port": 7687})

# Create typed nodes
p = Product(product_key="P-001", product_family="Widgets", name="Widget Pro")  # extra props resolved
c = Category(category_key="C-TOOLS", name="Tools")

db.create_or_merge(p)
db.create_or_merge(c)

# Create a typed edge via build helper (accepts instances or IDs)
rel = BelongsTo.build(p, c, val="primary")
db.create_or_merge(rel)

# Fetch back using criteria
from cypher_graphdb import MatchNodeCriteria, MatchEdgeCriteria
fetched_product = db.fetch_nodes({"label_": "Product", "product_key": "P-001"}, unnest_result=True)
print(fetched_product.product_family, fetched_product.properties_["product_key"])  # Widget info

edges = db.fetch(MatchEdgeCriteria(label_="BELONGS_TO", fetch_nodes_=True))
for (edge,) in edges:
    print(edge.label_, edge.val, edge.start_id_, edge.end_id_)
```
Key points:
- Decorators register classes at import time through the global ModelProvider
- Typed fields become first-class attributes (e.g. `product_family`) while remaining in `properties_` when flattened
- `build` resolves node IDs automatically when passing instances

### Result Unnesting
`unnest_result` simplifies common patterns:
```python
value = db.execute("RETURN 1 AS x", unnest_result=True)  # returns 1 (not [(1,)])
row = db.execute("RETURN 1 AS x, 2 AS y", fetch_one=True, unnest_result=True)  # (1, 2)
```

### Integration-Test Style Data Setup
From `tests/integration/test_example.py`:
```python
memgraph_db.execute("""
    CREATE (p1:Person {name: 'Alice', age: 30, email: 'alice@example.com'})
    CREATE (p2:Person {name: 'Bob', age: 25, email: 'bob@example.com'})
    CREATE (c:Company {name: 'TechCorp', founded: 2010})
    CREATE (p1)-[:WORKS_FOR {since: 2020, role: 'Engineer'}]->(c)
    CREATE (p2)-[:WORKS_FOR {since: 2021, role: 'Designer'}]->(c)
    CREATE (p1)-[:KNOWS {since: 2019}]->(p2)
""")
```

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

Registering happens automatically on import; instances are created through queries or via `ModelProvider` implicitly used in row factories.

## CLI Usage
Run interactive CLI:
```bash
task run:cli
```
Inside CLI:
```
> connect memgraph host=localhost port=7687
> MATCH (n) RETURN n LIMIT 3;
> :stats            # (if implemented) show last execution stats
> :help
```

Direct binary (activated venv):
```bash
cypher-graphdb
```
Or without activation:
```bash
.venv/bin/cypher-graphdb
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

## Task Automation Reference
```bash
# Environment & install
task setup                # Create venv (.venv)
task install              # Install (editable) with extras

# Formatting & linting
task format               # Ruff format
task check                # Ruff lint + docstring checks
task check:fix            # Auto-fix

# Testing
task test:unit            # Unit tests
task test:integration     # Integration tests (requires Docker)
task test:cov             # Unit tests w/ coverage (HTML + XML)
task test:all             # All tests

# Build & docs
task build                # Build wheel & sdist
task build:docs           # mkdocs build
task build:all            # build + docs

# Docs live server
task serve:docs           # mkdocs serve (implied)

# Composite
task fct                  # format + check + test

# Cleanup
task clean                # build artifacts
task clean:docs           # site/ docs build
task clean:venv           # remove .venv
task clean:caches         # py & ruff caches
task clean:all            # everything

# Pre-commit hooks
task pre-commit:setup
task pre-commit:run

# CLI
task run:cli              # Run interactive CLI
```

## Testing Strategy
- Unit tests focus on query building, models, and utilities
- Integration tests spin up Memgraph via `testcontainers` to validate backend execution & row factories
- Coverage report is generated into `docs/coverage` via `task test:cov`

## Deployment (Summary)
Tag-based release pipeline (GitLab CI expected) builds & publishes to Artifactory. Ensure the following CI variables exist:
- `ARTIFACTORY_USER`
- `ARTIFACTORY_PASSWORD`

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

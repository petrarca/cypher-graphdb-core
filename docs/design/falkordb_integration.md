# Analysis: FalkorDB Integration

This document outlines the technical design for integrating FalkorDB as a supported backend in the `cypher-graphdb` library.

## 1. Library Dependencies

*   **Package**: `falkordb` (PyPI: `FalkorDB`)
*   **Version**: Latest stable.
*   **License**: BSD-3-Clause (Client is open source).

## 2. Backend Implementation

We will create a new backend module: `lib/src/cypher_graphdb/backends/falkordb/falkordb.py`.

### Class Structure

```python
from cypher_graphdb.backend import CypherBackend
from falkordb import FalkorDB

class FalkorDBBackend(CypherBackend):
    name = "FALKORDB"

    def connect(self, cinfo: str | None = None, graph_name: str = "falkor", **kwargs):
        # Parse connection info
        # Connect using FalkorDB client
        self._client = FalkorDB(host=host, port=port, password=password)
        self._driver = self._client.select_graph(graph_name)
```

### Multi-Graph Support

FalkorDB supports multiple graphs natively.

*   **`create_graph(name)`**: FalkorDB creates graphs lazily on the first query, or we can use `select_graph(name)`.
*   **`delete_graph(name)`**: `self._client.select_graph(name).delete()`
*   **`graph_exists(name)`**: Check keys in Redis matching the graph name.

### Query Execution

FalkorDB's `query` method returns a `ResultSet`. We need to map this to `TabularResult`.

```python
def execute_cypher(self, query, ...):
    result_set = self._driver.query(query.parsed_query, query.parameters)
    # Convert result_set to TabularResult
    # Map Node/Edge objects to GraphObject
```

### Type Mapping

We need a `FalkorDBRowFactory` to convert FalkorDB types to `cypher-graphdb` types.

*   **Nodes**: FalkorDB Node -> `GraphObject(type=NODE)`
*   **Edges**: FalkorDB Edge -> `GraphObject(type=EDGE)`
*   **Scalars**: Direct mapping (int, float, string, bool).

## 3. Integration Steps

1.  **Add Dependency**: Add `falkordb` to `pyproject.toml`.
2.  **Create Backend**: Implement `FalkorDBBackend` class.
3.  **Implement Row Factory**: Handle type conversion.
4.  **Register Backend**: Add to `cypher_graphdb.backends` registry.
5.  **Tests**: Add integration tests using `FalkorDBLite` or a Docker container.

## 4. Access from Python

Once integrated, usage would be:

```python
from cypher_graphdb import init_graphdb

# Initialize with FalkorDB backend
init_graphdb(
    backend="FALKORDB",
    cinfo="redis://localhost:6379",
    graph="my_app_graph"
)

from cypher_graphdb import get_graph

with get_graph() as db:
    # This is now a FalkorDBBackend instance
    db.execute_cypher("MATCH (n) RETURN n")
```

## 5. Feasibility

The integration is **highly feasible**. The `falkordb` python client is mature and follows a similar pattern to `neo4j` and `mgclient`. The mapping of types will be the main implementation effort.

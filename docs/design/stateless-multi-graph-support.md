# Analysis: Stateless Multi-Graph Support

This document analyzes the current stateful graph handling in `cypher-graphdb` and proposes a stateless design.

## 1. Current Statefulness

Currently, the `CypherBackend` class holds the `_graph_name` state:

```python
class CypherBackend(abc.ABC):
    def __init__(self, _, **kwargs):
        self._graph_name = kwargs.get("graph_name")

    @property
    def graph_name(self) -> str:
        return self._graph_name
```

This means a `CypherGraphDB` instance is tied to a specific graph. To switch graphs, you must call `change_graph_name()`, which mutates the state of the connection. This is problematic for concurrent usage in a multi-tenant environment (e.g., a shared connection pool).

## 2. Proposed Stateless Design

We should allow passing `graph` (or `graph_name`) as an argument to execution methods. The backend should use this argument for the specific operation without mutating its internal state.

### API Changes

#### `CypherGraphDB`

Update execution methods to accept an optional `graph` argument.

```python
class CypherGraphDB:
    def execute(self, query, parameters=None, graph: str = None, ...):
        # Pass graph to backend
        return self._backend.execute_cypher(parsed_query, graph=graph, ...)

    def fetch_nodes(self, criteria, graph: str = None, ...):
        # Pass graph to backend
        ...
```

#### `CypherBackend`

Update the abstract methods to accept `graph`.

```python
class CypherBackend(abc.ABC):
    @abc.abstractmethod
    def execute_cypher(self, query, graph: str = None, ...):
        pass
```

### Backend Implementation (FalkorDB Example)

For FalkorDB, which supports stateless graph selection per query (conceptually), we can implement it like this:

```python
class FalkorDBBackend(CypherBackend):
    def execute_cypher(self, query, graph: str = None, ...):
        target_graph = graph or self._graph_name
        # Select graph for this execution only
        driver = self._client.select_graph(target_graph)
        return driver.query(query)
```

**Note**: For FalkorDB, the `select_graph` call is lightweight (it just creates a proxy object), so this is efficient.

### Backend Implementation (Memgraph Example)

For Memgraph (Enterprise), which supports `USE DATABASE`, we can inject the `USE` clause or switch the session.

```python
class MemgraphDB(CypherBackend):
    def execute_cypher(self, query, graph: str = None, ...):
        if graph and graph != "memgraph":
             # If Memgraph supports USE clause (Enterprise)
             query = f"USE {graph} {query}"
        ...
```

## 3. Benefits

1.  **Thread Safety**: A single `CypherGraphDB` instance (or pool) can handle requests for multiple apps concurrently without race conditions on `change_graph_name`.
2.  **Simplicity**: The API becomes explicit: `db.execute(..., graph="app1")`.
3.  **Flexibility**: Easy to support the "Graph per App" strategy.

## 4. Migration Path

1.  Keep `_graph_name` as a default.
2.  Add `graph` argument to all `execute*` and `fetch*` methods.
3.  Update backends to respect the argument.

## 5. Impact on High-Level Methods

To fully support stateless operations, the `graph` argument must be propagated through all high-level methods in `CypherGraphDB`.

### Method Chain Updates

The following methods need to accept `graph: str = None` and pass it down:

1.  **`execute(..., graph=None)`** -> calls `_parse_and_execute(..., graph=graph)`
2.  **`fetch(..., graph=None)`** -> calls `fetch_nodes` or `fetch_edges`
3.  **`fetch_nodes(..., graph=None)`** -> calls `_fetch_node_by_criteria` -> `_parse_and_execute`
4.  **`fetch_edges(..., graph=None)`** -> calls `_fetch_edge_by_criteria` -> `_parse_and_execute`
5.  **`create_or_merge(..., graph=None)`** -> calls internal create/merge methods -> `_parse_and_execute`
6.  **`delete(..., graph=None)`** -> calls internal delete methods -> `_parse_and_execute`

### Internal Helper Updates

The `_parse_and_execute` method is the central bottleneck. It must be updated:

```python
def _parse_and_execute(
    self,
    cypher_cmd: str | ParsedCypherQuery,
    fetch_one: bool = False,
    raw_data: bool = False,
    graph: str = None,  # New argument
) -> TabularResult | None:
    # ...
    result, self._exec_statistics = self._backend.execute_cypher(
        parsed_query, 
        fetch_one=fetch_one, 
        raw_data=raw_data, 
        graph=graph  # Pass to backend
    )
    # ...
```

This ensures that *any* operation can be targeted to a specific graph without changing the global connection state.

## 6. Driver Compatibility Verification

I have verified that this design is compatible with the underlying drivers/databases:

*   **FalkorDB**: The Python client supports `client.select_graph(name)` which returns a lightweight driver object for that graph. This is perfectly aligned with the design.
*   **Memgraph (Enterprise)**: Supports the `USE DATABASE` Cypher clause (e.g., `USE DATABASE my_app; MATCH ...`). We can inject this clause in the `MemgraphDB` backend when a `graph` argument is present.
*   **Apache AGE**: The `cypher()` function in PostgreSQL takes the graph name as its first argument: `cypher('my_graph', $$ MATCH ... $$)`. The `AgeDB` backend can simply pass the `graph` argument to this function call.

**Conclusion**: The stateless design is technically feasible and idiomatic for all three supported backends.

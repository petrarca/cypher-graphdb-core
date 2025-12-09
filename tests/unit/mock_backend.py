"""Reusable in-memory mock graph backend & DB for tests (no global monkeypatch).

Provides:
- MockBackend: minimal `CypherBackend` subclass storing nodes/edges in-memory.
- MockGraphDB: `CypherGraphDB` subclass implementing `create_or_merge` & `fetch_nodes`
    directly, delegating to its `MockBackend` instance (isolation per test instance).
- build_db(): convenience factory returning a fresh `MockGraphDB`.

This replaces the earlier implementation that monkeypatched `CypherGraphDB` at the
class level (global side-effects) which risked interference across tests when run
in parallel. Each test now gets its own isolated object graph.
"""

from __future__ import annotations

from cypher_graphdb.backend import CypherBackend
from cypher_graphdb.cyphergraphdb import CypherGraphDB
from cypher_graphdb.models import GraphEdge, GraphNode


class MockBackend(CypherBackend):
    def __init__(self):  # noqa: D401
        super().__init__(None, autocommit=True)
        self._id = "mock"
        self.graph_name = "test"
        self.nodes: dict[int, GraphNode] = {}
        self.edges: dict[int, GraphEdge] = {}
        self.next_node_id = 1
        self.next_edge_id = 1000
        self._commits = 0

    # ---- abstract method implementations (no-op/trivial) ----
    def connect(self, *a, **k):  # noqa: D401
        return None

    def disconnect(self):  # noqa: D401
        return None

    def create_graph(self, graph_name=None):  # noqa: D401
        return None

    def drop_graph(self, graph_name=None):  # noqa: D401
        return None

    def graph_exists(self, graph_name: str = None) -> bool:  # noqa: D401
        return True

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False):  # noqa: D401
        return [], None

    def fulltext_search(self, cypher_query, fts_query, language=None):  # noqa: D401
        return [], None

    def labels(self):  # noqa: D401
        return []

    def graphs(self):  # noqa: D401
        return []

    def commit(self):  # noqa: D401
        self._commits += 1

    def rollback(self):  # noqa: D401
        return None

    def get_capability(self, capability: str) -> str:
        """Mock backend with specific capabilities."""
        if capability == "SINGLE_LABEL":
            return "false"  # Mock backend supports multiple labels
        elif capability == "SUPPORT_MULTIPLE_LABELS":
            return "true"  # Mock backend supports multiple labels
        else:
            raise ValueError(f"Unknown capability: {capability}")

    # ---- helpers used by monkeypatched CypherGraphDB methods ----
    def create_or_merge_node(self, node: GraphNode):  # type: ignore
        if node.id_ is None:
            node.id_ = self.next_node_id
            self.next_node_id += 1
        self.nodes[node.id_] = node
        return node

    def create_or_merge_edge(self, edge: GraphEdge):  # type: ignore
        if edge.id_ is None:
            edge.id_ = self.next_edge_id
            self.next_edge_id += 1
        self.edges[edge.id_] = edge
        return edge


class MockGraphDB(CypherGraphDB):
    """Graph DB wired to a `MockBackend` with isolated graph ops.

    Implements the small subset of "graph ops" needed by importer/exporter tests:
    - create_or_merge(GraphNode|GraphEdge)
    - fetch_nodes(criteria, unnest_result=None, fetch_one=None)
    """

    def __init__(self, backend: MockBackend | None = None):  # noqa: D401
        super().__init__(backend or MockBackend())

    # --- public API used by tests / importers / exporters ---
    def create_or_merge(self, obj):  # type: ignore  # noqa: D401
        if isinstance(obj, GraphNode):
            return self._backend.create_or_merge_node(obj)  # type: ignore[attr-defined]
        if isinstance(obj, GraphEdge):
            return self._backend.create_or_merge_edge(obj)  # type: ignore[attr-defined]
        raise RuntimeError("Unsupported graph object type for create_or_merge")

    def fetch_nodes(self, criteria, unnest_result=None, fetch_one=None):  # type: ignore  # noqa: D401
        proj = criteria.projection_ or []
        gid_filter = None
        if criteria.properties_ and "gid_" in criteria.properties_:
            gid_filter = criteria.properties_["gid_"]
        id_filter = getattr(criteria, "id_", None)

        results = []
        for node in self._backend.nodes.values():  # type: ignore[attr-defined]
            if id_filter is not None and node.id_ != id_filter:
                continue
            if gid_filter is not None and node.properties_.get("gid_") != gid_filter:
                continue
            if proj == ["id(n)"]:
                results.append(node.id_)
            elif proj == ["gid_", "labels(n)[0]"] or proj == ["gid_", "label(n)"]:
                results.append((node.properties_.get("gid_"), node.label_))
            else:
                results.append(node)
        if unnest_result:
            if len(results) == 1:
                return results[0]
            if len(results) == 0:
                return None
        return results

    # --- Test helper methods for accessing mock state ---
    def get_nodes(self) -> list[GraphNode]:
        """Return all nodes in the mock backend."""
        return list(self._backend.nodes.values())  # type: ignore[attr-defined]

    def get_edges(self) -> list[GraphEdge]:
        """Return all edges in the mock backend."""
        return list(self._backend.edges.values())  # type: ignore[attr-defined]

    def node_count(self) -> int:
        """Return the number of nodes in the mock backend."""
        return len(self._backend.nodes)  # type: ignore[attr-defined]

    def edge_count(self) -> int:
        """Return the number of edges in the mock backend."""
        return len(self._backend.edges)  # type: ignore[attr-defined]


def build_db() -> MockGraphDB:
    """Return a fresh isolated `MockGraphDB` instance for a test."""
    return MockGraphDB()


__all__ = ["MockBackend", "MockGraphDB", "build_db"]

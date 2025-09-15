"""graphops module: Graph analysis and traversal utilities.

Provides functions for graph metrics, node/edge filtering, tree building,
and topological analysis of Graph objects.
"""

from . import config
from .models import Graph, GraphEdge, GraphNode


def is_empty(graph: Graph) -> bool:
    """Return True if the graph contains no nodes."""
    return len(graph.nodes) == 0


def density(graph: Graph) -> float:
    """Calculate graph density as edges / (nodes * (nodes - 1))."""
    m = len(graph.edges)
    n = len(graph.nodes)

    return m / (n * (n - 1))


def edges(graph: Graph) -> tuple[GraphEdge]:
    """Return all edges in the graph as a tuple."""
    return tuple(graph.edges)


def get_edge(graph: Graph, edge_ref: str | int) -> GraphEdge | None:
    """Get an edge by reference (ID or GID), returning None if not found."""
    result = graph[edge_ref]

    return result if isinstance(result, GraphEdge) else None


def edges_between_nodes(graph: Graph, start_ref: int, end_ref: int) -> tuple[GraphEdge]:
    """Find all edges connecting two specific nodes."""
    result = []
    for edge in graph.edges:
        if edge.start_id_ == start_ref and edge.end_id_ == end_ref:
            result.append(edge)

    return tuple(result)


def nodes(graph: Graph) -> tuple[GraphNode]:
    """Return all nodes in the graph as a tuple."""
    return tuple(graph.nodes)


def get_node(graph: Graph, node_ref: int | str) -> GraphNode | None:
    """Get a node by reference (ID or GID), returning None if not found."""
    result = graph[node_ref]

    return result if isinstance(result, GraphNode) else None


def root_nodes(graph: Graph, direction: str = "incoming", with_unbound_nodes: bool = False) -> tuple[GraphNode]:
    """Find root nodes based on edge direction.

    Args:
        graph: The graph to analyze.
        direction: 'incoming' or 'outgoing' edge direction.
        with_unbound_nodes: Whether to include nodes with no edges.

    Returns:
        Tuple of nodes that are roots in the specified direction.

    """
    assert direction in ("incoming", "outgoing")

    # incoming direction, node must not have any outgoing edges
    proc1 = outgoing_nodes if direction == "incoming" else incoming_nodes
    proc2 = incoming_nodes if proc1 == outgoing_nodes else outgoing_nodes

    result = []

    for node in graph.nodes:
        r1 = proc1(graph, node)
        r2 = proc2(graph, node)

        if not r1 and not r2:
            if with_unbound_nodes:
                result.append(node)
            continue

        if not r1:
            result.append(node)

    return tuple(result)


def unbound_nodes(graph: Graph) -> tuple[GraphNode]:
    """Return nodes that are not connected to any edges."""
    all_nodes = set(graph.nodes)
    ref_nodes = set()

    for edge in graph.edges:
        ref_nodes.add(graph[edge.start_id_])
        ref_nodes.add(graph[edge.end_id_])

    return all_nodes - ref_nodes


def missing_nodes(graph: Graph) -> set[int]:
    """Find node IDs referenced by edges but missing from the graph."""
    result = set()

    for edge in graph.edges:
        if graph[edge.start_id_] is None:
            result.add(edge.start_id_)
        if edge.end_id_ != edge.start_id_ and graph[edge.end_id_] is None:
            result.add(edge.end_id_)

    return result


def self_referenced_nodes(graph: Graph, label: str = None) -> set[GraphNode]:
    """Find nodes with self-referencing edges, optionally filtered by edge label."""
    result = set()

    for edge in graph.edges:
        if edge.start_id_ == edge.end_id_ and (not label or edge.label_ == label):
            result.add(graph[edge.start_id_])

    return result


def incoming_nodes(graph: Graph, node: GraphNode, label: str = None) -> tuple[tuple[GraphEdge, GraphNode]]:
    """Find nodes connected to the given node via incoming edges.

    Args:
        graph: The graph to search.
        node: Target node to find incoming connections for.
        label: Optional edge label filter.

    Returns:
        Tuple of (edge, source_node) pairs for incoming connections.

    """
    result = []
    for edge in graph.edges:
        # skip over self references
        if edge.start_id_ == edge.end_id_:
            continue

        if edge.end_id_ == node.id_ and (not label or edge.label_ == label):
            # TODO: remove C409 result.append(tuple((edge, graph[edge.start_id_])))
            result.append((edge, graph[edge.start_id_]))

    return tuple(result)


def outgoing_nodes(graph: Graph, node: GraphNode, label: str = None) -> tuple[tuple[GraphEdge, GraphNode]]:
    """Find nodes connected to the given node via outgoing edges.

    Args:
        graph: The graph to search.
        node: Source node to find outgoing connections for.
        label: Optional edge label filter.

    Returns:
        Tuple of (edge, target_node) pairs for outgoing connections.

    """
    result = []
    for edge in graph.edges:
        # skip over self references
        if edge.start_id_ == edge.end_id_:
            continue

        if edge.start_id_ == node.id_ and (not label or edge.label_ == label):
            result.append((edge, graph[edge.end_id_]))

    return tuple(result)


# TODO: Type annotation fo result (recursive tuples)
def create_tree(
    graph: Graph,
    roots: GraphNode | tuple[GraphNode],
    direction: str = config.DEFAULT_TREE_DIRECTION,
    label: str = None,
    max_depth: int = -1,
):
    """Create a tree structure from graph starting at specified root nodes.

    Args:
        graph: The graph to traverse.
        roots: Root node(s) to start tree construction.
        direction: 'incoming' or 'outgoing' traversal direction.
        label: Optional edge label filter.
        max_depth: Maximum tree depth (-1 for unlimited).

    Returns:
        Nested tuple structure representing the tree.

    """

    def get_children(parent: GraphNode, depth: int):
        if max_depth != -1 and depth > max_depth:
            # Depth limit reached: no further expansion
            return ()

        childs = []
        if direction == "incoming":
            children = incoming_nodes(graph, parent, label=label)
        else:
            children = outgoing_nodes(graph, parent, label=label)

        for edge, child_node in children:
            collected_childs = get_children(child_node, depth + 1)
            childs.append([child_node, edge, collected_childs])

        return tuple(childs)

    roots = [roots] if not isinstance(roots, list | tuple) else roots

    result = []
    level = 0

    for node in roots:
        result.append([node, None, get_children(node, level + 1)])

    return tuple(result)


# TODO Type annotation for result, take from create_tree
def build_tree(
    graph: Graph,
    direction: str = config.DEFAULT_TREE_DIRECTION,
    with_unbound_nodes: bool = True,
):
    """Build a complete tree from graph root nodes.

    Args:
        graph: The graph to analyze.
        direction: 'incoming' or 'outgoing' traversal direction.
        with_unbound_nodes: Include nodes with no edges as roots.

    Returns:
        Tree structure starting from all identified root nodes.

    """
    roots = root_nodes(graph, direction=direction, with_unbound_nodes=with_unbound_nodes)

    result = create_tree(graph, roots) if roots else ()

    return result

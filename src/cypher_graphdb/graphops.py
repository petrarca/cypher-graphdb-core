"""graphops module: Graph analysis and traversal utilities.

Provides functions for graph metrics, node/edge filtering, tree building,
and topological analysis of Graph objects.
"""

from collections import defaultdict
from dataclasses import dataclass

from . import config
from .models import Graph, GraphEdge, GraphNode, TreeResult


@dataclass
class TreeAnalysis:
    """Result of tree structure analysis."""

    is_tree: bool
    reason: str | None = None
    direction: str | None = None
    root_nodes: tuple[GraphNode, ...] = ()
    dominant_pattern: str | None = None


@dataclass
class DirectionAnalysis:
    """Analysis of edge direction patterns."""

    pattern: str  # "parent_to_child", "child_to_parent", "mixed"
    dominant_direction: str  # "outgoing", "incoming"
    roots_outgoing: tuple[int, ...] = ()
    roots_incoming: tuple[int, ...] = ()


def is_empty(graph: Graph) -> bool:
    """Return True if the graph contains no nodes."""
    return len(graph.nodes) == 0


def density(graph: Graph) -> float:
    """Calculate graph density as edges / (nodes * (nodes - 1))."""
    m = len(graph.edges)
    n = len(graph.nodes)

    return m / (n * (n - 1))


def get_edges(graph: Graph) -> tuple[GraphEdge, ...]:
    """Return all edges in the graph as a tuple."""
    return tuple(graph.edges.values())


def get_edge(graph: Graph, edge_ref: str | int) -> GraphEdge | None:
    """Get an edge by reference (ID or GID), returning None if not found."""
    if isinstance(edge_ref, int):
        return graph.edges.get(edge_ref)
    elif isinstance(edge_ref, str):
        # GID lookup - search through edges
        for edge in graph.edges.values():
            if edge.gid_ == edge_ref:
                return edge
        return None
    return None


def get_nodes(graph: Graph) -> tuple[GraphNode, ...]:
    """Return all nodes in the graph as a tuple."""
    return tuple(graph.nodes.values())


def edges_between_nodes(graph: Graph, start_ref: int, end_ref: int) -> tuple[GraphEdge]:
    """Find all edges connecting two specific nodes."""
    result = []
    for edge in graph.edges.values():
        if edge.start_id_ == start_ref and edge.end_id_ == end_ref:
            result.append(edge)

    return tuple(result)


def nodes(graph: Graph) -> tuple[GraphNode, ...]:
    """Return all nodes in the graph as a tuple."""
    return tuple(graph.nodes.values())


def edges(graph: Graph) -> tuple[GraphEdge, ...]:
    """Return all edges in the graph as a tuple."""
    return tuple(graph.edges.values())


def get_node(graph: Graph, node_ref: int | str) -> GraphNode | None:
    """Get a node by reference (ID or GID), returning None if not found."""
    if isinstance(node_ref, int):
        return graph.nodes.get(node_ref)
    elif isinstance(node_ref, str):
        # GID lookup - search through nodes
        for node in graph.nodes.values():
            if node.gid_ == node_ref:
                return node
        return None
    return None


def root_nodes(
    graph: Graph,
    direction: str = "incoming",
    with_unbound_nodes: bool = False,
) -> tuple[GraphNode, ...]:
    """Find root nodes in graph based on edge direction.

    Args:
        graph: The graph to analyze.
        direction: Direction to consider ("incoming" or "outgoing").
        with_unbound_nodes: Include nodes with no connections.

    Returns:
        Tuple of nodes that are roots in the specified direction.

    """
    assert direction in ("incoming", "outgoing")

    # incoming direction, node must not have any outgoing edges
    proc1 = outgoing_nodes if direction == "incoming" else incoming_nodes
    proc2 = incoming_nodes if proc1 == outgoing_nodes else outgoing_nodes

    result = []

    for node in graph.nodes.values():
        r1 = proc1(graph, node)
        r2 = proc2(graph, node)

        if not r1 and not r2:
            if with_unbound_nodes:
                result.append(node)
            continue

        if not r1:
            result.append(node)

    return tuple(result)


def unbound_nodes(graph: Graph) -> tuple[GraphNode, ...]:
    """Return nodes that are not connected to any edges."""
    all_nodes = set(graph.nodes.values())
    ref_nodes = set()

    for edge in graph.edges.values():
        if edge.start_id_ in graph.nodes:
            ref_nodes.add(graph.nodes[edge.start_id_])
        if edge.end_id_ in graph.nodes:
            ref_nodes.add(graph.nodes[edge.end_id_])

    return tuple(all_nodes - ref_nodes)


def missing_nodes(graph: Graph) -> set[int]:
    """Find node IDs referenced by edges but missing from the graph."""
    result = set()

    for edge in graph.edges.values():
        if edge.start_id_ not in graph.nodes:
            result.add(edge.start_id_)
        if edge.end_id_ != edge.start_id_ and edge.end_id_ not in graph.nodes:
            result.add(edge.end_id_)

    return result


def self_referenced_nodes(graph: Graph, label: str | None = None) -> set[GraphNode]:
    """Find nodes with self-referencing edges, optionally filtered by edge label."""
    result = set()

    for edge in graph.edges.values():
        if edge.start_id_ == edge.end_id_ and (not label or edge.label_ == label) and edge.start_id_ in graph.nodes:
            result.add(graph.nodes[edge.start_id_])

    return result


def incoming_nodes(graph: Graph, node: GraphNode, label: str | None = None) -> tuple[tuple[GraphEdge, GraphNode], ...]:
    """Find nodes connected to the given node via incoming edges.

    Args:
        graph: The graph to search.
        node: Target node to find incoming connections for.
        label: Optional edge label filter.

    Returns:
        Tuple of (edge, source_node) pairs for incoming connections.

    """
    result = []
    for edge in graph.edges.values():
        # skip over self references
        if edge.start_id_ == edge.end_id_:
            continue

        if edge.end_id_ == node.id_ and (not label or edge.label_ == label) and edge.start_id_ in graph.nodes:
            result.append((edge, graph.nodes[edge.start_id_]))

    return tuple(result)


def outgoing_nodes(graph: Graph, node: GraphNode, label: str | None = None) -> tuple[tuple[GraphEdge, GraphNode], ...]:
    """Find nodes connected to the given node via outgoing edges.

    Args:
        graph: The graph to search.
        node: Source node to find outgoing connections for.
        label: Optional edge label filter.

    Returns:
        Tuple of (edge, target_node) pairs for outgoing connections.

    """
    result = []
    for edge in graph.edges.values():
        # skip over self references
        if edge.start_id_ == edge.end_id_:
            continue

        if edge.start_id_ == node.id_ and (not label or edge.label_ == label) and edge.end_id_ in graph.nodes:
            result.append((edge, graph.nodes[edge.end_id_]))

    return tuple(result)


# TODO: Type annotation fo result (recursive tuples)
def create_tree(
    graph: Graph,
    roots: GraphNode | tuple[GraphNode, ...],
    direction: str = config.DEFAULT_TREE_DIRECTION,
    label: str | None = None,
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


def build_tree(
    graph: Graph,
    direction: str = config.DEFAULT_TREE_DIRECTION,
    with_unbound_nodes: bool = True,
) -> TreeResult:
    """Build a complete tree from graph root nodes.

    Args:
        graph: The graph to analyze.
        direction: 'incoming' or 'outgoing' traversal direction.
        with_unbound_nodes: Include nodes with no edges as roots.

    Returns:
        TreeResult wrapping the tree structure starting from all identified root nodes.
    """
    roots = root_nodes(graph, direction=direction, with_unbound_nodes=with_unbound_nodes)
    tree_structure = create_tree(graph, roots, direction=direction) if roots else ()
    return TreeResult(tree_structure=tree_structure, direction=direction)


def has_cycles(graph: Graph) -> bool:
    """Check if graph contains cycles using DFS."""
    visited = set()
    rec_stack = set()

    def dfs(node_id: int) -> bool:
        visited.add(node_id)
        rec_stack.add(node_id)

        # Get neighbors based on edge direction
        for edge in graph.edges.values():
            if edge.start_id_ == node_id:
                neighbor_id = edge.end_id_
                if neighbor_id not in visited:
                    if dfs(neighbor_id):
                        return True
                elif neighbor_id in rec_stack:
                    return True

        rec_stack.remove(node_id)
        return False

    return any(node_id not in visited and dfs(node_id) for node_id in graph.nodes)


def find_connected_components(graph: Graph) -> list[set[int]]:
    """Find all connected components in the graph."""
    visited = set()
    components = []

    def dfs(node_id: int, component: set[int]):
        visited.add(node_id)
        component.add(node_id)

        # Check both incoming and outgoing edges for connectivity
        for edge in graph.edges.values():
            if edge.start_id_ == node_id and edge.end_id_ not in visited:
                dfs(edge.end_id_, component)
            elif edge.end_id_ == node_id and edge.start_id_ not in visited:
                dfs(edge.start_id_, component)

    for node_id in graph.nodes:
        if node_id not in visited:
            component = set()
            dfs(node_id, component)
            components.append(component)

    return components


def analyze_edge_directions(graph: Graph) -> DirectionAnalysis:
    """Analyze edge direction patterns in the graph."""
    outgoing_count = defaultdict(int)
    incoming_count = defaultdict(int)

    for edge in graph.edges.values():
        outgoing_count[edge.start_id_] += 1
        incoming_count[edge.end_id_] += 1

    # Find potential roots
    # nodes_with_no_outgoing = leaf nodes (no children)
    # nodes_with_no_incoming = root nodes (no parents)
    nodes_with_no_outgoing = tuple(node_id for node_id in graph.nodes if outgoing_count.get(node_id, 0) == 0)
    nodes_with_no_incoming = tuple(node_id for node_id in graph.nodes if incoming_count.get(node_id, 0) == 0)

    # Simple heuristic: check the majority of edge directions
    # In parent_to_child: most edges point from lower ID to higher ID (typical creation order)
    # In child_to_parent: most edges point from higher ID to lower ID (pointing to parents)

    forward_edges = 0
    reverse_edges = 0

    for edge in graph.edges.values():
        if edge.start_id_ < edge.end_id_:
            forward_edges += 1
        else:
            reverse_edges += 1

    # Use a simple rule: if most edges go forward, it's parent_to_child
    # if most edges go backward, it's child_to_parent
    if forward_edges > reverse_edges:
        return DirectionAnalysis(
            pattern="parent_to_child",
            dominant_direction="outgoing",
            roots_outgoing=nodes_with_no_outgoing,
            roots_incoming=nodes_with_no_incoming,
        )
    elif reverse_edges > forward_edges:
        return DirectionAnalysis(
            pattern="child_to_parent",
            dominant_direction="incoming",
            roots_outgoing=nodes_with_no_outgoing,
            roots_incoming=nodes_with_no_incoming,
        )
    else:
        # Equal or ambiguous - check if we have clear root/leaf pattern
        if len(nodes_with_no_incoming) == 1 and len(nodes_with_no_outgoing) == 1:
            # Single root and single leaf - likely a chain, check direction
            # If the root has outgoing edges, it's parent_to_child
            # If the root has incoming edges, it's child_to_parent
            root_id = nodes_with_no_incoming[0]
            if outgoing_count.get(root_id, 0) > 0:
                return DirectionAnalysis(
                    pattern="parent_to_child",
                    dominant_direction="outgoing",
                    roots_outgoing=nodes_with_no_outgoing,
                    roots_incoming=nodes_with_no_incoming,
                )
            else:
                return DirectionAnalysis(
                    pattern="child_to_parent",
                    dominant_direction="incoming",
                    roots_outgoing=nodes_with_no_outgoing,
                    roots_incoming=nodes_with_no_incoming,
                )

    # Default to mixed for ambiguous cases
    return DirectionAnalysis(
        pattern="mixed",
        dominant_direction="outgoing",  # default to outgoing for mixed
        roots_outgoing=nodes_with_no_outgoing,
        roots_incoming=nodes_with_no_incoming,
    )


def is_tree_like(graph: Graph, direction_analysis: DirectionAnalysis) -> bool:
    """Check if graph follows tree structure (no multiple parents)."""
    if direction_analysis.dominant_direction == "outgoing":
        # Each node should have max 1 incoming edge (single parent)
        incoming_edges = defaultdict(list)
        for edge in graph.edges.values():
            incoming_edges[edge.end_id_].append(edge)

        return all(len(edges) <= 1 for edges in incoming_edges.values())

    else:  # incoming direction
        # Each node should have max 1 outgoing edge (single child in reverse)
        outgoing_edges = defaultdict(list)
        for edge in graph.edges.values():
            outgoing_edges[edge.start_id_].append(edge)

        return all(len(edges) <= 1 for edges in outgoing_edges.values())


def analyze_tree_structure(graph: Graph, skip_detection: bool = False) -> TreeAnalysis:
    """Analyze if graph can be exported as tree structure.

    Args:
        graph: The graph to analyze.
        skip_detection: If True, skip expensive validation and assume tree structure.
    """
    if is_empty(graph):
        return TreeAnalysis(is_tree=False, reason="Empty graph")

    # Analyze direction patterns (needed for both detection and forced export)
    direction_analysis = analyze_edge_directions(graph)

    if skip_detection:
        # Skip expensive validation, assume it's a tree
        # Find root nodes based on dominant direction
        if direction_analysis.dominant_direction == "outgoing":
            root_ids = direction_analysis.roots_outgoing
        else:
            root_ids = direction_analysis.roots_incoming

        root_nodes = tuple(graph.nodes[node_id] for node_id in root_ids if node_id in graph.nodes)

        return TreeAnalysis(
            is_tree=True,
            direction=direction_analysis.dominant_direction,
            root_nodes=root_nodes,
            dominant_pattern=direction_analysis.pattern,
        )

    # Perform expensive validation only when not forced
    # Check for cycles
    if has_cycles(graph):
        return TreeAnalysis(is_tree=False, reason="Contains cycles")

    # Check connectivity
    components = find_connected_components(graph)
    if len(components) > 1:
        return TreeAnalysis(is_tree=False, reason=f"Multiple disconnected components ({len(components)})")

    # Check for tree properties
    if is_tree_like(graph, direction_analysis):
        # Find root nodes based on dominant direction
        if direction_analysis.dominant_direction == "outgoing":
            root_ids = direction_analysis.roots_outgoing
        else:
            root_ids = direction_analysis.roots_incoming

        root_nodes = tuple(graph.nodes[node_id] for node_id in root_ids if node_id in graph.nodes)

        return TreeAnalysis(
            is_tree=True,
            direction=direction_analysis.dominant_direction,
            root_nodes=root_nodes,
            dominant_pattern=direction_analysis.pattern,
        )
    else:
        return TreeAnalysis(is_tree=False, reason="Multiple parents detected (not tree-like)")

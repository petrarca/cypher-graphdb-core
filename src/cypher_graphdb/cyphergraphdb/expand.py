"""Node-expansion mixin for CypherGraphDB.

Provides ``expand_node``: a backend-agnostic, bounded one-hop traversal that
returns a node's neighbors and connecting edges. This is the traversal
primitive graph-exploration UIs need (click a node to load its
neighborhood), factored here so every consumer shares the same
dialect-safe query construction and dedup/cap semantics rather than
re-implementing them.

Deliberately *not* included here: any existence/404 check. ``expand_node``
returns an empty result when the node has no neighbors or does not exist.
Callers that must distinguish "node not found" from "no neighbors" do a
cheap ``fetch_nodes_by_ids([node_id])`` guard first -- that policy is a
transport concern (e.g. mapping to HTTP 404), not a core concern.
"""

from typing import TYPE_CHECKING, Literal

from loguru import logger

from ..cypherbuilder import CypherBuilder
from ..models import ExpandResult, GraphEdge, GraphNode

if TYPE_CHECKING:
    from ._host import CypherGraphDBHost as _Host
else:
    _Host = object

ExpandDirection = Literal["both", "out", "in"]

# Default neighbor cap shared by expand_node. Consumers may override per call.
DEFAULT_EXPAND_LIMIT = 100


class ExpandMixin(_Host):
    """Mixin providing bounded node-neighborhood expansion."""

    def expand_node(
        self,
        node_id: int,
        *,
        direction: ExpandDirection = "both",
        rel_types: list[str] | None = None,
        limit: int = DEFAULT_EXPAND_LIMIT,
    ) -> ExpandResult:
        """Expand a node into its one-hop neighborhood (bounded).

        Returns at most ``limit`` unique neighbor nodes plus the edges that
        connect them to the center, with ``has_more`` signalling that further
        neighbors exist beyond the cap.

        For ``direction="both"`` two directional queries are issued and merged
        rather than a single bidirectional pattern: on Memgraph the latter
        forces a full edge scan. The unique-node cap is applied while
        collecting results (never as a per-query ``LIMIT``), so edges between
        already-accepted nodes are not silently dropped.

        Args:
            node_id: The center node's database id.
            direction: Traversal direction (``"both"``, ``"out"``, ``"in"``).
            rel_types: Edge labels to include (None/empty = all).
            limit: Maximum unique neighbor nodes to return.

        Returns:
            An :class:`ExpandResult` with ``nodes``, ``edges`` and ``has_more``.
        """
        rows = self._run_expand_queries(node_id, direction, rel_types)
        nodes, edges, has_more = self._collect_expand_results(rows, limit)
        return ExpandResult(nodes=nodes, edges=edges, has_more=has_more)

    def _run_expand_queries(
        self,
        node_id: int,
        direction: ExpandDirection,
        rel_types: list[str] | None,
    ) -> list:
        """Execute the directional expansion query/queries and return raw rows.

        ``both`` runs the outgoing and incoming queries and concatenates their
        rows; the dedup step downstream reconciles overlaps.
        """
        directions = ("out", "in") if direction == "both" else (direction,)
        rows: list = []
        for one in directions:
            query, params = CypherBuilder.expand_node(node_id, one, rel_types)
            logger.debug("expand_node query={} params={}", query, params)
            result = self.execute(query, params=params)
            if result:
                rows.extend(result)
        return rows

    @staticmethod
    def _collect_expand_results(rows: list, limit: int) -> tuple[list[GraphNode], list[GraphEdge], bool]:
        """Deduplicate rows into (nodes, edges, has_more), capped at ``limit`` unique nodes.

        Each row is ``(connected_node, edge)``. Once the cap is reached we keep
        scanning (so a later row whose neighbor node was already accepted still
        contributes its edge) but stop admitting new nodes and set ``has_more``.

        An edge is only collected when its neighbor node was accepted: an edge
        to a node dropped by the cap would dangle (reference a node absent from
        ``nodes``), so it is skipped rather than returned.
        """
        nodes: list[GraphNode] = []
        edges: list[GraphEdge] = []
        seen_node_ids: set[int] = set()
        seen_edge_ids: set[int] = set()
        has_more = False

        for row in rows:
            node, edge = row[0], row[1]

            if node.id_ not in seen_node_ids:
                if len(seen_node_ids) >= limit:
                    has_more = True
                    continue
                nodes.append(node)
                seen_node_ids.add(node.id_)

            # Neighbor accepted (this row didn't `continue`); collect its edge.
            if edge.id_ not in seen_edge_ids:
                edges.append(edge)
                seen_edge_ids.add(edge.id_)

        return nodes, edges, has_more

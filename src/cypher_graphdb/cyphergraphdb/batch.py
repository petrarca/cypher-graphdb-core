"""Batch operations mixin for CypherGraphDB."""

from typing import Literal

from .. import config, utils
from ..exceptions import ReadOnlyModeError
from ..models import GraphEdge, GraphNode


class BatchMixin:
    """Mixin providing batch operations for CypherGraphDB."""

    def create_linked_nodes(
        self,
        parent_node: GraphNode,
        linked_pairs: list[tuple[GraphEdge, GraphNode]],
        strategy: Literal["merge", "force_create"] = "force_create",
    ) -> list[tuple[GraphEdge, GraphNode]]:
        """Create node+edge pairs in single batch operation.

        Creates multiple nodes with their relationships to a parent node in a single
        Cypher statement using UNWIND for optimal performance. Each tuple contains
        (edge, node) where the edge connects from the parent_node to the node.

        Args:
            parent_node: Existing parent node to connect from
            linked_pairs: List of (edge, node) tuples to create
            strategy: Creation strategy - "merge" or "force_create"
                - "merge": Update existing objects or create if not found
                - "force_create": Always create new objects, ignore existing ones

        Returns:
            List of (edge, node) tuples with database IDs assigned

        Examples:
            ```python
            # Create license nodes with HAS_LICENSE relationships
            linked_pairs = []
            for license_data in scan_data:
                license = License(
                    license_name=license_data["license_name"],
                    detection_type=license_data["detection_type"],
                    source_file=license_data["source_file"],
                    confidence=license_data["confidence"]
                )
                edge = HasLicense(
                    confidence=license_data["confidence"],
                    detected_at=datetime.now().isoformat()
                )
                linked_pairs.append((edge, license))

            # Single batch creation
            created_pairs = cdb.create_linked_nodes(
                parent_node=component,
                linked_pairs=linked_pairs,
                strategy="force_create"
            )
            ```

        Raises:
            ReadOnlyModeError: If the connection is in read-only mode
            ValueError: If linked_pairs is empty or contains invalid data
        """
        assert self._backend

        if self.read_only:
            raise ReadOnlyModeError("Cannot execute CREATE or MERGE in read-only mode")

        if not linked_pairs:
            return []

        assert strategy in config.CREATE_OR_MERGE_STRATEGY, f"Invalid strategy {strategy}!"

        # Validate and prepare data
        node_maps, edge_maps, node_class, edge_class = self._prepare_linked_pairs(linked_pairs)

        # Execute CREATE for each pair
        return self._execute_linked_creates(parent_node, linked_pairs, node_maps, edge_maps, node_class, edge_class)

    def _prepare_linked_pairs(self, linked_pairs: list[tuple[GraphEdge, GraphNode]]) -> tuple[list[dict], list[dict], type, type]:
        """Validate and prepare linked pairs for creation."""
        node_maps = []
        edge_maps = []
        node_class = None
        edge_class = None

        for edge, node in linked_pairs:
            if not isinstance(node, GraphNode):
                raise ValueError(f"Expected GraphNode, got {type(node)}")
            if not isinstance(edge, GraphEdge):
                raise ValueError(f"Expected GraphEdge, got {type(edge)}")

            if node_class is None:
                node_class = node.__class__
            elif node_class != node.__class__:
                raise ValueError("All nodes must be of the same class")

            if edge_class is None:
                edge_class = edge.__class__
            elif edge_class != edge.__class__:
                raise ValueError("All edges must be of the same class")

            node.create_gid_if_missing()
            edge.create_gid_if_missing()

            node_maps.append(node.flatten_properties())
            edge_maps.append(edge.flatten_properties())

        return node_maps, edge_maps, node_class, edge_class

    def _execute_linked_creates(
        self,
        parent_node: GraphNode,
        linked_pairs: list[tuple[GraphEdge, GraphNode]],
        node_maps: list[dict],
        edge_maps: list[dict],
        node_class: type,
        edge_class: type,
    ) -> list[tuple[GraphEdge, GraphNode]]:
        """Execute CREATE statements for linked pairs."""
        updated_pairs = []

        for i, (edge, node) in enumerate(linked_pairs):
            node_props = ", ".join(f"{k}: {utils.convert_to_str(v)}" for k, v in node_maps[i].items())
            edge_props = ", ".join(f"{k}: {utils.convert_to_str(v)}" for k, v in edge_maps[i].items()) if edge_maps[i] else ""

            edge_props_clause = f" {{{edge_props}}}" if edge_props else ""
            cypher = f"""
            MATCH (parent) WHERE id(parent) = {parent_node.id_}
            CREATE (parent)-[r:{edge_class.__name__}{edge_props_clause}]->(n:{node_class.__name__} {{{node_props}}})
            RETURN id(n) as node_id, id(r) as edge_id
            """

            result = self.execute(cypher, unnest_result="r")

            if result:
                node.id_ = result[0]
                edge.id_ = result[1]
                edge.start_id_ = parent_node.id_
                edge.end_id_ = node.id_
                updated_pairs.append((edge, node))

        return updated_pairs

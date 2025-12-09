"""Hierarchical exporter for JSON/YAML with single/multiple file support.

Handles export of graph data to JSON/YAML formats, supporting both single file
and multiple file export modes with nested structure reconstruction.
"""

from __future__ import annotations

import os
from typing import Any

from cypher_graphdb import CypherGraphDB

from .file_exporter import FileExporter, FileExporterOptions
from .json_yaml_data_source import JsonYamlDataSource


class HierarchicalExporter(FileExporter):
    """JSON/YAML exporter with single/multiple file support.

    This exporter can save graph data to JSON or YAML formats in two modes:
    1. Single file: All data in one JSON/YAML file with nested structure
    2. Multiple files: Each entity type in separate files within a directory

    It reconstructs the nested structure that was flattened during import,
    re-attaching relations to their source nodes where appropriate.
    """

    def __init__(self, db: CypherGraphDB, opts: FileExporterOptions = None) -> None:
        """Initialize hierarchical exporter.

        Args:
            db: CypherGraphDB instance to export data from.
            opts: Export options configuration.
        """
        super().__init__(db, opts)
        self.valid_file_extensions = JsonYamlDataSource.get_file_extensions()
        self.data_handler = JsonYamlDataSource()

    def _write_to_file(self, items, dirname, filename):
        """Export to single file or multiple files.

        Args:
            items: Graph data to export (list of (label, entities) tuples).
            dirname: Directory path for multi-file export.
            filename: File path for single-file export.
        """
        if not items:
            return

        if dirname:
            # Multi-file export
            self._export_multiple_files(items, dirname)
        else:
            # Single file export
            self._export_single_file(items, filename)

    def _export_single_file(self, items, filename):
        """Export all data to a single JSON/YAML file.

        Args:
            items: Graph data to export.
            filename: Output file path.
        """
        nested_data = self._build_nested_structure(items)

        self._determine_format(filename)
        self.data_handler.save_file(nested_data, filename)

    def _export_multiple_files(self, items, dirname):
        """Export data to multiple JSON/YAML files.

        Args:
            items: Graph data to export.
            dirname: Output directory path.
        """
        groups = self._group_by_entity_type(items)
        format_type = self._determine_format(dirname)

        os.makedirs(dirname, exist_ok=True)

        for entity_type, entities in groups.items():
            filename = os.path.join(dirname, f"{entity_type}.{format_type}")

            if entity_type.startswith("_"):
                # Pure relation collection
                serialized_relations = [self._serialize_edge_with_targets(edge) for edge in entities]
                data = {entity_type: serialized_relations}
            else:
                # Node collection - need to find relations
                nested_structure = self._build_nested_structure_for_entity(entity_type, entities)
                data = nested_structure

            self.data_handler.save_file(data, filename)
            self.on_export_file(len(entities), dirname, filename, entity_type)

    def _build_nested_structure(self, items: list[tuple[str, list[Any]]]) -> dict[str, Any]:
        """Reconstruct nested structure from flat graph data.

        Args:
            items: List of (label, entities) tuples.

        Returns:
            Nested data structure suitable for JSON/YAML export.
        """
        # Separate nodes and edges
        nodes_by_label = {}
        edges_by_label = {}

        for label, entities in items:
            if self._is_edge_label(label):
                edges_by_label[label] = entities
            else:
                nodes_by_label[label] = entities

        # Create node lookup for relation attachment
        node_lookup = self._build_node_lookup(nodes_by_label)

        # Build nested structure
        result = {}

        # Add nodes with attached relations
        for label, nodes in nodes_by_label.items():
            result[label] = []

            for node in nodes:
                # Serialize node
                node_data = self._serialize_node(node)

                # Find and attach relations
                relations = self._find_relations_for_node(node, edges_by_label, node_lookup)
                if relations:
                    for relation in relations:
                        relation_key = relation["label_"]
                        node_data[relation_key] = node_data.get(relation_key, [])
                        node_data[relation_key].append(relation["target_data"])

                result[label].append(node_data)

        # Add standalone relation collections (those that couldn't be attached)
        standalone_relations = self._find_standalone_relations(edges_by_label, node_lookup)
        for label, relations in standalone_relations.items():
            result[f"_{label.lower()}"] = relations

        return result

    def _build_nested_structure_for_entity(self, entity_type: str, entities: list[Any]) -> dict[str, Any]:
        """Build nested structure for a specific entity type.

        Args:
            entity_type: The entity type to build structure for.
            entities: List of entities for this type.

        Returns:
            Nested data structure for the entity type.
        """
        if self._is_edge_label(entity_type):
            # Pure relation collection
            serialized_relations = [self._serialize_edge_with_targets(edge) for edge in entities]
            return {entity_type: serialized_relations}
        else:
            # Node collection - need to find relations
            # For now, serialize nodes without relations (complex case handled in full build)
            serialized_nodes = [self._serialize_node(node) for node in entities]
            return {entity_type: serialized_nodes}

    def _group_by_entity_type(self, items: list[tuple[str, list[Any]]]) -> dict[str, list[Any]]:
        """Group entities by type for multi-file export.

        Args:
            items: List of (label, entities) tuples.

        Returns:
            Dictionary mapping entity types to their entities.
        """
        groups = {}

        for label, entities in items:
            if self._is_edge_label(label):
                # Relations go to separate files
                groups[f"_{label.lower()}"] = entities
            else:
                # Nodes grouped by label
                groups[label] = entities

        return groups

    def _build_node_lookup(self, nodes_by_label: dict[str, list[Any]]) -> dict[str, Any]:
        """Build lookup dictionary for finding nodes by GID or ID.

        Args:
            nodes_by_label: Dictionary of nodes grouped by label.

        Returns:
            Lookup dictionary mapping GIDs/IDs to node data.
        """
        lookup = {}

        for label, nodes in nodes_by_label.items():
            for node in nodes:
                # Use GID if available, otherwise ID
                key = node.get("gid_") or node.get("id_")
                if key:
                    lookup[key] = {"node": node, "label": label}

        return lookup

    def _find_relations_for_node(
        self, node: Any, edges_by_label: dict[str, list[Any]], node_lookup: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Find all relations that originate from this node.

        Args:
            node: The source node.
            edges_by_label: Dictionary of edges grouped by label.
            node_lookup: Node lookup dictionary.

        Returns:
            List of relations with target node data.
        """
        relations = []
        node_id = node.get("id_")
        node_gid = node.get("gid_")

        for _label, edges in edges_by_label.items():
            for edge in edges:
                # Check if this edge originates from our node
                if edge.get("start_id_") == node_id or edge.get("start_gid_") == node_gid:
                    # Find target node
                    target_gid = edge.get("end_gid_")
                    target_id = edge.get("end_id_")

                    target_node = None
                    if target_gid and target_gid in node_lookup:
                        target_node = node_lookup[target_gid]["node"]
                    elif target_id:
                        # Find by ID (slower)
                        target_node = self._find_node_by_id(target_id, node_lookup)

                    if target_node:
                        relation_data = {
                            "label_": edge.get("label_"),
                            "target_data": self._merge_edge_with_target(edge, target_node),
                        }
                        relations.append(relation_data)

        return relations

    def _find_standalone_relations(
        self, edges_by_label: dict[str, list[Any]], node_lookup: dict[str, Any]
    ) -> dict[str, list[dict[str, Any]]]:
        """Find relations that couldn't be attached to specific nodes.

        Args:
            edges_by_label: Dictionary of edges grouped by label.
            node_lookup: Node lookup dictionary.

        Returns:
            Dictionary of standalone relation collections.
        """
        standalone = {}

        for label, edges in edges_by_label.items():
            unattached = []

            for edge in edges:
                # Check if edge can be resolved with current nodes
                start_gid = edge.get("start_gid_")
                end_gid = edge.get("end_gid_")

                if start_gid and end_gid and start_gid in node_lookup and end_gid in node_lookup:
                    node_lookup[start_gid]["node"]
                    target_node = node_lookup[end_gid]["node"]

                    relation_data = self._merge_edge_with_target(edge, target_node)
                    unattached.append(relation_data)

            if unattached:
                standalone[label] = unattached

        return standalone

    def _find_node_by_id(self, node_id: int, node_lookup: dict[str, Any]) -> dict[str, Any] | None:
        """Find node by ID in lookup dictionary.

        Args:
            node_id: Node ID to find.
            node_lookup: Node lookup dictionary.

        Returns:
            Node data if found, None otherwise.
        """
        for entry in node_lookup.values():
            if entry["node"].get("id_") == node_id:
                return entry["node"]
        return None

    def _merge_edge_with_target(self, edge: dict[str, Any], target_node: dict[str, Any]) -> dict[str, Any]:
        """Merge edge and target node data using new explicit relation format.

        Args:
            edge: Edge object or dictionary.
            target_node: Target node object or dictionary.

        Returns:
            Merged dictionary for relation export with explicit target information.
        """
        edge_dict = self._serialize_edge(edge)
        target_dict = self._serialize_node(target_node)

        # New explicit format: separate relation data from target data
        result = {}

        # Add relation identification (include GID for round-trip compatibility)
        relation_gid = edge_dict.get("gid_")
        if relation_gid:
            result["gid_"] = relation_gid

        # Add target identification (required)
        result["target_gid"] = target_dict.get("gid_")
        result["target_label"] = target_dict.get("label_") or "Entity"

        # Add edge properties (exclude internal fields)
        edge_props = {
            k: v for k, v in edge_dict.items() if k not in {"start_gid_", "end_gid_", "label_", "start_id_", "end_id_", "gid_"}
        }
        result.update(edge_props)

        # Add target node properties (exclude identification fields already handled)
        target_props = {k: v for k, v in target_dict.items() if k not in {"gid_", "label_"}}

        # Only add target properties if there are any beyond identification
        if target_props:
            result.update(target_props)

        return result

    def _serialize_edge(self, edge: Any) -> dict[str, Any]:
        """Serialize edge object to dictionary.

        Args:
            edge: Edge object or dictionary.

        Returns:
            Serialized edge dictionary.
        """
        if hasattr(edge, "flatten_properties"):
            edge_dict = edge.flatten_properties()
        elif isinstance(edge, dict):
            edge_dict = edge.copy()
        else:
            edge_dict = dict(edge)

        # Ensure required fields are present
        required_fields = ["start_gid_", "end_gid_", "label_"]
        for field in required_fields:
            if field not in edge_dict:
                edge_dict[field] = None

        return edge_dict

    def _serialize_node(self, node: Any) -> dict[str, Any]:
        """Serialize node object to dictionary.

        Args:
            node: Node object or dictionary.

        Returns:
            Serialized node dictionary.
        """
        if hasattr(node, "flatten_properties"):
            node_dict = node.flatten_properties()
            # Add label_ if node has label information
            if hasattr(node, "label_") and node.label_:
                node_dict["label_"] = node.label_
        elif isinstance(node, dict):
            node_dict = node.copy()
        else:
            node_dict = dict(node)

        return node_dict

    def _serialize_edge_with_targets(self, edge: Any) -> dict[str, Any]:
        """Serialize edge with embedded target information.

        Args:
            edge: Edge object with target information.

        Returns:
            Serialized edge with target data.
        """
        edge_dict = self._serialize_edge(edge)

        # Convert to new explicit format if target info is available
        if "target_gid_" in edge_dict:
            result = {
                "target_gid": edge_dict.pop("target_gid_"),
                "target_label": edge_dict.pop("target_label_", "Entity"),
            }

            # Add relation GID if available
            if "gid_" in edge_dict:
                result["gid_"] = edge_dict.pop("gid_")

            # Add remaining edge properties
            result.update(edge_dict)
            return result

        return edge_dict

    def _is_edge_label(self, label: str) -> bool:
        """Determine if label represents an edge type.

        Args:
            label: Label to check.

        Returns:
            True if label is an edge type, False otherwise.
        """
        # Edge labels often start with underscore or are all caps
        return (
            label.startswith("_") or label.isupper() or label.endswith("_BY") or label.endswith("_TO") or label.endswith("_FROM")
        )

    def _determine_format(self, filepath: str) -> str:
        """Determine export format from file path or options.

        Args:
            filepath: File path to analyze.

        Returns:
            Format string ('json' or 'yaml').
        """
        if self.opts and hasattr(self.opts, "format") and self.opts.format:
            return self.opts.format.lower()

        # Extract from file extension
        _, ext = os.path.splitext(filepath)
        ext = ext.lower().lstrip(".")

        if ext in ["yaml", "yml"]:
            return "yaml"
        elif ext in ["json"]:
            return "json"
        else:
            # Default to YAML for hierarchical export
            return "yaml"


__all__ = ["HierarchicalExporter"]

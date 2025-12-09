"""Data flattening utilities for hierarchical JSON/YAML structures.

Provides utilities to convert nested graph data structures into flat rows
suitable for processing by the existing tabular import infrastructure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class FlattenedNode:
    """Represents a flattened node with extracted relations."""

    node_data: dict[str, Any]
    relations: list[dict[str, Any]]
    source_gid: str | None = None
    source_label: str | None = None


class DataFlattener:
    """Converts nested structures to flat rows for processing.

    This class handles the conversion of hierarchical JSON/YAML data
    into a format that can be processed by the existing tabular import
    infrastructure while preserving relationship information.
    """

    @classmethod
    def flatten_item(cls, item: dict[str, Any], default_label: str) -> FlattenedNode:
        """Extract node properties and separate relations from a nested item.

        Args:
            item: The nested item containing node data and relations.
            default_label: Default label if not explicitly specified.

        Returns:
            FlattenedNode with separated node data and relations.
        """
        # Extract node properties (everything that's not a relation)
        node_data = cls._extract_node_properties(item)
        relations = cls._extract_relations(item)

        # Ensure label is present
        if "label_" not in node_data:
            node_data["label_"] = item.get("label_", default_label)

        return FlattenedNode(
            node_data=node_data, relations=relations, source_gid=item.get("gid_"), source_label=node_data.get("label_")
        )

    @classmethod
    def _extract_node_properties(cls, item: dict[str, Any]) -> dict[str, Any]:
        """Extract node properties from a nested item.

        Args:
            item: The nested item to process.

        Returns:
            Dictionary containing only node properties.
        """
        node_props = {}

        for key, value in item.items():
            # Skip relation fields and special internal fields
            if not cls._is_edge_field(key) and not cls._is_internal_field(key) and cls._is_node_property(value):
                node_props[key] = value

        return node_props

    @classmethod
    def _extract_relations(cls, item: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract relation data from a nested item.

        Relations are identified by 'edge:' prefix and can be:
        1. EXPLICIT format: target_gid/target_label for referencing existing nodes
        2. NESTED format: inline node: definitions for hierarchical structures

        Edge direction can be specified with suffixes:
        - edge:RELATION: (default, forward direction: parent → child)
        - edge:RELATION:forward (explicit forward direction: parent → child)
        - edge:RELATION:reverse (reverse direction: parent ← child)

        Args:
            item: The nested item to process.

        Returns:
            List of relation dictionaries with metadata.
        """
        relations = []

        for key, value in item.items():
            if cls._is_edge_field(key):
                relation_key, direction = cls._parse_edge_direction(key.removeprefix("edge:"))
                if isinstance(value, list):
                    # List of relations - check each item's format
                    for relation_item in value:
                        if isinstance(relation_item, dict) and any(k.startswith("node:") for k in relation_item):
                            # Nested format: {gid_: ..., node:Label: {...}}
                            relations.append(
                                {"relation_key": relation_key, "data": relation_item, "format": "nested", "direction": direction}
                            )
                        else:
                            # Explicit format: {target_gid: ..., target_label: ...}
                            relations.append(
                                {
                                    "relation_key": relation_key,
                                    "data": relation_item,
                                    "format": "explicit",
                                    "direction": direction,
                                }
                            )
                elif isinstance(value, dict):
                    # Check if nested format (contains node:) or explicit format
                    if any(k.startswith("node:") for k in value):
                        relations.append(
                            {"relation_key": relation_key, "data": value, "format": "nested", "direction": direction}
                        )
                    else:
                        relations.append(
                            {"relation_key": relation_key, "data": value, "format": "explicit", "direction": direction}
                        )

        return relations

    @classmethod
    def _parse_edge_direction(cls, edge_key: str) -> tuple[str, str]:
        """Parse edge key to extract relation and direction.

        Args:
            edge_key: Edge key with optional direction suffix.

        Returns:
            Tuple of (relation_name, direction) where direction is "forward" or "reverse".
        """
        if edge_key.endswith(":reverse"):
            return edge_key[:-8], "reverse"
        elif edge_key.endswith(":forward"):
            return edge_key[:-8], "forward"
        else:
            return edge_key, "forward"  # default direction

    @classmethod
    def _is_edge_field(cls, key: str) -> bool:
        """Check if field represents an edge using explicit prefix.

        Args:
            key: Field name to check.

        Returns:
            True if field starts with 'edge:', False otherwise.
        """
        return key.startswith("edge:")

    @classmethod
    def _is_node_field(cls, key: str) -> bool:
        """Check if field represents a node collection using explicit prefix.

        Args:
            key: Field name to check.

        Returns:
            True if field starts with 'node:', False otherwise.
        """
        return key.startswith("node:")

    @classmethod
    def _is_internal_field(cls, key: str) -> bool:
        """Check if field is internal metadata.

        Args:
            key: Field name to check.

        Returns:
            True if field is internal, False otherwise.
        """
        # Only exclude properties_ as internal, keep gid_ and label_ for processing
        internal_fields = {"properties_"}
        return key in internal_fields

    @classmethod
    def _is_node_property(cls, value: Any) -> bool:
        """Check if value should be treated as node property.

        Args:
            value: Value to check.

        Returns:
            True if value is a node property, False otherwise.
        """
        # Primitive values are always node properties
        if value is None or isinstance(value, (str, int, float, bool)):
            return True

        # Simple lists might be node properties (but not nested objects)
        if isinstance(value, list):
            # Only allow lists of primitives
            return all(isinstance(item, (str, int, float, bool)) for item in value)

        # Dicts are likely relations or nested structures
        return False

    @classmethod
    def extract_edge_properties(cls, relation_data: dict[str, Any]) -> dict[str, Any]:
        """Extract edge-specific properties from relation data.

        Edge properties are all properties EXCEPT:
        - target_gid, target_label (target identification for explicit format)
        - node: fields (nested nodes for nested format)

        Args:
            relation_data: The relation data to process.

        Returns:
            Dictionary containing edge properties.
        """
        edge_props = {}

        for key, value in relation_data.items():
            # Skip target identification fields and nested node definitions
            if key in {"target_gid", "target_label"} or key.startswith("node:"):
                continue
            # Include everything else as edge property
            edge_props[key] = value

        return edge_props

    @classmethod
    def extract_nested_nodes(cls, relation_data: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
        """Extract nested node definitions from a relation.

        Args:
            relation_data: The relation data containing nested nodes.

        Returns:
            List of tuples (node_label, node_data) for nested nodes.
        """
        nested_nodes = []

        for key, value in relation_data.items():
            if key.startswith("node:"):
                node_label = key.removeprefix("node:")
                if isinstance(value, list):
                    for node_data in value:
                        nested_nodes.append((node_label, node_data))
                elif isinstance(value, dict):
                    nested_nodes.append((node_label, value))

        return nested_nodes

    @classmethod
    def extract_target_node_properties(cls, relation_data: dict[str, Any]) -> dict[str, Any]:
        """Extract target node properties from relation metadata.

        In explicit format, target node properties are minimal:
        - target_gid becomes gid_
        - target_label is used for node label

        Args:
            relation_data: The relation data to process.

        Returns:
            Dictionary containing target node properties.
        """
        target_props = {}

        # Only extract explicit target identification
        if "target_gid" in relation_data:
            target_props["gid_"] = relation_data["target_gid"]

        return target_props


__all__ = [
    "DataFlattener",
    "FlattenedNode",
]

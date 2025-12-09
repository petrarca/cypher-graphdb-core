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
            if not cls._is_relation_field(key, value) and not cls._is_internal_field(key) and cls._is_node_property(value):
                node_props[key] = value

        return node_props

    @classmethod
    def _extract_relations(cls, item: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract relation data from a nested item.

        Relations are identified by 'edge:' prefix.

        Args:
            item: The nested item to process.

        Returns:
            List of relation dictionaries with metadata.
        """
        relations = []

        for key, value in item.items():
            if cls._is_edge_field(key):
                relation_key = key.removeprefix("edge:")
                if isinstance(value, list):
                    for relation_item in value:
                        relations.append({"relation_key": relation_key, "data": relation_item})
                elif isinstance(value, dict):
                    relations.append({"relation_key": relation_key, "data": value})

        return relations

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
    def _is_relation_field(cls, key: str, value: Any) -> bool:
        """Determine if a field represents a relation.

        DEPRECATED: Use _is_edge_field instead.
        Kept for backward compatibility during transition.

        Args:
            key: Field name to check.
            value: Field value to analyze (unused, kept for compatibility).

        Returns:
            True if field is a relation, False otherwise.
        """
        _ = value  # Mark as intentionally unused for backward compatibility
        # Use new explicit prefix detection
        return cls._is_edge_field(key)

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
        - target_gid, target_label (target identification)
        - Any property that looks like a node property (name, etc.)

        In explicit format, edge properties should be clearly separated.
        For simplicity, we include all non-target-identification properties.

        Args:
            relation_data: The relation data to process.

        Returns:
            Dictionary containing edge properties.
        """
        edge_props = {}

        for key, value in relation_data.items():
            # Skip target identification fields
            if key in {"target_gid", "target_label"}:
                continue
            # Include everything else as edge property
            edge_props[key] = value

        return edge_props

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

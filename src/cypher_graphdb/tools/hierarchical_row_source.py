"""Hierarchical row source for nested JSON/YAML data structures.

Extends the RowSource interface to handle nested graph data from JSON/YAML files,
converting it to flat rows compatible with the existing tabular import system.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from .data_flattener import DataFlattener, FlattenedNode
from .row_source import RowSource


class HierarchicalRowSource(RowSource):
    """Row source for nested JSON/YAML data structures.

    This class adapts nested JSON/YAML data to the RowSource interface,
    allowing it to be processed by the existing tabular import infrastructure.
    It handles the conversion from hierarchical data to flat rows while
    preserving relationship information for later processing.
    """

    def __init__(self, data: dict[str, Any], source_file: str = None):
        """Initialize hierarchical row source.

        Args:
            data: The nested data structure from JSON/YAML file.
            source_file: Optional source file path for error reporting.
        """
        self.data = data
        self.source_file = source_file
        self._current_iterator = None
        self._flattened_data = []
        self._current_index = 0

        # Pre-process the data to flatten it
        self._flatten_data()

    def _flatten_data(self):
        """Pre-process nested data into flattened rows."""

        for root_key, items in self.data.items():
            if isinstance(items, list):
                for item in items:
                    flattened = DataFlattener.flatten_item(item, root_key)
                    self._flattened_data.append(flattened)
            elif isinstance(items, dict):
                # Single object - treat as list with one item
                flattened = DataFlattener.flatten_item(items, root_key)
                self._flattened_data.append(flattened)

    def columns(self) -> list[str]:
        """Get flattened column names from nested structure.

        Returns:
            List of column names that will be present in the flattened rows.
        """
        if not self._flattened_data:
            return []

        # Collect all possible columns from all flattened items
        all_columns = set()

        for flattened_item in self._flattened_data:
            # Add node property columns
            all_columns.update(flattened_item.node_data.keys())

            # Add special relation marker columns
            all_columns.add("_has_relations")
            all_columns.add("_relation_count")

        # Sort with special columns first, then alphabetically
        special_columns = {"gid_", "label_", "_has_relations", "_relation_count"}
        sorted_special = sorted([col for col in all_columns if col in special_columns])
        sorted_regular = sorted([col for col in all_columns if col not in special_columns])

        return sorted_special + sorted_regular

    def iter_batches(self, batch_size: int) -> Iterator[Iterable[dict[str, Any]]]:
        """Yield flattened rows from nested data.

        Args:
            batch_size: Number of rows to include in each batch.

        Yields:
            Iterables of flattened row dictionaries.
        """
        self._current_index = 0

        while self._current_index < len(self._flattened_data):
            batch = []

            while len(batch) < batch_size and self._current_index < len(self._flattened_data):
                flattened_item = self._flattened_data[self._current_index]

                # Convert flattened item to row format
                row = self._flattened_item_to_row(flattened_item)
                batch.append(row)

                self._current_index += 1

            if batch:
                yield batch

    def _flattened_item_to_row(self, flattened_item: FlattenedNode) -> dict[str, Any]:
        """Convert a flattened item to a row dictionary.

        Args:
            flattened_item: The flattened item to convert.

        Returns:
            Row dictionary with flattened data.
        """
        row = flattened_item.node_data.copy()

        # Add relation metadata
        row["_has_relations"] = len(flattened_item.relations) > 0
        row["_relation_count"] = len(flattened_item.relations)

        # Store relations for later processing (not part of the row itself)
        row["_relations"] = flattened_item.relations

        return row

    def get_relations_for_row(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract relations from a flattened row.

        Args:
            row: The flattened row dictionary.

        Returns:
            List of relation dictionaries.
        """
        return row.get("_relations", [])

    def close(self):
        """Clean up resources."""
        self._current_iterator = None
        self._flattened_data.clear()
        self._current_index = 0

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics about the flattened data.

        Returns:
            Dictionary with statistics about the data.
        """
        if not self._flattened_data:
            return {"total_items": 0, "items_with_relations": 0, "total_relations": 0, "unique_labels": set()}

        total_items = len(self._flattened_data)
        items_with_relations = sum(1 for item in self._flattened_data if item.relations)
        total_relations = sum(len(item.relations) for item in self._flattened_data)
        unique_labels = {item.source_label for item in self._flattened_data if item.source_label}

        return {
            "total_items": total_items,
            "items_with_relations": items_with_relations,
            "total_relations": total_relations,
            "unique_labels": unique_labels,
        }


class RelationRowSource(RowSource):
    """Row source for processing standalone relation collections.

    Handles collections that contain only relation data (typically files
    with underscore prefixes like '_relations.json').
    """

    def __init__(self, relations_data: dict[str, Any], source_file: str = None):
        """Initialize relation row source.

        Args:
            relations_data: Dictionary containing relation collections.
            source_file: Optional source file path for error reporting.
        """
        self.relations_data = relations_data
        self.source_file = source_file
        self._relation_rows = []
        self._current_index = 0

        # Pre-process relations into flat rows
        self._flatten_relations()

    def _flatten_relations(self):
        """Convert nested relations to flat row format."""

        for relation_type, relations in self.relations_data.items():
            if isinstance(relations, list):
                for relation in relations:
                    row = self._relation_to_row(relation, relation_type)
                    self._relation_rows.append(row)
            elif isinstance(relations, dict):
                # Single relation
                row = self._relation_to_row(relations, relation_type)
                self._relation_rows.append(row)

    def _relation_to_row(self, relation: dict[str, Any], relation_type: str) -> dict[str, Any]:
        """Convert a single relation to row format.

        Args:
            relation: Relation data dictionary.
            relation_type: Type of the relation.

        Returns:
            Row dictionary representing the relation.
        """
        row = {
            "label_": relation_type.upper(),
            "start_gid_": relation.get("start_gid_"),
            "end_gid_": relation.get("end_gid_"),
            "start_label_": relation.get("start_label_"),
            "end_label_": relation.get("end_label_"),
        }

        # Add relation properties
        edge_props = DataFlattener.extract_edge_properties(relation)
        row.update(edge_props)

        return row

    def columns(self) -> list[str]:
        """Get column names for relation rows."""
        if not self._relation_rows:
            return []

        all_columns = set()
        for row in self._relation_rows:
            all_columns.update(row.keys())

        return sorted(all_columns)

    def iter_batches(self, batch_size: int) -> Iterator[Iterable[dict[str, Any]]]:
        """Yield relation rows in batches."""
        self._current_index = 0

        while self._current_index < len(self._relation_rows):
            batch = []

            while len(batch) < batch_size and self._current_index < len(self._relation_rows):
                batch.append(self._relation_rows[self._current_index])
                self._current_index += 1

            if batch:
                yield batch

    def close(self):
        """Clean up resources."""
        self._relation_rows.clear()
        self._current_index = 0


__all__ = [
    "HierarchicalRowSource",
    "RelationRowSource",
]

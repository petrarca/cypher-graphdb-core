"""Hierarchical importer for JSON/YAML data with two-phase processing.

Handles import of nested graph data structures using a two-phase approach:
1. Create all nodes from all entity types
2. Create all relations after nodes exist
"""

from __future__ import annotations

import glob
import os
from typing import Any

from cypher_graphdb import CypherGraphDB, utils
from cypher_graphdb.models import GraphEdge, GraphNode

from .data_flattener import DataFlattener
from .file_importer import FileImporter


class HierarchicalImporter(FileImporter):
    """Importer for hierarchical JSON/YAML data structures.

    This importer handles nested graph data where relationships are embedded
    within node objects, requiring a two-phase import approach.
    """

    def __init__(self, db: CypherGraphDB):
        """Initialize hierarchical importer.

        Args:
            db: CypherGraphDB instance to import data into.
        """
        super().__init__(db)
        self.valid_file_extensions = (".json", ".yaml", ".yml")

        # Import state
        self.node_cache = {}  # GID -> database ID mapping
        self.pending_relations = []  # Deferred relations for Phase 2
        self.nodes_created = 0
        self.edges_created = 0

    def load(self, file_or_dirname: str, recursive: bool = False) -> list[str]:
        """Load folder as combined dataset or single file.

        Args:
            file_or_dirname: Path to file or directory containing JSON/YAML files.
            recursive: Whether to scan directories recursively.

        Returns:
            List of successfully processed files.
        """
        processed_files = []
        combined_data = {}

        # 1. Load and combine all data
        if os.path.isfile(file_or_dirname):
            # Single file mode
            combined_data = self._load_single_file(file_or_dirname)
            if combined_data:  # Only add to processed_files if loading succeeded
                processed_files = [file_or_dirname]
            else:
                processed_files = []
        elif os.path.isdir(file_or_dirname):
            # Directory mode
            processed_files = self._load_directory(file_or_dirname, recursive, combined_data)
        else:
            print(f"Error: {file_or_dirname} is not a valid file or directory")
            return []

        # 2. Two-phase import on combined data
        if combined_data:
            self._import_combined_data(combined_data)

        return processed_files

    def load_from_file(self, filename: str):
        """Load data from a specific file."""
        combined_data = self._load_single_file(filename)
        if combined_data:
            self._import_combined_data(combined_data)

    def _load_directory(self, dirname: str, recursive: bool, combined_data: dict[str, Any]) -> list[str]:
        """Load all JSON/YAML files from directory and combine data."""

        pattern = "**/*" if recursive else "*"
        search_pattern = os.path.join(dirname, pattern)

        processed_files = []
        for filepath in glob.glob(search_pattern, recursive=recursive):
            if os.path.isfile(filepath) and filepath.endswith(self.valid_file_extensions):
                try:
                    file_data = self._load_single_file(filepath)
                    if file_data:
                        self._merge_data(combined_data, file_data)
                        processed_files.append(filepath)
                        self.on_import_file(filepath, None, len(file_data))
                except (OSError, ValueError, KeyError) as ex:
                    print(f"Failed to load {filepath}: {ex}")

        return processed_files

    def _load_single_file(self, filename: str) -> dict[str, Any]:
        """Load a single JSON/YAML file."""

        try:
            from .json_yaml_data_source import JsonYamlDataSource

            data = JsonYamlDataSource.load_file(filename)

            total_items = sum(len(items) if isinstance(items, list) else 1 for items in data.values())
            self.on_import_file(filename, None, total_items)

            return data

        except (OSError, ValueError, KeyError) as ex:
            print(f"Failed to load {filename}: {ex}")
            self.on_invalid_file(filename)
            return {}

    def _merge_data(self, combined: dict[str, Any], new_data: dict[str, Any]):
        """Merge new data into combined dataset."""

        for key, items in new_data.items():
            if key in combined:
                if isinstance(combined[key], list) and isinstance(items, list):
                    combined[key].extend(items)
                else:
                    # Convert to list if needed
                    if not isinstance(combined[key], list):
                        combined[key] = [combined[key]]
                    if not isinstance(items, list):
                        items = [items]
                    combined[key].extend(items)
            else:
                combined[key] = items

    def _import_combined_data(self, combined_data: dict[str, Any]):
        """Two-phase import on merged dataset."""

        # Reset counters
        self.nodes_created = 0
        self.edges_created = 0
        self.node_cache.clear()
        self.pending_relations.clear()

        # Use transaction batching for performance
        try:
            # Phase 1: Create all nodes
            self._phase1_create_all_nodes(combined_data)

            # Phase 2: Create all relations
            self._phase2_create_all_relations(combined_data)

            # Final commit
            self.db.commit()

        except (ValueError, KeyError, RuntimeError) as ex:
            # Rollback on error
            self.db.rollback()
            print(f"Error during import: {ex}")
            raise

    def _phase1_create_all_nodes(self, combined_data: dict[str, Any]):
        """Extract and create all nodes from combined dataset."""

        for entity_type, items in combined_data.items():
            if self._is_node_collection(entity_type, items):
                self._process_node_collection(entity_type, items)

    def _process_node_collection(self, entity_type: str, items: list[dict[str, Any]]):
        """Process a collection of nodes with embedded relations."""

        for item in items:
            try:
                # Flatten the item to separate node data from relations
                flattened = DataFlattener.flatten_item(item, entity_type)

                # Create the node
                node_id = self._create_node_from_flattened(flattened)

                # Collect relations for Phase 2
                for relation in flattened.relations:
                    self.pending_relations.append(
                        {
                            "source_id": node_id,
                            "source_gid": flattened.source_gid,
                            "relation_data": relation,
                            "source_context": flattened.node_data,
                        }
                    )

            except (ValueError, KeyError, TypeError) as ex:
                print(f"Error processing node in {entity_type}: {ex}")
                continue

    def _create_node_and_cache_id(self, node: GraphNode, original_gid: str | None = None) -> int:
        """Create node using create_or_merge and cache the ID for relationship resolution.

        This is the single source of truth for node creation and ID caching,
        eliminating duplication across the importer.
        """
        result = self.db.create_or_merge(node)

        # Get the node ID (create_or_merge returns GraphNode with updated state)
        if isinstance(result, GraphNode):
            node_id = result.id_
            # Use the actual GID from the created/merged node for caching
            actual_gid = result.properties_.get("gid_")
            if actual_gid:
                self.node_cache[actual_gid] = node_id
        else:
            node_id = result
            # Cache using original GID as fallback
            if original_gid:
                self.node_cache[original_gid] = node_id

        return node_id

    def _create_node_from_flattened(self, flattened: DataFlattener.FlattenedNode) -> int:
        """Create a node from flattened data, preserving original GID."""

        # Use the same approach as tabular importer - slice model fields from properties
        _, node_properties = utils.slice_model_properties(GraphNode, flattened.node_data)
        node_properties = utils.resolve_properties(node_properties)

        # Create the node with original GID preserved
        node = GraphNode(label_=flattened.source_label, properties_=node_properties)

        node_id = self._create_node_and_cache_id(node, flattened.source_gid)
        self.nodes_created += 1
        return node_id

    def _phase2_create_all_relations(self, combined_data: dict[str, Any]):
        """Create all relations after all nodes exist."""

        # Process embedded relations from node collections
        for pending in self.pending_relations:
            try:
                self._create_relation_from_pending(pending)
            except (ValueError, KeyError, RuntimeError) as ex:
                print(f"Error creating relation: {ex}")
                continue

        # Process standalone relation collections
        for entity_type, items in combined_data.items():
            if self._is_relation_collection(entity_type, items):
                self._process_standalone_relations(entity_type, items)

    def _create_edge_and_increment(self, label: str, start_id: int, end_id: int, properties: dict[str, Any]) -> None:
        """Create edge using create_or_merge and increment counter.

        This follows the same pattern as CSV/Tabular importer - includes GID in properties
        to let create_or_merge handle duplicate detection properly via GID matching.
        """
        edge = GraphEdge(
            label_=label,
            start_id_=start_id,
            end_id_=end_id,
            properties_=properties,
        )
        self.db.create_or_merge(edge)
        self.edges_created += 1

    def _create_relation_from_pending(self, pending: dict[str, Any]):
        """Create a relation from pending data."""

        source_id = pending["source_id"]
        relation_key = pending["relation_data"]["relation_key"]
        relation_data = pending["relation_data"]["data"]

        # Resolve target node
        target_id = self._resolve_target_node(relation_data)
        if not target_id:
            print(f"Warning: Could not resolve target node for relation {relation_key}")
            return

        # Extract edge properties
        edge_properties = DataFlattener.extract_edge_properties(relation_data)

        # Create the edge
        self._create_edge_and_increment(relation_key, source_id, target_id, edge_properties)

    def _resolve_target_node(self, relation_data: dict[str, Any]) -> int | None:
        """Resolve target node by target_gid.

        Requires explicit format with target_gid. If target node doesn't exist
        in cache, creates it using target_gid and target_label.
        """
        target_gid = relation_data.get("target_gid")
        if not target_gid:
            return None

        # Check cache first
        if target_gid in self.node_cache:
            return self.node_cache[target_gid]

        # Create target node if it doesn't exist
        return self._create_target_node(relation_data)

    def _create_target_node(self, relation_data: dict[str, Any]) -> int:
        """Create target node from relation data, preserving original GID."""

        target_properties = DataFlattener.extract_target_node_properties(relation_data)

        # Require explicit target_label in new format
        target_label = relation_data.get("target_label")
        if not target_label:
            raise ValueError(f"Missing required 'target_label' in relation data: {relation_data}")

        # Handle new explicit format
        target_gid = relation_data.get("target_gid")
        if not target_gid:
            raise ValueError(f"Missing required 'target_gid' in relation data: {relation_data}")

        node = GraphNode(label_=target_label, properties_=target_properties)
        return self._create_node_and_cache_id(node, target_gid)

    def _process_standalone_relations(self, entity_type: str, items: list[dict[str, Any]]):
        """Process standalone relation collections."""

        for item in items:
            try:
                # For standalone relations, we need to resolve both endpoints
                source_gid = item.get("source_gid")
                target_gid = item.get("target_gid")

                if source_gid and target_gid:
                    source_id = self.node_cache.get(source_gid)
                    target_id = self.node_cache.get(target_gid)

                    if source_id and target_id:
                        self._create_edge_and_increment(entity_type, source_id, target_id, item)

            except (ValueError, KeyError, RuntimeError) as ex:
                print(f"Error processing standalone relation: {ex}")
                continue

    def _is_node_collection(self, entity_type: str, items: Any) -> bool:
        """Determine if a collection represents nodes."""
        return not self._is_relation_collection(entity_type, items)

    def _is_relation_collection(self, entity_type: str, items: Any) -> bool:
        """Determine if a collection represents relations."""
        _ = items  # Parameter kept for interface consistency
        return entity_type.endswith("_relation") or entity_type.endswith("_relations")


__all__ = ["HierarchicalImporter"]

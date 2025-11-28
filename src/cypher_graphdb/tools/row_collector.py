"""RowCollector: lightweight replacement for DataFrameBuilder (no pandas).

Converts GraphNode / GraphEdge collections into list-of-dict rows for CSV / Excel.
Expands edge start/end references to gid (and optional label) matching previous
export semantics while avoiding DataFrame overhead.

Uses pure Python dictionary operations for optimal performance with zero dependencies.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from cypher_graphdb import config, utils
from cypher_graphdb.backend import BackendCapability
from cypher_graphdb.cyphergraphdb import CypherGraphDB, MatchCriteria
from cypher_graphdb.models import GraphEdge, GraphNode


class RowCollector:
    """Collect graph nodes/edges into list-of-dict rows with minimal lookups.

    Responsibilities:
    - Cache node (gid,label) metadata in an in-memory dictionary for fast lookups
    - Batch-fetch missing node metadata to minimize backend round-trips when
      expanding edge start/end references.
    - Flatten edge/node properties into a tabular friendly structure consumed
      by CSV/Excel exporters without requiring pandas.

    Note: Call close() when done to release resources (currently no-op for pure Python).
    """

    def __init__(self, db: CypherGraphDB, with_label: bool = True):
        self.db = db
        self.with_label = with_label
        self._label: str | None = None
        # node metadata cache: id -> (gid, label)
        self._node_cache: dict[int, tuple[str | None, str | None]] = {}

    def collect(self, entities: Iterable[GraphNode | GraphEdge]) -> list[dict[str, Any]]:
        # Reset label constraint so the same collector instance can be reused across
        # multiple label groups (exporters process node groups first, then edge groups).
        # Cached node metadata (id -> (gid, label)) persists across calls to avoid
        # repeated backend lookups when edges are processed after their nodes.
        self._label = None

        # Separate nodes and edges to pre-fill node cache and allow batch edge expansion
        nodes: list[GraphNode] = []
        edges: list[GraphEdge] = []
        for e in entities:
            if isinstance(e, GraphNode):
                nodes.append(e)
            elif isinstance(e, GraphEdge):
                edges.append(e)

        rows: list[dict[str, Any]] = []

        # Process nodes first (sets self._label and caches metadata)
        if nodes:
            for n in nodes:
                rows.append(self._row_from_entity(n))
                gid = n.properties_.get(config.PROP_GID) if n.properties_ else None
                self._node_cache[n.id_] = (gid, n.label_)

        if not edges:
            return rows

        rows.extend(self._collect_edges_pure_python(edges))

        return rows

    def _collect_edges_pure_python(self, edges: list[GraphEdge]) -> list[dict[str, Any]]:
        """Process edges using pure Python dictionary lookups instead of SQL JOINs."""
        # Determine endpoint ids for edges; if they're already in cache, skip lookups entirely.
        endpoint_ids: set[int] = set()
        for e in edges:
            if e.start_id_ is not None:
                endpoint_ids.add(e.start_id_)
            if e.end_id_ is not None:
                endpoint_ids.add(e.end_id_)
        missing_ids = endpoint_ids.difference(self._node_cache.keys())

        # Batch populate cache for any node ids not already seen (minimize backend lookups)
        if missing_ids:
            self._bulk_populate_node_cache(missing_ids)

        result: list[dict[str, Any]] = []

        for edge in edges:
            edge_data = edge.model_dump(context={"with_type": False})

            # Get start node metadata
            start_gid, start_label = self._node_cache.get(edge.start_id_, (None, None))
            # Get end node metadata
            end_gid, end_label = self._node_cache.get(edge.end_id_, (None, None))

            # Build result row
            row: dict[str, Any] = {
                "start_gid_": start_gid,
                "end_gid_": end_gid,
            }

            # Add edge label if requested
            if self.with_label:
                row["label_"] = edge.label_

            # Add labels if requested
            if self.with_label:
                row["start_label_"] = start_label
                row["end_label_"] = end_label

            # Add edge properties
            edge_props = edge_data.pop("properties_", {}) or {}
            row.update(edge_props)

            result.append(row)

        return result

    def _row_from_entity(self, obj: GraphNode | GraphEdge) -> dict[str, Any]:
        obj.resolve()
        if obj.label_ is None:
            raise RuntimeError("GraphObject requires a label!")
        if self._label and obj.label_ != self._label:
            raise RuntimeError(f"Mixed labels not supported: '{self._label}' != '{obj.label_}'")
        data = obj.model_dump(context={"with_type": False})
        self._update_references(obj, data)

        # Add properties but respect with_label setting
        properties = data.pop("properties_", {}) or {}
        data.update(properties)

        # Ensure label_ is removed if with_label is False (might be added back from properties)
        if not self.with_label:
            data.pop("label_", None)

        self._label = obj.label_
        return data

    def _update_references(self, obj, data: dict[str, Any]):
        data.pop("id_", None)
        if not self.with_label:
            data.pop("label_", None)
        if isinstance(obj, GraphEdge):
            self._expand_edge(obj, data)

    def _expand_edge(self, edge: GraphEdge, data: dict[str, Any]):
        # Use cached metadata populated earlier; fallback to on-demand fetch if missing
        sgid, slabel = self._node_cache.get(edge.start_id_, (None, None))
        egid, elabel = self._node_cache.get(edge.end_id_, (None, None))

        if sgid is None and edge.start_id_ is not None:
            self._populate_node_cache(edge.start_id_)
            sgid, slabel = self._node_cache.get(edge.start_id_, (None, None))
        if egid is None and edge.end_id_ is not None:
            self._populate_node_cache(edge.end_id_)
            egid, elabel = self._node_cache.get(edge.end_id_, (None, None))

        data["start_gid_"], data["end_gid_"] = sgid, egid
        if self.with_label:
            data["start_label_"], data["end_label_"] = slabel, elabel
        data.pop("start_id_", None)
        data.pop("end_id_", None)

    def _populate_node_cache(self, node_id: int):
        # Delegate to bulk path for code reuse
        self._bulk_populate_node_cache({node_id})

    # --- internal helpers -------------------------------------------------
    def _bulk_populate_node_cache(self, node_ids: Iterable[int]):
        """Populate node cache for given ids in batches.

        Uses backend batch fetch to minimize round-trips. Falls back silently
        if backend returns missing nodes.
        """
        to_fetch = [nid for nid in node_ids if nid not in self._node_cache]
        if not to_fetch:
            return
        for batch in utils.chunk_list(to_fetch, 50):
            self._fetch_batch_node_meta(batch)

    def _fetch_batch_node_meta(self, batch: list[int]):
        """Fetch metadata for a batch of node ids using batch API or fallback."""
        try:
            nodes = self.db.fetch_nodes_by_ids(batch) or []
        except Exception:  # noqa: BLE001 - fallback to individual fetching on any batch API error
            self._fallback_fetch_nodes(batch)
            return
        insert_rows: list[tuple[int, str | None, str | None]] = []
        for n in nodes:
            gid = n.properties_.get(config.PROP_GID) if n.properties_ else None
            self._node_cache[n.id_] = (gid, n.label_)
            insert_rows.append((n.id_, gid, n.label_))
        if insert_rows:
            self._insert_or_replace_nodes(insert_rows)
        returned_ids = {n.id_ for n in nodes}
        for nid in batch:
            if nid not in returned_ids and nid not in self._node_cache:
                self._node_cache[nid] = (None, None)
                self._insert_or_replace_nodes([(nid, None, None)])

    def _fallback_fetch_nodes(self, batch: list[int]):
        """Per-id metadata fetch fallback when batch API fails."""
        # Get the label function pattern from backend capabilities (once)
        label_pattern = self.db.backend.get_capability(BackendCapability.LABEL_FUNCTION)
        label_expr = utils.resolve_template(label_pattern, node="n")

        for nid in batch:
            criteria = MatchCriteria(prefix_="n", projection_=[config.PROP_GID, label_expr])
            criteria.id_ = nid
            meta = self.db.fetch_nodes(criteria, unnest_result=True)
            if isinstance(meta, tuple):
                gid, label = meta
            elif meta is not None:
                gid, label = meta.properties_.get(config.PROP_GID), meta.label_
            else:
                gid, label = None, None
            self._node_cache[nid] = (gid, label)
            self._insert_or_replace_nodes([(nid, gid, label)])

    def _insert_or_replace_nodes(self, rows: list[tuple[int, str | None, str | None]]):
        """Insert or replace node cache rows in dictionary (pure Python)."""
        if not rows:
            return
        for node_id, gid, label in rows:
            self._node_cache[node_id] = (gid, label)

    def close(self):
        """Release resources (no-op for pure Python implementation).

        Kept for API compatibility with existing code.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


__all__ = ["RowCollector"]

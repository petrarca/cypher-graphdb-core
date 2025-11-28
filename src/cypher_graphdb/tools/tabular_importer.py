"""Tabular importer (CSV / Excel row-source based) replacing DataFrameImporter.

Phase 1 implementation: functional parity with existing DataFrameImporter
using the new RowSource abstraction. Edge reference pre-resolution and
UNWIND batching will be added in subsequent steps.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from cypher_graphdb import config, utils
from cypher_graphdb.cyphergraphdb import CypherGraphDB, MatchCriteria
from cypher_graphdb.models import GraphEdge, GraphNode

from .duckdb_source import RowSource


@dataclass
class ColumnInfo:
    """Metadata about input columns used to guide import logic.

    Flags are derived once per source to avoid recomputation per row and to
    decide whether rows describe nodes or edges and how edge endpoints should
    be resolved (by id, gid or composite key).
    """

    contains_label: bool = False
    contains_gid: bool = False
    edge_node_references: config.MatchReference | None = None


class TabularImporter:
    """Importer operating on a generic RowSource (streamed batches).

    Usage:
        source = CsvSource("file.csv")
        TabularImporter(db).load(source, label="Person")
    """

    def __init__(
        self,
        db: CypherGraphDB,
        import_batch_size: int | None = None,
        commit_every: int | None = None,
    ) -> None:
        assert isinstance(db, CypherGraphDB)
        self.db = db
        self.import_batch_size = import_batch_size or getattr(config, "IMPORT_BATCH_SIZE", 1000)
        self.commit_every = commit_every or getattr(config, "COMMIT_EVERY", self.import_batch_size)

        # statistics / telemetry counters
        self.nodes_created = 0
        self.edges_created = 0
        self._processed_since_commit = 0

    # ---------------------------- public API ----------------------------
    def load(self, source: RowSource, label: str | None = None) -> list[dict[str, Any]]:
        """Stream rows from the source into the graph database.

        Args:
            source: RowSource implementation (CsvSource / ExcelRowSource).
            label: Fallback label if not present as column.

        Returns:
            List of error records (each original row + error_ message).

        Note:
            This method does not close the source. The caller is responsible for
            closing the source or using it as a context manager with the 'with' statement.
        """
        errors: list[dict[str, Any]] = []

        columns = source.columns()
        if not columns:
            return errors

        col_info = self.analyze_columns(columns)

        if not col_info.contains_label and not label:
            errors.append({"error_": "Missing label (no label_ column and no fallback provided)"})
            return errors

        # Placeholder: future optimization pre-resolution of edge references
        # self._preresolve_edge_references(source, col_info)

        for batch in source.iter_batches(self.import_batch_size):
            self._process_batch(batch, col_info, label, errors)
            if self._processed_since_commit >= self.commit_every:
                self.db.commit()
                self._processed_since_commit = 0

        if self._processed_since_commit:
            self.db.commit()
            self._processed_since_commit = 0

        return errors

    # ------------------------- internal helpers -------------------------
    @classmethod
    def analyze_columns(cls, columns: list[str]) -> ColumnInfo:
        info = ColumnInfo()
        for col in columns:
            if col == config.PROP_LABEL:
                info.contains_label = True
            elif col == config.PROP_GID:
                info.contains_gid = True

        # determine edge reference type
        reference_props = [
            (config.MatchReference.BY_GID, config.REF_PROPS_BY_SE_GID),
            (config.MatchReference.BY_KEY, config.REF_PROPS_BY_KEY),
        ]
        col_set = set(columns)
        for ref_type, required_cols in reference_props:
            if set(required_cols).issubset(col_set):
                info.edge_node_references = ref_type
                break
        return info

    def _process_batch(
        self,
        batch: Iterable[dict[str, Any]],
        col_info: ColumnInfo,
        fallback_label: str | None,
        errors: list[dict[str, Any]],
    ):
        for row in batch:
            try:
                self._process_row(row, col_info, fallback_label, errors)
            except Exception as ex:  # noqa: BLE001 - capture unexpected row issues
                err = dict(row)
                err["error_"] = f"Unexpected error: {ex}"  # do not re-raise to continue streaming
                errors.append(err)

    def _process_row(
        self,
        row: dict[str, Any],
        col_info: ColumnInfo,
        fallback_label: str | None,
        errors: list[dict[str, Any]],
    ):
        # Determine label
        row_label = fallback_label if not col_info.contains_label else row.get("label_")
        if utils.isnan(row_label):  # handle NaN numeric from CSV data
            self._add_error(row, "Missing node/edge label", errors)
            return

        if col_info.edge_node_references:
            self._create_or_merge_edge(row, row_label, col_info, errors)
        else:
            self._create_or_merge_node(row, row_label)

    # ----------------------------- node logic ----------------------------
    def _create_or_merge_node(self, row: dict[str, Any], label: str):
        _, properties = utils.slice_model_properties(GraphNode, row)
        if (raw_props := row.get("properties_")) and isinstance(raw_props, str):
            # Expect JSON string; fallback to legacy safe eval if needed
            props = self._parse_properties(raw_props)
            if props:
                properties.update(props)
        properties = utils.resolve_properties(properties)
        node = GraphNode(properties_=properties, label_=label)
        self.db.create_or_merge(node)
        self.nodes_created += 1
        self._processed_since_commit += 1

    # ----------------------------- edge logic ----------------------------
    def _create_or_merge_edge(
        self,
        row: dict[str, Any],
        label: str,
        col_info: ColumnInfo,
        errors: list[dict[str, Any]],
    ):
        start_id, end_id = self._resolve_start_end_ids(row, col_info.edge_node_references, errors)
        if not start_id or not end_id:
            return

        # Slice out model fields; extra columns (including reference columns + JSON props) go to properties
        _, properties = utils.slice_model_properties(GraphEdge, row)

        # Parse JSON encoded dynamic properties if present (mirrors node path)
        if (raw_props := row.get("properties_")) and isinstance(raw_props, str):
            props = self._parse_properties(raw_props)
            if props:
                properties.update(props)

        # Remove edge reference helper columns from final properties (they are only for lookup)
        ref_cols = (
            "start_gid_",
            "end_gid_",
            "start_label_",
            "end_label_",
            "start_key_",
            "end_key_",
        )
        for k in ref_cols:
            properties.pop(k, None)

        properties = utils.resolve_properties(properties)
        edge = GraphEdge(label_=label, start_id_=start_id, end_id_=end_id, properties_=properties)
        self.db.create_or_merge(edge)
        self.edges_created += 1
        self._processed_since_commit += 1

    def _resolve_start_end_ids(
        self,
        row: dict[str, Any],
        edge_reference: config.MatchReference | None,
        errors: list[dict[str, Any]],
    ):
        def node_id_by_props(id_, label_, properties_, is_start: bool):
            criteria = MatchCriteria(
                id_=id_,
                label_=label_,
                prefix_="n",
                properties_=utils.dict_from_value_pairs(properties_) if properties_ else None,
                projection_=["id(n)"],
            )
            result = self.db.fetch_nodes(criteria, unnest_result=True)
            if not result:
                self._add_error(row, f"{'Start' if is_start else 'End'} node: Not found!", errors)
                return None
            if not isinstance(result, int):
                self._add_error(row, f"{'Start' if is_start else 'End'} node: Must exact match to one node!", errors)
                return None
            return result

        match edge_reference:
            case config.MatchReference.BY_ID:
                return row.get("start_id_"), row.get("end_id_")
            case config.MatchReference.BY_GID:
                return (
                    node_id_by_props(None, None, {config.PROP_GID: row.get("start_gid_")}, True),
                    node_id_by_props(None, None, {config.PROP_GID: row.get("end_gid_")}, False),
                )
            case config.MatchReference.BY_KEY:
                return (
                    node_id_by_props(None, row.get("start_label_"), row.get("start_key_"), True),
                    node_id_by_props(None, row.get("end_label_"), row.get("end_key_"), False),
                )
            case _:
                self._add_error(row, "Unsupported edge reference type", errors)
                return None, None

    # ----------------------------- utilities -----------------------------
    def _add_error(self, row: dict[str, Any], message: str, errors: list[dict[str, Any]]):
        err = dict(row)
        err["error_"] = message
        errors.append(err)

    def _parse_properties(self, raw: str):  # noqa: D401 - obvious
        import json

        try:
            return json.loads(raw)
        except Exception:
            # fallback legacy format (python literal) - removed later
            try:
                import ast

                return ast.literal_eval(raw)
            except Exception:
                return None


__all__ = ["TabularImporter", "ColumnInfo"]

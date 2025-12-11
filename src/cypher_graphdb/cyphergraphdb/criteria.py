"""Match criteria classes for CypherGraphDB queries."""

from typing import Any

from pydantic import BaseModel

from .. import config
from ..models import GraphEdge, GraphNode


class MatchCriteria(BaseModel):
    """Base criteria for matching nodes or edges in the graph database.

    Defines id, labels, and property filters for queries.
    """

    id_: int | None = None
    label_: str | type[GraphNode | GraphEdge] | list[str | type[GraphNode | GraphEdge]] | None = None
    properties_: dict[str, Any] | None = None
    prefix_: str | None = None
    projection_: list[str] | None = None

    @property
    def has_id(self):
        return self.id_ is not None

    @property
    def has_gid(self):
        return self.properties_.get(config.PROP_GID, False) if isinstance(self.properties_, dict) else False

    @property
    def has_unique_ids(self):
        return self.has_id or self.has_gid

    @property
    def has_properties(self):
        return self.properties_

    @property
    def has_labels(self):
        return isinstance(self.label_, list | tuple)

    @property
    def has_projection(self):
        return self.projection_

    def get_prefix(self, default_prefix=None):
        return self.prefix_ if self.prefix_ is not None else default_prefix

    def resolve(self):
        if isinstance(self.properties_, dict):
            if "id_" in self.properties_:
                self.id_ = self.properties_.get("id_") if self.id_ is None else self.id_
                self.properties_.pop("id_")

            if "label_" in self.properties_:
                self.label_ = self.properties_.get("label_") if self.label_ is None else self.label_
                self.properties_.pop("label_")

        self._resolve_label()

    def _resolve_label(self) -> str:
        def label_to_literal(lbl) -> str:
            if isinstance(lbl, str):
                return lbl

            if issubclass(lbl, GraphNode | GraphEdge):
                return lbl.graph_info_.label_

            raise RuntimeError("Label must either be 'str|GraphNode|GraphEdge'")

        if self.label_ is None:
            return None

        if isinstance(self.label_, list):
            self.label_ = [label_to_literal(lbl) for lbl in self.label_]
        else:
            self.label_ = label_to_literal(self.label_)


class MatchNodeCriteria(MatchCriteria):
    """Criteria for matching graph nodes (inherits MatchCriteria)."""

    pass


class MatchNodeById(MatchNodeCriteria):
    """Criteria for matching a node by its numeric ID."""

    def __init__(self, id):
        """Initialize with the given node ID."""
        self.id_ = id


class MatchEdgeCriteria(MatchCriteria):
    """Criteria for matching graph edges, including optional start/end filters and fetch behavior."""

    start_criteria_: MatchCriteria = None
    end_criteria_: MatchCriteria = None
    fetch_nodes_: bool = False

    def resolve(self):
        super().resolve()
        if self.start_criteria_ is not None:
            assert isinstance(self.start_criteria_, MatchCriteria)
            self.start_criteria_.resolve()

        if self.end_criteria_ is not None:
            assert isinstance(self.end_criteria_, MatchCriteria)
            self.end_criteria_.resolve()


class MatchEdgeById(MatchEdgeCriteria):
    """Criteria for matching an edge by its numeric ID."""

    def __init__(self, id):
        """Initialize with the given edge ID."""
        self.id_ = id

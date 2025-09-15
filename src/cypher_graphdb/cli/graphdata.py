"""CLI graph data module: Manages graph data operations in the CLI.

This module handles graph data manipulation and storage for the CLI interface,
including adding results and managing the current graph state.
"""

import math
from typing import Any

import rich

import cypher_graphdb.config as config
import cypher_graphdb.utils as utils
from cypher_graphdb import CypherGraphDB, Graph, GraphEdge, GraphNode, GraphObject, MatchCriteria

from .provider import GraphDataProvider, VarProvider


class CLIGraphData(VarProvider, GraphDataProvider):
    """CLI provider for graph data management and variable storage."""

    def __init__(self) -> Any:
        self.db: CypherGraphDB = None
        self._graph: Graph = Graph()
        self._last_result = None
        self._variables = {}

    @property
    def graph(self):
        return self._graph

    @property
    def last_result(self):
        return self._last_result

    @last_result.setter
    def last_result(self, value):
        self._last_result = value

    @property
    def variables(self):
        return self._variables

    def get_var(self, varname: str | None) -> dict:
        if varname:
            if varname == ".":
                return self._last_result
            elif varname.startswith("_"):
                return self._graph
            else:
                return self._variables.get(varname, None)
        else:
            return None

    def get_varnames(self) -> tuple[str]:
        result = sorted(self._variables.keys())

        if self._last_result:
            result.append(".")
        if self._graph:
            result.append("_")

        return tuple(result)

    def set_var(self, name: str, value: object) -> dict:
        if value is None:
            # remove variable
            return self._variables.pop(name, None)

        if isinstance(value, str):
            obj = self._last_result if value in (".", "") else self.fetch_graph_obj(value)

            if obj is None:
                rich.print(f"[red]Value '{value}' could not be resolved to a graph object!")
                return None
        else:
            obj = value

        if name == ".":
            self._last_result = obj
        elif name in ("_", "__"):
            # merge int graph, with (double underscore) or without (underscore) clearing the graph before
            self._graph.merge(obj, name == "__")
            # return graph, not the object in that case
            obj = self._graph
        else:
            self._variables.update({name: obj})

        return obj

    def _parse_row_col_from_ref(self, graph_obj_ref) -> tuple[int, int]:
        """Parse row and column indices from a graph object reference."""
        if not graph_obj_ref:
            return (1, 1)

        if not graph_obj_ref.startswith("$"):
            return math.inf, math.inf

        result = graph_obj_ref[1:].split(":")
        if not result[0]:
            # no row
            return math.inf, math.inf

        row = int(result[0])
        col = int(result[1]) if len(result) == 2 else -1

        return row, col

    def _fetch_from_last_result(self, graph_obj_ref) -> Any:
        """Fetch a graph object from the last query result."""
        if not self._last_result:
            return None

        if isinstance(graph_obj_ref, str) and graph_obj_ref == ".":
            return self._last_result

        row_count, col_count, *_ = utils.resolve_nested_lengths(self.last_result)

        if row_count == 0:
            return None

        row, col = self._parse_row_col_from_ref(graph_obj_ref)

        # if more than one column, column must be explicitly defined $[row]:[col]
        if col == -1:
            if col_count == 1:
                col = 1
            else:
                return None

        if row > row_count or col > col_count:
            return None

        return self._last_result[row - 1][col - 1]

    def _fetch_by_criteria(self, node_ref, graph_obj_type: str | None) -> GraphObject | None:
        """Fetch a graph object by criteria."""
        if criteria := self._node_ref_to_criteria(node_ref):
            if (not graph_obj_type or graph_obj_type == "node") and (result := self.db.fetch_nodes(criteria, unnest_result=True)):
                return result
            if (not graph_obj_type or graph_obj_type == "edge") and (result := self.db.fetch_edges(criteria, unnest_result=True)):
                return result

        return None

    def fetch_graph_obj(self, graph_obj_ref, graph_obj_type: str | None = None) -> GraphObject | None:
        """Fetch a graph object by reference.

        Args:
            graph_obj_ref: Reference to the graph object (variable name, ID, or special syntax)
            graph_obj_type: Optional type filter ("node" or "edge")

        Returns:
            The graph object if found, None otherwise
        """
        # Case 1: Special syntax references ($row:col or .)
        if not graph_obj_ref or isinstance(graph_obj_ref, str) and graph_obj_ref.startswith(("$", ".")):
            return utils.unnest_result(self._fetch_from_last_result(graph_obj_ref), True)

        # Case 2: Try to evaluate as a literal (variable name)
        node_ref, is_literal = utils.try_literal_eval(graph_obj_ref) if isinstance(graph_obj_ref, str) else (graph_obj_ref, False)

        if is_literal:
            # Reference by variable
            return utils.unnest_result(self.get_var(node_ref), True)

        # Case 3: Fetch by ID or GID criteria
        return self._fetch_by_criteria(node_ref, graph_obj_type)

    def fetch_node(self, node_ref: str | int) -> GraphNode | None:
        result = self.fetch_graph_obj(node_ref, "node")

        return result if isinstance(result, GraphNode) else None

    def fetch_edge(self, edge_ref: str | int) -> GraphNode | None:
        result = self.fetch_graph_obj(edge_ref, "edge")

        return result if isinstance(result, GraphEdge) else None

    def _node_ref_to_criteria(self, node_ref):
        if isinstance(node_ref, int):
            # node reference by id
            criteria = MatchCriteria(id_=node_ref)
        elif isinstance(node_ref, str):
            # node reference by gid
            criteria = MatchCriteria(properties_={config.PROP_GID: node_ref})
        else:
            criteria = None

        return criteria

"""cli_graphdb module: CLI interface for graph database operations.

Provides CLIGraphDB class for managing graph database connections and operations
through the command-line interface.
"""

import sys
from typing import Any

import rich
from prompt_toolkit import prompt

import cypher_graphdb.config as config
import cypher_graphdb.utils as utils
from cypher_graphdb import CypherGraphDB, GraphEdge, GraphNode, MatchEdgeCriteria, MatchNodeCriteria, backend_provider
from cypher_graphdb.cli.renderer import ResultRenderer
from cypher_graphdb.settings import get_settings

from .provider import GraphDBProvider


class CLIGraphDB(GraphDBProvider):
    """CLI provider for graph database operations and connection management."""

    def __init__(self, renderer: ResultRenderer, autocommit: bool = True):
        self.db = None
        self._renderer = renderer
        self._autocommit = autocommit
        self._autoconfirm = False

    @property
    def id(self):
        return self.db.id if self.db else ""

    @property
    def graph_name(self):
        return self.db.graph_name if self.db else None

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = value
        if self.db and self.db._backend:
            self.db._backend.autocommit = value

    @property
    def autoconfirm(self):
        return self._autoconfirm

    @autoconfirm.setter
    def autoconfirm(self, value):
        self._autoconfirm = value

    def get_graphs(self) -> tuple[str]:
        return self.db.graphs()

    def connect(self, options, from_prompt: bool = False) -> bool:
        # Get settings and override with CLI options if provided
        settings = get_settings()

        # CLI args take precedence over settings
        backend_type = options.get("backend") or settings.backend
        graph_name = options.get("graph") or settings.graph
        cinfo = options.get("cinfo") or settings.cinfo

        if not backend_type:
            rich.print("[red]Please specify a backend to connect to!", file=sys.stderr)
            return False

        if not (backend := backend_provider.resolve(backend_type)):
            rich.print(f"[red]Invalid backend: {backend_type}")
            return False

        if self.db:
            self.disconnect()
            self.db = None

        # Settings are already loaded from .env file
        self.db = CypherGraphDB(backend=backend)

        # TODO: exception handling
        # set_graph_if_not_exists: prevent from changing current graph to non existing one for the prompt
        try:
            self.db.connect(
                cinfo=cinfo,
                graph_name=graph_name,
                set_graph_if_not_exists=False,
                autocommit=self._autocommit,
            )
        # pylint: disable=W0718
        except Exception as e:
            rich.print("[red]Conection failed")
            rich.print(f"[red]{e.args[0]}")

            self.db = None

            return False

        if from_prompt:
            rich.print(f"[green]Successfully connected to {self.id}")

        return True

    def disconnect(self) -> bool:
        assert self.db
        self.db.disconnect()
        self.db = None

        rich.print("[green]Successfully disconnected")
        return True

    def execute(self, cyper_cmd: str) -> list[tuple[Any, ...]]:
        return self.db.execute(cyper_cmd, unnest_result=False)

    def execute_sql(self, sql_str: str) -> list[tuple[Any, ...]]:
        return self.db.execute_sql(sql_str, unnest_result=False)

    def commit(self) -> bool:
        assert self.db
        self.db.commit()
        return True

    def rollback(self) -> bool:
        assert self.db
        self.db.rollback()
        return True

    def create_graph(self, args) -> bool:
        assert self.db
        if not (graph_name := self._resolve_graph_name(args)):
            return False

        if not self._autoconfirm:
            result = prompt(f"Do you really want to create graph {graph_name} [yN]? ")
            if result.lower() not in ("y", "yes"):
                return False
        else:
            rich.print(f"[blue]Auto-confirming creation of graph {graph_name}")

        # TODO Exception handling
        self.db._backend.create_graph(graph_name)

        rich.print(f"[green]Graph {graph_name} successfully created.")
        return True

    def drop_graph(self, args) -> bool:
        assert self.db
        if not (graph_name := self._resolve_graph_name(args)):
            return False

        if not self._autoconfirm:
            result = prompt(f"Do you really want to drop graph {graph_name} [yN]? ")
            if result.lower() not in ("y", "yes"):
                return False
        else:
            rich.print(f"[blue]Auto-confirming dropping graph {graph_name}")

        # TODO Exception handling
        self.db._backend.drop_graph(graph_name)

        rich.print(f"[green]Graph {graph_name} successfully dropped.")
        return True

    def graph_exists(self, args) -> bool:
        assert self.db
        if not (graph_name := self._resolve_graph_name(args)):
            return False

        if result := self.db._backend.graph_exists(graph_name):
            rich.print(f"[green]Graph {graph_name} exists!")
        else:
            rich.print(f"[yellow]Graph {graph_name} does not exist!")

        return result

    def change_graph(self, args) -> bool:
        if not (graph_name := self._resolve_graph_name(args)):
            return False

        # character sequence of "" and '' are treated as empty strings.
        # TODO check if this should be handled in option parsing.
        graph_name = "" if graph_name in ('""', "''") else graph_name

        if self.db._backend.change_graph_name(graph_name):
            if graph_name:
                rich.print(f"[green]Successfully changed graph to {graph_name}.")
            else:
                rich.print("[green]Successfully reset graph name")
            return True
        else:
            rich.print(f"[red]Graph {graph_name} does not exist!")
            return False

    def fetch_nodes(self, args, kwargs):
        if criteria := self._create_criteria_from_args(MatchNodeCriteria, args, kwargs):
            result = self.db.fetch_nodes(criteria, unnest_result=False)

            # we may get only tuples, so convert it to a List[Tuple[...]]
            return self.db.nest_result(result)
        else:
            return None

    def fetch_edges(self, args, kwargs):
        if "fetch_nodes" in args:
            # alternative for users for fetch_nodes by argument to kwargs
            args = list(args)
            args.remove("fetch_nodes")
            kwargs.update({"fetch_nodes_": True})

        if criteria := self._create_criteria_from_args(MatchEdgeCriteria, args, kwargs):
            result = self.db.fetch_edges(criteria, unnest_result=False)

            # we may get only tuples, so convert it to a List[Tuple[...]]
            return self.db.nest_result(result)
        else:
            return None

    def create_linked_node(
        self, linked_node: GraphNode, direction: str, edge_label: str, args, kwargs
    ) -> tuple[GraphNode, GraphEdge, GraphNode] | None:
        node_fields = self._resolve_fields(GraphNode, args[0], kwargs)
        node = GraphNode(**node_fields)

        edge_fields = self._resolve_fields(GraphEdge, edge_label, {})
        edge = GraphEdge(**edge_fields)

        self._renderer.render([(node, edge, linked_node)], output_format="table")

        if not self._autoconfirm:
            choice = prompt("Do you want to create node and edge [Yn]? ")
            if choice.lower() not in ("", "y", "yes"):
                return None
        else:
            rich.print("[blue]Auto-confirming creation of node and edge")

        # required to create id
        node = self.db.create_or_merge(node)

        if direction == config.TREE_DIRECTION_OUTGOING:
            edge_links = {"start_id_": node.id_, "end_id_": linked_node.id_}
        else:
            edge_links = {"start_id_": linked_node.id_, "end_id_": node.id_}

        edge.__dict__.update(edge_links)
        edge = self.db.create_or_merge(edge)

        return [(node, edge, linked_node)]

    def create_node(self, args, kwargs) -> GraphNode:
        if len(args) != 1:
            return None

        label = args[0]
        fields = self._resolve_fields(GraphNode, label, kwargs)

        node = GraphNode(**fields)
        self._renderer.render([(node,)], output_format="table")

        if not self._autoconfirm:
            choice = prompt("Do you want to create node [Yn]? ")
            if choice.lower() not in ("", "y", "yes"):
                return False
        else:
            rich.print("[blue]Auto-confirming creation of node")

        node = self.db.create_or_merge(node)
        rich.print("\n[green]Successfully created")

        # convert to result set: List[Tuple[]]
        return [(node,)]

    def create_edge(self, start_node: GraphNode, end_node: GraphNode, args, kwargs) -> GraphEdge:
        # <label>
        if len(args) > 1:
            return None

        label = args[0] if len(args) == 1 else None
        fields = self._resolve_fields(GraphEdge, label, kwargs)

        # update start and end node id
        fields.update({"start_id_": start_node.id_, "end_id_": end_node.id_})
        edge = GraphEdge(**fields)

        self._renderer.render(
            [
                (
                    start_node,
                    edge,
                    end_node,
                )
            ],
            output_format="table",
        )

        if not self._autoconfirm:
            choice = prompt("Do you want to create edge [Yn]? ")
            if choice.lower() not in ("", "y", "yes"):
                return False
        else:
            rich.print("[blue]Auto-confirming creation of edge")

        edge = self.db.create_or_merge(edge)

        # convert to result set: List[Tuple[]]
        return [(edge,)]

    def update_graph_obj(self, graphobj, args, kwargs) -> GraphNode | GraphEdge:
        if len(args) > 1:
            return None

        _, props = utils.slice_model_properties(GraphNode, kwargs)
        if props:
            utils.resolve_properties(props)
            graphobj.properties_.update(props)

            self._renderer.render([(graphobj,)], output_format="table")

            if not self._autoconfirm:
                choice = prompt("Do you want to update node [Yn]? ")
                if choice.lower() not in ("", "y", "yes"):
                    return None
            else:
                rich.print("[blue]Auto-confirming update of node")

            graphobj = self.db.create_or_merge(graphobj)

            # return as result set List[Tuple[]]
            return [(graphobj,)]
        else:
            return None

    def delete_graph_obj(self, graphobj, args, kwargs) -> bool:
        if len(args) > 1:
            return False

        detach = kwargs.get("detach", True)

        self._renderer.render([(graphobj,)], output_format="table")

        if not self._autoconfirm:
            result = prompt("Do you want to delete graph object [yN]? ")
            if result.lower() not in ("y", "yes"):
                return False
        else:
            rich.print("[blue]Auto-confirming deletion of graph object")

        self.db.delete(graphobj, detach)

        rich.print("[green]Sucessfully deleted.")

        return True

    def _resolve_fields(self, cls, label, kwargs):
        fields, props = utils.slice_model_properties(cls, kwargs)

        if config.PROP_LABEL not in fields:
            fields.update({config.PROP_LABEL: label})

        if props:
            utils.resolve_properties(props)
            fields.update({"properties_": props})

        return fields

    def _create_criteria_from_args(self, cls, args, kwargs) -> bool:
        label = None

        if args:
            # first argument is either label, id_ or gid_
            value, is_literal = utils.try_literal_eval(args[0])

            if is_literal:
                label = utils.str_to_collection(value)
            else:
                if isinstance(value, int):
                    kwargs.update({config.PROP_ID: value})
                elif isinstance(value, str):
                    kwargs.update({config.PROP_GID: value})
                else:
                    rich.print(f"[red]Invalid argument {value}", file=sys.stderr)
                    return None

        if not args and not kwargs:
            return None

        fields = self._resolve_fields(cls, label, kwargs)

        return cls(**fields)

    def _resolve_graph_name(self, args):
        if len(args) != 1:
            rich.print(
                "[red]Invalid arguments. Valid option(s) are <graph_name>",
                file=sys.stderr,
            )
            return None

        return args[0]

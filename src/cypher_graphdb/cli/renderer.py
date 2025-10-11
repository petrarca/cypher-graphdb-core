"""cli_renderer module: Result rendering for CLI output.

Provides ResultRenderer class for formatting and displaying query results
in various formats including tables, JSON, trees, and ASCII art.
"""

import json
from typing import Any

import rich
from pydantic import BaseModel
from rich import box
from rich.json import JSON, JSONHighlighter
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

import cypher_graphdb.config as config
import cypher_graphdb.options as options
import cypher_graphdb.utils as utils
from cypher_graphdb import Graph, GraphEdge, GraphJSONEncoder, GraphNode, GraphPath


class ResultRenderer:
    """Handles rendering of query results in various output formats."""

    def __init__(self, default_format: str = None) -> None:
        # When True, all render operations become no-ops. This allows the
        # caller (CLI) to suppress output (e.g. for script / batch execution)
        # without changing call sites throughout the code base.
        self.suppress_output: bool = False

        self.default_format = default_format if default_format is not None else "json"

    def render(self, result, output_format: str = None, args=None, kwargs=None):
        if self.suppress_output:
            return
        args = args or []
        kwargs = kwargs or {}

        output_format = output_format if output_format is not None else self.default_format

        match output_format:
            case "json":
                self.render_as_json(result)
            case "table":
                self.render_as_table(result, kwargs)
            case "list":
                self.render_as_tree_list(result, args, kwargs)
            case _:
                rich.print(f"[red]Invalid rendering format: {output_format}")

    def _resolve_col_title(self, probe: Any) -> str:
        """Determine the column title based on the data type."""
        if isinstance(probe, GraphNode | GraphEdge):
            return probe.label_
        if isinstance(probe, GraphPath):
            return "Path"
        if isinstance(probe, Graph):
            return "Graph"
        if isinstance(probe, list):
            return "Edges (Hops)"

        return "Value"

    def _is_graph_obj(self, value) -> bool:
        """Check if the value is a graph object."""
        return isinstance(value, GraphNode | GraphEdge | GraphPath)

    def _renderable_cell_value(self, value: Any, with_type) -> Text | JSON:
        """Convert a cell value to a renderable format."""
        if isinstance(value, BaseModel):
            return JSON(value.model_dump_json(indent=2, context={"with_type": with_type}))

        if value is None:
            return Text("Null")

        if utils.is_scalar_type(value):
            return Text(str(value))

        return JSON(json.dumps(utils.to_collection(value), indent=2))

    def _prepare_table_data(self, result):
        """Prepare data for table rendering."""
        if isinstance(result, dict):
            # row labels are dictionary keys
            row_labels = list(result)
            # transform them into internal representations [(v,...) ]
            data = [(v,) for v in result.values()]
        else:
            row_labels = None
            # Handle scalar values by wrapping them in a list structure
            is_iterable = hasattr(result, "__iter__") and not isinstance(result, str | bytes)
            data = result if is_iterable else [(result,)]

        if isinstance(result, Graph):
            nodes = [(node,) for node in result.nodes]
            edges = [(edge,) for edge in result.edges]
            data = nodes + edges
            is_graph = True
        else:
            is_graph = False

        return data, row_labels, is_graph

    def _add_table_columns(self, table, data, row_labels, is_graph, col_count, opts, value_header, contains_dict):
        """Add columns to the table based on data structure."""
        if is_graph:
            # graph has only one column
            table.add_column(self._resolve_col_title(data[0][0]))
            return

        if not row_labels:
            if contains_dict:
                for key in data[0]:
                    table.add_column(key, style="green")
            else:
                # Handle wildcard case (col_headers is None)
                if opts.col_headers is None:
                    # Auto-detect columns, then append explicit ones
                    col_headers = []
                    wildcard_cols = col_count - len(opts.explicit_cols)
                    for i in range(wildcard_cols):
                        probe = data[0] if utils.is_scalar_type(data[0]) else data[0][i]
                        col_headers.append(self._resolve_col_title(probe))
                    # Append explicit columns after wildcard
                    col_headers.extend(opts.explicit_cols)
                else:
                    # Normal case - use provided headers, auto-detect rest
                    col_headers = opts.col_headers.copy()
                    for i in range(len(col_headers), col_count):
                        probe = data[0] if utils.is_scalar_type(data[0]) else data[0][i]
                        col_headers.append(self._resolve_col_title(probe))

                for col_title in col_headers:
                    table.add_column(col_title, style=opts.col_style)
        else:
            # rendering dictionaries, value column
            table.add_column(value_header, style="green")

    def _prepare_row_cells(self, row_data, col_count, is_graph, contains_dict):
        """Prepare cells for a table row."""
        if contains_dict:
            return [str(v) for v in row_data.values()]

        if utils.is_scalar_type(row_data):
            return [row_data]

        return [self._renderable_cell_value(row_data[j], is_graph) for j in range(col_count)]

    def render_as_table(self, result, kwargs):
        """Render results as a formatted table."""
        if self.suppress_output:
            return
        if not result:
            self.render_no_result(False)
            return

        opts = RenderTableOpts.from_opts(None, kwargs)
        data, row_labels, is_graph = self._prepare_table_data(result)

        # assume that result is not unnested
        row_count = len(data)

        if row_count == 0:
            self.render_no_result(is_graph)
            return

        col_count = 1 if utils.is_scalar_type(data[0]) or isinstance(data[0], dict) else len(data[0])
        contains_dict = isinstance(data[0], dict)

        # variables
        assert col_count == 1 if row_labels else True
        table = Table(show_header=opts.show_header, show_lines=True, box=box.SQUARE)

        # Set up the index/key column and determine value header
        if row_labels:
            table.add_column(opts.key_name, style="bright_blue")
            value_header = "Graph Object" if self._is_graph_obj(data[0][0]) else opts.value_name
        else:
            table.add_column("#")
            value_header = None

        # Add data columns
        self._add_table_columns(table, data, row_labels, is_graph, col_count, opts, value_header, contains_dict)

        # Add rows
        for i in range(row_count):
            cells = self._prepare_row_cells(data[i], col_count, is_graph, contains_dict)

            # Add index/key as first column
            cells.insert(0, row_labels[i] if row_labels else str(i + 1))

            # Add the row to the table
            table.add_row(*cells)

        rich.print(table)

    def render_as_json(self, result):
        if self.suppress_output:
            return
        if result:
            rich.print_json(self.to_json(result))
        else:
            self.render_no_result(False)

    def render_no_result(self, is_graph):
        if self.suppress_output:
            return
        rich.print(f"[yellow]{'Graph is empty!' if is_graph else 'No result!'}")

    def render_as_tree(self, result, args, _) -> None:
        if self.suppress_output:
            return

        def walk_tree(items, parent):
            nonlocal left_arrow, right_arrow, json_highlighter, opts

            for item in items:
                # graph node
                node: GraphNode = item[0]
                # in childs, reference to edge
                edge: GraphEdge = item[1]
                # child nodes
                childs = item[2]

                text_caption = Text()
                # edge
                text_caption.append(self._edge_to_text(edge, opts, json_highlighter, left_arrow, right_arrow))
                # node
                text_caption.append(self._node_to_text(node, opts, json_highlighter))

                child_tree = parent.add(text_caption)

                if childs:
                    walk_tree(childs, child_tree)

        if not result:
            self.render_no_result(False)
            return

        opts = RenderTreeOpts.from_opts(args, None)
        left_arrow, right_arrow = {"outgoing": ("", "->"), "incoming": ("<-", "")}[opts.direction]

        json_highlighter = JSONHighlighter()
        tree = Tree("Result")

        # Every node are tuppled as (GraphNode, GraphEdge, (child, ...))
        walk_tree(result, tree)

        rich.print("")
        rich.print(tree)
        rich.print("")

    def render_as_tree_list(self, result, args=None, kwargs=None) -> None:
        if self.suppress_output:
            return

        if not result:
            self.render_no_result(False)
            return

        opts = RenderTreeOpts.from_opts(args, kwargs)
        opts.with_edge_props = True

        json_highlighter = JSONHighlighter()
        tree = Tree(opts.root_label)

        self._walk_tree_list(result, tree, opts, json_highlighter)

        rich.print("")
        rich.print(tree)
        rich.print("")

    def _walk_tree_list(self, item, parent, opts, json_highlighter) -> None:
        """Helper method to walk through tree items for tree list rendering."""
        if item is None:
            return parent.add("None")

        if isinstance(item, GraphNode):
            return parent.add(self._node_to_text(item, opts, json_highlighter))
        if isinstance(item, GraphEdge):
            return parent.add(self._edge_to_text(item, opts, json_highlighter))

        if isinstance(item, Graph):
            tree_node = parent.add("Graph")
        elif isinstance(item, GraphPath):
            tree_node = parent.add("Path")
        elif isinstance(item, list | tuple | dict):
            tree_node = parent.add("+") if len(item) > 1 else parent
        else:
            # value
            return parent.add(str(item))

        for val in item:
            # handle dict
            self._walk_tree_list(val, tree_node, opts, json_highlighter)

    def to_json(self, result, indent=None):
        return json.dumps(result, cls=GraphJSONEncoder, indent=indent)

    def _edge_to_text(
        self,
        edge: GraphEdge,
        opts: type["RenderTreeOpts"],
        json_highlighter,
        left_arrow: str = "",
        right_arrow: str = "",
    ) -> Text:
        if not edge:
            return Text("")

        props = edge.properties_.copy()
        if not opts.with_edge_gid:
            props.pop(config.PROP_GID)

        if opts.with_edge_props:
            props_text = Text(f" {json.dumps(props)}")
            json_highlighter.highlight(props_text)
        else:
            props_text = Text("")

        edge_id = f" {edge.id_}" if edge and opts.with_edge_id else ""
        edge_gid = f" {edge.gid_}" if edge and opts.with_edge_gid else ""

        return Text(
            f"{left_arrow}[:{edge.label_}{edge_id}{props_text}{edge_gid}]{right_arrow}" if edge else "",
            "magenta",
        )

    def _node_to_text(self, node: GraphNode, opts: type["RenderTreeOpts"], json_highlighter: JSONHighlighter) -> Text:
        props = node.properties_.copy()
        if not opts.with_gid:
            props.pop(config.PROP_GID)

        props_text = Text(json.dumps(props))
        json_highlighter.highlight(props_text)

        result = Text()

        # label of node
        result.append(f" {node.label_} ", "yellow")
        # props of node
        result.append(props_text)

        if opts.with_id:
            result.append(f" {node.id_}")

        return result


class RenderTableOpts(options.TypedOptionModel):
    """Options for table rendering format."""

    show_header: bool = True
    key_name: str = "Variable"
    value_name: str = "Value"
    col_headers: list[str] | None = []
    explicit_cols: list[str] = []
    col_style: str = None


class RenderTreeOpts(options.TypedOptionModel):
    """Options for tree rendering format."""

    with_id: bool = False
    with_gid: bool = False
    with_edge_id: bool = False
    with_edge_gid: bool = False
    with_edge_props: bool = False
    with_ids: bool = False
    with_gids: bool = False
    with_all: bool = False
    root_label: str = "Result"
    direction: str = config.DEFAULT_TREE_DIRECTION

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)

        if self.with_all:
            self.set_all(True)
        else:
            if self.with_ids:
                self.with_id = True
                self.with_edge_id = True

            if self.with_gids:
                self.with_gid = True
                self.with_edge_gid = True

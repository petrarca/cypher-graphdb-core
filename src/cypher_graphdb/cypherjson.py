"""JSON encoding module: Custom JSON encoder for graph objects.

This module provides the GraphJSONEncoder class for serializing graph
objects (nodes, edges, paths, and graphs) to JSON format with optional
type information.
"""

import json
from typing import Any

from . import utils
from .models import Graph, GraphEdge, GraphNode, GraphPath


class GraphJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for graph objects.

    Extends the standard JSONEncoder to handle graph objects like nodes,
    edges, paths, and entire graphs, with optional type metadata.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the graph JSON encoder.

        Args:
            *args: Additional arguments for JSONEncoder.
            **kwargs: Additional keyword arguments for JSONEncoder.
                with_type: Whether to include type information in output.
        """
        self.with_type = kwargs.pop("with_type", True)

        super().__init__(*args, **kwargs)

    def encode(self, o: Any) -> str:
        """Encode graph objects to JSON format.

        Args:
            o: Object to encode (graph node, edge, path, or graph).

        Returns:
            JSON string representation of the object.
        """
        if isinstance(o, GraphNode | GraphEdge | GraphPath | Graph):
            return o.model_dump_json(indent=self.indent, context={"with_type": self.with_type})

        if utils.is_scalar_type(o) or o is None:
            return super().encode({"val": o})
        else:
            return super().encode(utils.to_collection(o))

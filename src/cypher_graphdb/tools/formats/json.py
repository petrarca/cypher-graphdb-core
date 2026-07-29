"""JSON format registration (stdlib only)."""

from cypher_graphdb.formatprovider import data_format
from cypher_graphdb.tools.hierarchical_exporter import HierarchicalExporter
from cypher_graphdb.tools.json_importer import JsonImporter


@data_format("json", extensions=(".json",))
class JsonFormat:
    """Hierarchical JSON, supporting round-trip export/import."""

    importer = JsonImporter
    exporter = HierarchicalExporter

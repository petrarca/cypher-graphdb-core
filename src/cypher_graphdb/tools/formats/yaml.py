"""YAML format registration (PyYAML is a core dependency)."""

from cypher_graphdb.formatprovider import data_format
from cypher_graphdb.tools.hierarchical_exporter import HierarchicalExporter
from cypher_graphdb.tools.yaml_importer import YamlImporter


@data_format("yaml", extensions=(".yaml", ".yml"))
class YamlFormat:
    """Hierarchical YAML, supporting round-trip export/import."""

    importer = YamlImporter
    exporter = HierarchicalExporter

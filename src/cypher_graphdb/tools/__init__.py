"""Tools package: Import and export utilities for graph data.

Standard library based streaming importers and exporters, with JSON/YAML
hierarchical processing. Excel support needs openpyxl, which lives in the
``excel`` extra.

The Excel symbols are re-exported **lazily** (PEP 562) so that importing this
package -- or using the CSV/JSON/YAML paths -- does not require openpyxl. Prefer
resolving a format through ``cypher_graphdb.formatprovider`` over importing an
importer/exporter class directly.
"""

from cypher_graphdb._extras import require_extra

from .csv_exporter import CsvExporter
from .csv_importer import CsvImporter
from .csv_source import CsvSource
from .data_flattener import DataFlattener, FlattenedNode
from .file_exporter import FileExporterOptions
from .hierarchical_exporter import HierarchicalExporter
from .hierarchical_importer import HierarchicalImporter
from .hierarchical_row_source import HierarchicalRowSource, RelationRowSource
from .json_importer import JsonImporter
from .json_yaml_data_source import JsonYamlDataSource
from .row_collector import RowCollector
from .row_set import RowSet
from .row_source import RowSource
from .tabular_importer import TabularImporter
from .yaml_importer import YamlImporter

# name -> module providing it, for the openpyxl-backed symbols
_EXCEL_EXPORTS = {
    "ExcelImporter": ".excel_importer",
    "ExcelExporter": ".excel_exporter",
    "ExcelRowSource": ".excel_row_source",
}

__all__ = [
    "ExcelImporter",
    "ExcelExporter",
    "CsvImporter",
    "CsvExporter",
    "JsonImporter",
    "YamlImporter",
    "HierarchicalExporter",
    "FileExporterOptions",
    # New tabular abstractions
    "TabularImporter",
    "RowSource",
    "CsvSource",
    "ExcelRowSource",
    "RowSet",
    "RowCollector",
    # Hierarchical processing
    "HierarchicalImporter",
    "HierarchicalRowSource",
    "RelationRowSource",
    "DataFlattener",
    "FlattenedNode",
    "JsonYamlDataSource",
]


def __getattr__(name: str) -> object:
    """Resolve the openpyxl-backed symbols on first access (PEP 562).

    Args:
        name: Attribute being looked up on the package.

    Returns:
        The requested attribute.

    Raises:
        ModuleNotFoundError: If the ``excel`` extra is not installed.
        AttributeError: If the attribute is not part of the public surface.
    """
    module_name = _EXCEL_EXPORTS.get(name)
    if module_name is not None:
        import importlib

        try:
            module = importlib.import_module(module_name, __name__)
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on install shape
            if exc.name == "openpyxl":
                raise require_extra("excel", exc) from exc
            raise
        return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

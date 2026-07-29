---
type: Guide
title: Import, export and custom formats
description: Moving graph data in and out as CSV, Excel, JSON or YAML, checking what your install supports, and registering a format of your own.
tags: [cypher-graphdb, import, export, formats, registry, excel, csv]
status: stable
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Import, export and custom formats

## Built-in formats

| Format | Extensions | Needs | Import | Export |
|---|---|---|---|---|
| `csv` | `.csv` | — | yes | yes |
| `json` | `.json` | — | yes | yes |
| `yaml` | `.yaml`, `.yml` | — | yes | yes |
| `excel` | `.xlsx`, `.xls` | `[excel]` extra | yes | yes |

CSV, JSON and YAML need nothing beyond the core install. Excel needs `openpyxl`:

```bash
pip install 'cypher-graphdb[excel]'   # or [cli], which includes it
```

JSON and YAML use the hierarchical round-trip format, which deduplicates nodes
by `gid_` so an export can be re-imported without creating duplicates.

## From the CLI

```bash
# import reads straight into the graph
cypher-graphdb --graph mygraph --yes -e "import data.csv"

# export writes the IN-MEMORY graph, so populate it first with `add`
cypher-graphdb --graph mygraph --yes -e "MATCH (n:Person) RETURN n | add; export data.xlsx"

# a tree export feeds the pipeline directly
cypher-graphdb --graph mygraph --yes -e "MATCH p=(:Person)-[:WORKS_FOR]->(:Company) RETURN p | tree | export tree.json"
```

The format is taken from the file extension. Exporting without populating the
in-memory graph reports `Empty graph could not be exported` — that is the
missing `add`, not a broken format. See [cli.md](cli.md).

## Asking what this install supports

Formats live in a registry. Naming the format of a file is **metadata** — it
never imports anything and never fails:

```python
from cypher_graphdb import format_provider

format_provider.format_name_for_file("report.xlsx")   # "excel", even without [excel]
format_provider.format_name_for_file("notes.txt")     # None
```

To find out whether a format is actually *usable*, probe it. This never raises,
so it is safe for listing and for choosing a fallback:

```python
result = format_provider.probe("excel")

if not result:
    print(result.reason)
    # Format 'excel' needs the 'excel' extra, which is not installed (missing: openpyxl).
    print(result.extra)            # "excel"
    print(result.missing_package)  # "openpyxl"
```

`probe()` distinguishes two different failures, which matters when reporting to
a user:

| | `registered` | `available` | Meaning |
|---|---|---|---|
| `excel` without the extra | True | False | known format, dependency missing |
| `parquet` | False | False | nobody registered it |

`probe_all()` returns the same for every known format — the basis for a
"what can this installation read and write?" listing.

Both `FormatSpec` and `FormatAvailability` are Pydantic models, so
`model_dump()` gives you something JSON-serializable (importer and exporter
render as class names).

## Getting the implementation

`resolve()` is the throwing counterpart — use it when you actually intend to
read or write:

```python
spec = format_provider.resolve("csv")
importer = spec.importer(db)
exporter = spec.exporter(db, options)
```

It raises `ValueError` for an unknown format, and `ModuleNotFoundError` naming
the extra when the dependency is missing. Check `supports_import()` /
`supports_export()` before assuming both directions exist.

## Registering your own format

Formats are global — `"csv"` means the same thing everywhere — so registration
goes on the shared provider, exactly as [backends](../design/index.md) do. (Graph
*models* are different: they are per-graph and use an explicit `ModelProvider`;
see [typed-models.md](typed-models.md).)

```python
from cypher_graphdb import data_format
from cypher_graphdb.tools import BaseExporter, BaseImporter

class ParquetImporter(BaseImporter): ...
class ParquetExporter(BaseExporter): ...

@data_format("parquet", extensions=(".parquet",))
class ParquetFormat:
    importer = ParquetImporter
    exporter = ParquetExporter
```

From then on `parquet` resolves by name, `.parquet` files resolve by extension,
and the CLI's `import graph` / `export graph` accept it without changes.

A format may be one-directional — supply only `importer` or only `exporter`.

If your format depends on an optional package of your own, declare it with
`extra=` so `probe()` can report it the same way the built-ins do:

```python
@data_format("parquet", extensions=(".parquet",), extra="parquet")
class ParquetFormat: ...
```

Re-registering an existing name replaces it, so you can override a built-in with
your own implementation. The previous format's extensions are released, so no
stale mapping is left behind.

### Keeping it out of the global registry

For tests, or to avoid touching global state, construct your own provider and
pass it explicitly:

```python
from cypher_graphdb import FormatProvider

provider = FormatProvider()

@data_format("parquet", extensions=(".parquet",), provider=provider)
class ParquetFormat: ...
```

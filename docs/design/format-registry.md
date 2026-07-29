---
type: Concept
title: The format registry
description: Why import/export formats are resolved through a global registry, how metadata, availability and resolution are deliberately separated, and how a consumer registers its own format.
tags: [cypher-graphdb, formats, registry, import, export, extensibility, extras]
status: stable
implementation: implemented
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# The format registry

> **Built.** `formatprovider.py` (`FormatProvider`, `FormatSpec`,
> `FormatAvailability`, `data_format`); built-in formats register from
> `tools/formats/<name>.py`.

Usage in [import-export.md](../usage/import-export.md).

## Problem

Format selection was hardcoded in three places that could disagree:

- a `match` statement in `cli/exporter.py`,
- a dict in `cli/importer.py`,
- an extension→name map in `utils.resolve_fileformat`.

Adding a format meant editing all three, and a consumer could not add one at
all. The third map had already drifted: it claimed `.xml` support that no
importer or exporter ever provided.

Splitting Excel into an optional extra made this worse. Once a format's
dependency can be absent, "which format is this file?" and "can I actually use
that format?" become different questions, and code that conflated them would
either import `openpyxl` just to name a file's format, or crash while listing
what is available.

## Design

### One registry, keyed by name and by extension

`FormatProvider` mirrors `BackendProvider`: a process-global registry with a
lock, and lazy loading by convention from `cypher_graphdb.tools.formats.<name>`.
A `FormatSpec` carries the name, its extensions, an optional importer, an
optional exporter, and the extra its dependency lives in.

A format may be one-directional — JSON originally had an importer and no
exporter — so `supports_import()` and `supports_export()` are separate.

### Why global, when models are not

Graph *models* deliberately abandoned a global registry
([ADR-0001](../adr/0001-explicit-model-providers.md)) because two graphs
legitimately hold different classes under the same label, and a global registry
let one silently overwrite the other.

Formats do not have that property. `"csv"` means the same thing in every graph
and every connection, exactly as a backend name does. The collision hazard that
justified per-graph model providers does not exist here, so the registry follows
`BackendProvider` rather than `ModelProvider`. A `FormatProvider` can still be
constructed explicitly — tests do — but the global one is the default.

### Three questions, three answers

The distinction is the point of the design, and each level does strictly less
than the one below it:

| Call | Imports? | Raises? | Answers |
|---|---|---|---|
| `format_name_for_file(f)` | never | never | "what format is this file?" |
| `probe(name)` | tries | never | "can I use it, and if not why?" |
| `resolve(name)` | yes | yes | "give me the implementation" |

`format_name_for_file` is metadata. Naming the format of `report.xlsx` must work
on an installation without `openpyxl`, so it answers from a static extension
table rather than by importing the format module.

`probe` returns a `FormatAvailability` that separates two failures a caller must
report differently:

| | `registered` | `available` |
|---|---|---|
| `excel` without the extra | True | False |
| `parquet` (nobody registered it) | False | False |

and carries `reason`, `extra` and `missing_package` so the caller can tell the
user what to install. `probe_all()` is the basis of a "what can this
installation read and write?" listing.

`resolve` is the throwing counterpart, for when the caller intends to read or
write: `ValueError` for an unknown format, `ModuleNotFoundError` naming the
extra when a dependency is missing.

### Serializable by construction

`FormatSpec` and `FormatAvailability` are Pydantic models, per the house
standard. `FormatSpec` serializes its importer and exporter as class names,
since a class object does not survive a serialization boundary.

### Registration and override

`@data_format(name, extensions=..., extra=...)` registers a class exposing
`importer` and/or `exporter`. Registration is an **import side effect**, exactly
as for backends: a consumer's format is registered when its module is imported.
There is no entry-point discovery, so the CLI only knows the built-ins — no
consumer has needed otherwise.

Re-registering a name **replaces** it, which lets a consumer override a built-in.
Replacement is wholesale: an override supplying only an `importer` leaves the
format export-less. Merging was rejected because it makes "what am I actually
running?" harder to answer than a clean replacement. The previous registration's
extensions are released, so no stale extension mapping survives.

## Consequences

Adding a format is one module, not three edits. `resolve_fileformat` now answers
from the registry, so the phantom `.xml` mapping disappeared. Excel could move
behind an extra without the CSV/JSON/YAML paths paying for it.

The cost is a global mutable registry, with the import-order sensitivity that
implies — mitigated by lazy loading, which means the built-ins register on first
use rather than depending on someone having imported them.

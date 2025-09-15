# DuckDB Migration: Replacing pandas Import/Export

## Goal
Single implementation for all tabular import/export using DuckDB + Arrow (and openpyxl for Excel I/O only). We REMOVE pandas completely (including previous `DataFrameBuilder` / `DataFrameImporter`) in one step since there are no external users depending on them. Gains: lower memory, faster CSV parsing, unified batching, simpler maintenance, foundation for Parquet + bulk UNWIND.

## Scope (Phase 1 – Single Cutover)
- Remove pandas dependency entirely.
- Delete `DataFrameBuilder` and `DataFrameImporter` modules.
- Introduce unified tabular abstraction (`RowSource`, `DuckDBSource`, `ExcelRowSource`).
- Keep existing class names for importers/exporters (Csv/Excel) so CLI invocations remain unchanged.
- Batch graph writes & pre-resolve edge references.
- Standardize `properties_` column to JSON; eliminate `ast.literal_eval` usage.
- Replace exporter callback DataFrame argument with a lightweight `RowSet` summary object (supports `__len__` and `columns`).

## Non-Goals (Deferred)
- Parquet export/import (Phase 2).
- Bulk UNWIND Cypher generation (Phase 2 optimization).
- Advanced parallel export/import.

## Rationale
| Issue | Current pandas Path | DuckDB Approach |
|-------|---------------------|-----------------| 
| Memory | Full DataFrame materialization | Streaming Arrow batches | 
| CSV Parsing | Single-threaded mostly | Parallel, type inference | 
| Row Iteration | Python per row | Batch fetch → lower overhead | 
| Edge Ref Resolution | Per-row fetch_nodes() | Pre-batch map join | 
| Properties Parsing | `ast.literal_eval` + NaN checks | Valid JSON + `json.loads` | 
| Future Parquet | Extra lib setup | Native COPY TO parquet | 

## High-Level Architecture
```
CsvImporter / ExcelImporter
  ↓ (RowSource: DuckDBSource | ExcelRowSource)
TabularImporter
  - analyze_columns(columns)
  - iterate batches source.iter_batches(BATCH)
  - node / edge construction
  - batched id resolution (edges)
  - commit_every N
```

### New Components
- `RowSource` (protocol / ABC)
  - `columns() -> list[str]`
  - `iter_batches(batch_size:int) -> Iterator[list[dict]]`
- `DuckDBSource(RowSource)`
  - CSV ingestion via `read_csv_auto` (parallel, type inference)
  - Optionally register Arrow Tables
- `ExcelRowSource(RowSource)`
  - Uses openpyxl to stream rows (no pandas)
  - Yields dict rows consistent with CSV columns
- `RowSet`
  - Lightweight object passed to exporter callbacks (len + columns)
  - Avoids materializing large datasets as DataFrame

### Import Flow (CSV & Excel)
1. Importer validates extension and builds appropriate RowSource.
2. RowSource exposes column names; TabularImporter analyzes (labels, gid, edge refs).
3. Pre-resolve edge references (BY_GID / BY_KEY) via distinct queries (DuckDB or in-memory for Excel) + single Cypher lookup.
4. Iterate batches: build nodes/edges, normalize JSON properties.
5. Commit every `COMMIT_EVERY` entities.
6. Collate errors into list (not DataFrame).

### Edge Reference Pre-Resolution
- BY_GID: DuckDB query: `SELECT DISTINCT start_gid_ FROM rel UNION SELECT DISTINCT end_gid_ FROM rel`.
- Fetch all gids in one Cypher query (UNWIND) to id map.
- BY_KEY: JSON parse key columns; build set of (label, key_props) combinations; resolve via batched Cypher.

### Export Flow (CSV & Excel)
1. Group entities by label.
2. Convert each group to list[dict] (RowCollector).
3. CSV: Register Arrow table or in-memory data as DuckDB relation and `COPY TO` CSV.
4. Excel: Create workbook (openpyxl); write header + rows streaming; edges prefixed/suffixed per config.
5. Invoke callback with `RowSet(count, columns)` for status reporting.

## Data Schema Adjustments
- `properties_` serialized as valid JSON string.
- Importer parses JSON only. (Legacy python-dict string format removed.)

## Configuration
Constants (add to `config.py`):
- `IMPORT_BATCH_SIZE = 1000`
- `COMMIT_EVERY = 5000` (defaults to `IMPORT_BATCH_SIZE` if None)
- (No feature flag needed: DuckDB path is default.)

## Error Handling
- Maintain `import_errors: list[dict]` with original row + `error_` message.
- Optional helper to expose errors as DuckDB relation (future).

## Step-by-Step Implementation Plan
1. Add dependencies: `duckdb`, `pyarrow` (optional but recommended), `openpyxl`.
2. Create `row_source.py` (RowSource, DuckDBSource, ExcelRowSource, RowSet, RowCollector).
3. Implement `tabular_importer.py` (replaces DataFrameImporter) using RowSource.
4. Refactor `csv_importer.py` & `excel_importer.py` to construct RowSource and call TabularImporter.
5. (DONE) Removed `df_importer.py` & `df_builder.py` and updated `tools/__init__.py` exports.
6. Implement edge reference pre-resolution utilities (gid/key batch lookup).
7. Implement exporters: CSV (DuckDB COPY), Excel (openpyxl streaming). Replace DataFrameBuilder usage.
8. Switch exporter callback to `RowSet`.
9. JSON properties serialization/deserialization.
10. (DONE) Confirm pandas dependency removed from `pyproject.toml` (not listed).
11. Write tests: column analysis, JSON conversion, gid batching, round-trip graph equivalence.
12. Update docs (this file, changelog, README) to reflect new design.

## Future Work (Phase 2+)
- Parquet export/import (`COPY TO 'file.parquet' FORMAT PARQUET`).
- Bulk UNWIND insertion for batch creates/merges.
- Parallel label group export/import.
- Expose pre-import SQL filtering capability.

## Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| Ordering differences vs prior DataFrame order | Treat ordering as undefined; compare sets in tests. |
| Excel performance vs pandas | Accept slight slowdown; optimize later with batch row buffering. |
| Large JSON properties performance | Consider Arrow struct columns / partial projection later. |
| Added dependencies | Keep list minimal; pyarrow optional. |

## Acceptance Criteria
- No pandas dependency in the project (confirmed; legacy pandas modules deleted).
- CSV & Excel import/export function via new pipeline.
- Round-trip (export→import→export) preserves graph semantics (labels, properties, edges).
- Callback receives `RowSet` and CLI displays counts correctly.
- JSON properties present & parsed (no `ast.literal_eval`).
- Edge reference pre-resolution reduces per-edge lookups (validated in test or benchmark log).

## Open Questions
- Pre-import SQL filtering exposure timing?
- When to introduce Parquet as default for large exports?
- Bulk UNWIND insertion priority vs Parquet?

## Migration Notes
No external users: internal refactor only. Old pandas-specific modules removed; callbacks now receive `RowSet`.

---
Updated on: 2025-09-04.

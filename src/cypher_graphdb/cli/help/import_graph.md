# Import Graph

**Command:** `import` | `import graph`

**Description:** Import graph data from various file formats into the current graph database.

## Syntax
```
import <source>[,format=<format>][,option=value,...]
```

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source` | string | Yes | File path, directory path, or glob pattern for source data |
| `format` | string | No | Explicit format specification: `csv` or `excel` (auto-detected if omitted) |

## Supported Formats

- **Excel** (`.xlsx`, `.xls`) - Default format
- **CSV** (`.csv`) - Comma-separated values

## Format Auto-Detection

The import command automatically detects file formats based on:
1. File extension (`.xlsx`, `.xls`, `.csv`)
2. Glob pattern matching (`*.csv`, `*.xlsx`)
3. Directory scanning for supported file types

If auto-detection fails, explicitly specify the format using `format=<format>`.

## Options

Currently, the import command supports the following options:

- **format**: Explicitly specify the import format (`csv` or `excel`)

### Current Limitations

The CSV and Excel importers currently use default settings:

**CSV Import:**
- Always expects headers in the first row
- Uses comma (`,`) as delimiter
- Uses UTF-8 encoding
- Uses standard library CSV parser with auto-detection for data types

**Excel Import:**
- Processes all worksheets in the file
- Always expects headers in the first row  
- Worksheets starting with `_` are treated as edge data

## Examples

### Basic Import
```bash
# Import single Excel file
import data.xlsx

# Import single CSV file  
import data.csv

# Import all CSV files in directory
import ./data/*.csv

# Import specific directory
import ./data/
```

### Format Specification
```bash
# Force CSV format
import data.txt,format=csv

# Force Excel format  
import data.data,format=excel
```

### Basic Import Examples
```bash
# CSV files (with headers expected)
import data.csv

# Excel files (all sheets processed)
import workbook.xlsx

# Multiple files using glob patterns
import ./data/*.csv
import ./data/*.xlsx
```

## Data Format Requirements

### CSV Files
- **Headers required**: First row must contain column names
- **Delimiter**: Comma (`,`) - currently not configurable
- **Encoding**: UTF-8 - currently not configurable  
- **Node data**: Any CSV with columns representing node properties
- **Edge data**: CSV files with edge reference columns (see below)

### Excel Files
- **Headers required**: First row of each sheet must contain column names
- **Multiple sheets**: All worksheets in the file are processed
- **Sheet naming**: Sheets starting with `_` are treated as edge data
- **Node data**: Regular worksheets with columns representing node properties
- **Edge data**: Worksheets with edge reference columns

### Edge Reference Columns
For importing edges, include reference columns to specify node connections:
- `start_gid_` and `end_gid_`: Reference nodes by their GID
- `start_label_` and `end_label_`: Specify node labels for lookups
- `start_key_` and `end_key_`: Reference nodes by key properties

## Interactive vs Non-Interactive Mode

### Interactive Mode (CLI prompt)
In interactive mode, you will be prompted to confirm the import operation:
```
Do you want to import from ./data/*.csv [yN]? y
```

### Non-Interactive Mode (Command Line)
For non-interactive usage (scripts, automation), use the `--yes` flag:
```bash
# Using -e execute flag
cypher-graphdb -b memgraph --yes -e "import ./data/*.csv"

# Using piped input (auto-confirms)
echo "import ./data/*.csv" | cypher-graphdb -b memgraph
```

## Output

The import command provides feedback during the operation:
```
Auto-confirming import from ./data/*.csv
  Importing from ./data/nodes.csv 
  Importing from ./data/edges.csv 
Successfully imported.
```

## Error Handling

Common error scenarios:
- **File not found**: Ensure the file path is correct and accessible
- **Invalid format**: Check file format and use explicit `format=` parameter
- **No files found**: Verify glob patterns and directory contents
- **Format detection failed**: Use explicit format specification

## Related Commands

- `export` / `export graph` - Export graph data to files
- `clear graph` - Clear the current in-memory graph
- `fetch nodes` / `fetch edges` - Query specific graph elements

## Notes

- Import operations add data to the current graph database
- Large files may take time to process
- CSV files should have consistent column structure
- Excel files support multiple sheets (first sheet used by default)
- File paths support both absolute and relative paths

# Export Graph

**Command:** export | export graph

**Description:** Export graph data from the current in-memory graph to various file formats. The export command can save graph data that has been loaded via import commands or built up through Cypher query results.

## Syntax

```
export <target>[,format=<format>][,option=value,...]
```

## Parameters

| Parameter | Type   | Required | Description                                                          |
|-----------|--------|----------|----------------------------------------------------------------------|
| target    | string | Yes      | File path or directory path for export destination                   |
| format    | string | No       | Explicit format specification: `csv` or `excel` (auto-detected if omitted) |

## Supported Formats

- **Excel (.xlsx, .xls)** - Microsoft Excel format with multiple worksheets
- **CSV (.csv)** - Comma-separated values format with multiple files

## Format Auto-Detection

The export command automatically detects output formats based on:

1. **File extension** (.xlsx, .xls, .csv)
2. **Target path analysis** (file vs directory)

If auto-detection fails, explicitly specify the format using `format=<format>`.

## Export Data Sources

The export command can export data from several sources:

### Current Graph Data
Export the current in-memory graph that has been built up through:
- Import operations (`import` command)
- Cypher query results added with `add` command
- Graph operations and transformations

### Cypher Query Results  
Export can also be used in command pipelines to export query results:
```
MATCH (n:Person) RETURN n | export persons.csv
```

## Export Behavior

### Directory vs File Export

**Directory Export:**
- Creates separate files for each node label and edge type
- Node files: `<Label>.csv` or `<Label>.xlsx`
- Edge files: `_<EdgeType>.csv` or `_<EdgeType>.xlsx` (with underscore prefix)

**File Export:**
- Single file containing all data (Excel only)
- Multiple worksheets for different labels/types
- CSV requires directory export for multiple types

### Data Structure

**Node Data:**
- Each row represents a node
- Columns for all node properties
- Special columns: `gid_` (graph ID), `label_` (optional)

**Edge Data:**
- Each row represents an edge/relationship
- Relationship properties as columns
- Reference columns: `start_gid_`, `end_gid_` for node connections
- Special columns: `label_` (optional)

## Options

Currently, the export command supports the following options:

- **format**: Explicitly specify the export format (`csv` or `excel`)
- **with_label**: Include label column in exported data (`true` or `false`, default: `true`)

### Export Option Details

**with_label**
- `true` (default): Include `label_` column showing node/edge types
- `false`: Omit label column for cleaner data export

## Examples

### Basic Export

```
# Export current graph to Excel directory
export ./output/

# Export to single Excel file
export data.xlsx

# Export to CSV directory  
export ./csv_data/

# Export single node type to CSV file (if graph contains only one type)
export nodes.csv
```

### Format Specification

```
# Force Excel format for unknown extension
export data.export,format=excel

# Force CSV format
export data.txt,format=csv
```

### Export Options

```
# Export without label columns
export data.xlsx,with_label=false

# Export with specific format and options
export ./output/,format=excel,with_label=true
```

### Pipeline Export Examples

```
# Export query results directly
MATCH (p:Person) RETURN p | export persons.xlsx

# Export specific nodes and edges
MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b | export relationships.xlsx

# Export with processing
MATCH (n) WHERE n.age > 30 RETURN n | export mature_users.csv
```

## File Structure Examples

### Excel Export Structure
```
data.xlsx
├── Person (worksheet)        # Node data
│   ├── gid_
│   ├── label_        
│   ├── name
│   └── age
└── _KNOWS (worksheet)        # Edge data
    ├── gid_
    ├── label_
    ├── start_gid_
    ├── end_gid_
    └── since
```

### CSV Directory Export Structure
```
output/
├── Person.csv               # Node data
│   ├── gid_,label_,name,age
│   └── p1,Person,Alice,30
└── _KNOWS.csv               # Edge data
    ├── gid_,label_,start_gid_,end_gid_,since
    └── e1,KNOWS,p1,p2,2020
```

## Export Process

1. **Data Validation**: Checks that graph contains data to export
2. **Format Resolution**: Determines output format from extension or explicit parameter
3. **Data Organization**: Groups nodes by label and edges by type
4. **File Generation**: Creates output files with appropriate structure
5. **Progress Feedback**: Shows export progress and statistics

## Output Information

The export command provides feedback during the operation:

```
 Exporting to directory ./output/, 150 graph object(s).
 Exporting to file ./output/Person.xlsx [Person], 100 graph object(s).
 Exporting to file ./output/_KNOWS.xlsx [_KNOWS], 50 graph object(s).
Successfully exported.
```

## Error Handling

Common error scenarios:

- **Empty graph**: No data to export - use `add` or `import` commands first
- **Invalid path**: Ensure target directory exists and is writable
- **Format mismatch**: CSV export requires directory for multiple types
- **Invalid format**: Check format specification and file extensions

## Data Integrity

**Column Consistency:**
- All nodes of the same label share the same column structure
- Missing properties are exported as empty/null values
- Property types are preserved where possible

**Reference Integrity:**
- Edge `start_gid_` and `end_gid_` columns reference actual node `gid_` values
- Maintains relationships between exported nodes and edges

## Performance Considerations

- Large graphs may take time to export
- Excel format is slower than CSV for very large datasets
- Directory export is faster than single-file export for mixed data types
- Memory usage scales with graph size during export

## Related Commands

- **import / import graph** - Import graph data from files
- **add / add graph** - Add Cypher query results to current graph
- **clear graph** - Clear the current in-memory graph
- **stats** - Show current graph statistics

## Notes

- Export operations read from the current in-memory graph
- Graph must contain data before export (not empty)
- Excel exports support multiple worksheets automatically
- CSV exports create separate files for different types
- File paths support both absolute and relative paths
- Export preserves all node and edge properties
- Special columns (`gid_`, `label_`, etc.) are automatically included

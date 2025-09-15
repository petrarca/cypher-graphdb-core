# Add Graph

**Command:** `add` | `add graph`

**Description:** Add query results or data to the current in-memory graph. The add command accumulates data from Cypher queries, variables, or other sources into a working graph that can be analyzed, exported, or further processed.

## Syntax

```
add [<source>]
```

## Parameters

| Parameter | Type   | Required | Description                                                    |
|-----------|--------|----------|----------------------------------------------------------------|
| source    | string | No       | Source to add to graph: variable name, result reference, or omit for last result |

## Data Sources

The add command can work with various data sources:

### Last Query Result (Default)
```
# Execute query, then add results
MATCH (p:Person) RETURN p
add

# Or combine in pipeline
MATCH (p:Person) RETURN p | add
```

### Explicit Result Reference
```
# Add the last result explicitly
add .

# Add specific result by position (row 1)
add #1

# Add specific result by position (row 2, column 1)  
add #2:1
```

### Stored Variables
```
# Store query result in variable
set nodes = MATCH (n:Person) RETURN n

# Add variable content to graph
add nodes
```

### Pipeline Integration
```
# Direct pipeline: query → add → export
MATCH (n)-[r]->(m) RETURN n, r, m | add | export graph.xlsx

# Multi-step pipeline
MATCH (p:Person) RETURN p | add | stats | tree
```

## Graph Merging Behavior

### Data Types Handled

The add command processes different data types:

**Graph Nodes (`GraphNode`)**
- Added to the graph's node collection
- Deduplicated by node ID
- Preserves all properties and labels

**Graph Edges (`GraphEdge`)**  
- Added to the graph's edge collection
- Deduplicated by edge ID
- Maintains start/end node references

**Graph Paths (`GraphPath`)**
- Extracts nodes and edges from the path
- Adds each component to appropriate collection
- Preserves path structure information

**Collections (Lists, Tuples)**
- Recursively processes each element
- Handles nested collections automatically
- Flattens complex result structures

**Scalar Values**
- Ignored during merge process
- Does not affect graph structure

### Deduplication Strategy

The add command prevents duplicate data:
- **ID-based deduplication**: Objects with existing IDs are skipped
- **Merge safety**: Same object can be added multiple times safely
- **Reference integrity**: Maintains consistent object relationships

## Advanced Usage

### Incremental Graph Building
```
# Start with empty graph
clear graph

# Add nodes step by step
MATCH (p:Person) RETURN p | add
MATCH (c:Company) RETURN c | add

# Add relationships
MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) RETURN p, r, c | add

# View accumulated graph
stats
```

### Conditional Adding
```
# Add only specific results
MATCH (p:Person) WHERE p.age > 30 RETURN p | add

# Add from multiple queries
MATCH (p:Person {role: 'manager'}) RETURN p | add
MATCH (p:Person {role: 'director'}) RETURN p | add
```

### Variable-based Workflow
```
# Store different result sets
set managers = MATCH (p:Person {role: 'manager'}) RETURN p
set employees = MATCH (p:Person {role: 'employee'}) RETURN p
set relationships = MATCH (p1)-[r]->(p2) RETURN p1, r, p2

# Add selectively to graph
add managers
add relationships
```

## Graph State Management

### Clear and Add Patterns
```
# Option 1: Clear then add (using __ variable)
set __ = MATCH (n) RETURN n  # Clears graph first, then adds

# Option 2: Explicit clear then add
clear graph
MATCH (n) RETURN n | add

# Option 3: Accumulative adding (default)
MATCH (p:Person) RETURN p | add
MATCH (c:Company) RETURN c | add  # Adds to existing graph
```

### Memory Management
```
# Check current graph state
stats

# Clear when memory gets large
clear graph

# Selective building for large datasets
MATCH (n:Person) WHERE n.department = 'IT' RETURN n | add
```

## Integration with Other Commands

### Export Workflow
```
# Build graph from multiple sources
MATCH (p:Person) RETURN p | add
MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN r | add

# Export accumulated graph
export ./output/

# Or pipeline directly
MATCH (n)-[r]->(m) RETURN n, r, m | add | export complete_graph.xlsx
```

### Analysis Workflow
```
# Build graph
MATCH (n)-[r]->(m) RETURN n, r, m | add

# Analyze structure
stats                    # Show graph statistics
tree                     # Convert to tree visualization
resolve edges           # Load missing referenced nodes
```

### Query Development
```
# Test queries and accumulate results
MATCH (p:Person {department: 'Engineering'}) RETURN p | add
MATCH (p:Person {department: 'Marketing'}) RETURN p | add

# Check what was added
stats

# Export for validation
export ./test_data.csv
```

## Examples

### Basic Adding
```
# Simple node addition
MATCH (p:Person) RETURN p | add

# Add with immediate export
MATCH (n) RETURN n | add | export nodes.xlsx

# Add last result explicitly
MATCH (c:Company) RETURN c
add .
```

### Complex Graph Building
```
# Multi-step graph construction
clear graph

# Add all persons
MATCH (p:Person) RETURN p | add

# Add all companies  
MATCH (c:Company) RETURN c | add

# Add employment relationships
MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) RETURN p, r, c | add

# Add colleague relationships
MATCH (p1:Person)-[r:COLLEAGUE]->(p2:Person) RETURN p1, r, p2 | add

# View final graph statistics
stats
```

### Pipeline Workflows
```
# Query → Add → Export pipeline
MATCH (p:Person) WHERE p.active = true RETURN p | add | export active_persons.csv

# Query → Add → Transform → Export pipeline  
MATCH (n)-[r]->(m) RETURN n, r, m | add | tree | export graph_tree.xlsx

# Multi-query accumulation
MATCH (p:Person) RETURN p | add
MATCH (c:Company) RETURN c | add
export complete_data.xlsx
```

### Variable-based Operations
```
# Store and selectively add
set core_data = MATCH (n:Core) RETURN n
set extended_data = MATCH (n:Extended) RETURN n

# Add core data first
add core_data
stats

# Add extended data if needed
add extended_data
```

## Output Information

The add command provides minimal feedback:
```
# Successful add (silent)
MATCH (p:Person) RETURN p | add

# Check what was added
stats
> nodes=150, (1 label(s)), edges=75 (2 label(s)), values=0, paths=0, rows=150, cols=1
```

## Error Handling

Common scenarios:

**Empty Results**
- Adding empty query results is safe (no-op)
- Graph state remains unchanged

**Invalid References**
- Invalid variable names show error message
- Command fails gracefully without affecting graph

**Memory Constraints**
- Large graphs may consume significant memory
- Use `clear graph` to free memory when needed

**Type Mismatches**
- Non-graph objects are ignored safely
- Only valid graph entities are added

## Performance Considerations

**Memory Usage**
- Graph data is held in memory
- Large datasets may require memory management
- Use `stats` to monitor graph size

**Incremental Building**
- Adding to existing graph is efficient
- Deduplication prevents memory bloat
- ID-based indexing ensures fast lookups

**Pipeline Efficiency**
- Direct pipelines are more efficient than multi-step
- Avoid unnecessary intermediate storage
- Use variables for reusable data sets

## Related Commands

- **import / import graph** - Load data from files, automatically adds to graph
- **export / export graph** - Export current graph data to files
- **clear graph** - Clear the current in-memory graph
- **stats** - Show current graph statistics
- **tree** - Convert graph to tree visualization
- **set** - Store query results in variables
- **resolve edges** - Load missing nodes referenced by edges

## Implementation Notes

**Internal Mechanism**
- The `add` command is internally rewritten as `set _=<source>`
- Uses the special `_` variable to trigger graph merging
- `__` variable clears graph before adding (not commonly used)

**Graph Object Types**
- Supports all cypher-graphdb graph object types
- Handles complex nested result structures
- Maintains object relationships and properties

**Pipeline Integration**
- Seamlessly integrates with command pipelines
- Can be chained with other graph operations
- Supports both input and output data flow

## Best Practices

1. **Use `stats` frequently** to monitor graph size and structure
2. **Clear graph before major operations** to avoid memory issues
3. **Pipeline when possible** for efficient data flow
4. **Store reusable results** in variables for repeated access
5. **Use incremental building** for complex graph construction
6. **Validate with export** to verify accumulated data

The add command is central to the cypher-graphdb workflow, enabling flexible graph construction and analysis by bridging query results with graph operations.

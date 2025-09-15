# Display Last Result

**Command:** `.` (dot)

**Description:** Display the last query result from the previous Cypher query or command execution. This command provides quick access to re-display the most recent result without re-executing the query.

## Syntax

```
.
```

## Parameters

No parameters required. The dot command takes no arguments and always displays the complete last result.

## Functionality

The `.` command retrieves and displays the last stored query result from the CLI session. It acts as a convenient shortcut to re-examine the most recent data without needing to re-run expensive queries.

### Result Storage

**Last Result Storage**
- The CLI automatically stores the result of every successful Cypher query
- Results from successful CLI commands are also stored
- Only the most recent result is kept (not a history of results)
- Results persist until the next successful query/command execution

**Storage Triggers**
- Cypher query execution: `MATCH (n) RETURN n`
- CLI commands that produce output: `stats`, `fetch nodes`, etc.
- Import operations: `import data.csv`
- Variable retrieval: `get varname`

### Display Behavior

**Rendering Format**
- Uses the current default output format (table, json, list)
- Respects current display settings and options
- Maintains the same column headers and formatting as the original result
- Shows the complete result set (no truncation)

**Empty Results**
- If no previous result exists: displays "[yellow]No result!"
- If previous result was empty: displays "[yellow]No result!"
- Safe to use at any time without errors

## Use Cases

### Quick Result Review
```bash
# Execute a complex query
MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) 
WHERE c.industry = 'Technology' 
RETURN p.name, p.role, c.name 
ORDER BY c.name, p.name

# Review the results again without re-execution
.
```

### Result Verification
```bash
# After data modification, check what was returned
CREATE (p:Person {name: 'Alice', age: 30}) RETURN p

# Verify the created node details
.
```

### Format Comparison
```bash
# Execute query with default format
MATCH (n:Person) RETURN n LIMIT 5

# Switch to JSON format and view same result
json
.

# Switch to table format and view same result  
table
.
```

### Pipeline Development
```bash
# Execute query and see results
MATCH (p:Person) RETURN p LIMIT 10

# Review results before deciding on next pipeline step
.

# Now build pipeline based on the result structure
MATCH (p:Person) RETURN p LIMIT 10 | add | export persons.csv
```

### Data Exploration Workflow
```bash
# Query large dataset
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 100

# Examine results in detail
.

# Switch to list format for better readability
list
.

# Count total results
MATCH (n)-[r]->(m) RETURN count(*)

# Go back to examine the sample data
.  # Shows the count result

# Need to see the actual data again - re-run original query
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 100
```

## Integration with Other Commands

### Variable System
```bash
# Execute query
MATCH (p:Person) RETURN p

# Store current result in variable
set persons = .

# Later retrieve and display
get persons
```

### Pipeline Operations
```bash
# Query results
MATCH (n:Company) RETURN n

# Use last result in pipeline (note: this pipes the result, not re-executes)
. | add | stats
```

### Export Operations
```bash
# Execute query
MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN p, r, f

# Export the last result
. | export relationships.xlsx

# Or use export with implicit last result
export relationships.xlsx  # Also exports last result
```

## Result Reference System

The `.` command is part of a broader result reference system:

### Reference Types
- **`.`** - Complete last result
- **`#1`** - First row of last result
- **`#2`** - Second row of last result  
- **`#1:2`** - First row, second column of last result

### Variable Integration
```bash
# The dot command uses the special "." variable
get .          # Same as using the dot command

# Explicitly set the "." variable (not recommended)
set . = MATCH (n) RETURN n
```

## Output Examples

### Table Format (Default)
```
┌───┬─────────────────────────────────────┐
│ # │ Person                              │
├───┼─────────────────────────────────────┤
│ 1 │ Person {name: "Alice", age: 30}     │
│ 2 │ Person {name: "Bob", age: 25}       │
│ 3 │ Person {name: "Charlie", age: 35}   │
└───┴─────────────────────────────────────┘
```

### JSON Format
```json
[
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25},
  {"name": "Charlie", "age": 35}
]
```

### List Format (Tree View)
```
Result
├── Person {name: "Alice", age: 30}
├── Person {name: "Bob", age: 25}
└── Person {name: "Charlie", age: 35}
```

## Error Handling

**No Previous Result**
```bash
# At start of session or after clear
.
# Output: No result!
```

**Empty Previous Result**
```bash
# After query that returns no rows
MATCH (n:NonExistent) RETURN n
.
# Output: No result!
```

**Safe Operation**
- Never throws errors or exceptions
- Always provides user-friendly feedback
- Can be used at any point in the session

## Performance Considerations

**Memory Efficiency**
- Results are stored in memory from the last operation
- No additional query execution required
- Instant display of stored data

**Large Results**
- Large result sets remain in memory until overwritten
- Consider memory usage for very large datasets
- Use `stats` to check result size before displaying

**Session Management**
- Results are session-specific (cleared on CLI restart)
- Not persisted between sessions
- Cleared when new results are generated

## Related Commands

- **`get .`** - Alternative way to access last result via variable system
- **`set varname = .`** - Store last result in a named variable
- **`stats`** - Show statistics about current graph and last result
- **`#1`, `#2:1`** - Access specific rows/cells from last result
- **`add`** - Add last result to in-memory graph when used without arguments

## Interactive vs Pipeline Usage

### Interactive Mode
```bash
# In interactive CLI session
cypher-graphdb> MATCH (p:Person) RETURN p LIMIT 3
# Results displayed...

cypher-graphdb> .
# Same results displayed again
```

### Non-Interactive Mode
```bash
# In script or batch mode
echo -e "MATCH (p:Person) RETURN p LIMIT 3\n." | cypher-graphdb -b memgraph

# Both commands execute in sequence
```

## Best Practices

1. **Use for verification** - Check results after data modifications
2. **Format experimentation** - Try different output formats on same data
3. **Pipeline development** - Review data structure before building pipelines
4. **Memory awareness** - Be mindful of large result sets in memory
5. **Session workflow** - Integrate into exploratory data analysis workflow
6. **Documentation** - Use when documenting query results and examples

## Implementation Notes

**Internal Mechanism**
- Mapped to `last_result_op_` action in command parser
- Retrieves data from `CLIGraphData.get_var(".")` 
- Uses `_fetch_from_last_result()` for data access
- Leverages standard renderer for output formatting

**Variable Integration**
- The "." is treated as a special variable name
- Automatically updated after successful query execution
- Can be accessed through variable system commands

**Output Formatting**
- Uses same rendering pipeline as direct query results
- Supports all output formats (table, json, list)
- Maintains original column headers and metadata

The `.` command is an essential tool for interactive data exploration, providing immediate access to recent results and supporting efficient CLI workflows for graph database analysis.

The command line (prompt) supports the folloing commands.
Details about every command can be foud by **help {command}** like "help create nodes".

Commands are in the form of {command} [options].

All other inputs are treated as **cypher statements**.

# List of commands

- **exit**,**quit**,**q**: Terminate the application.
- **help**: Show this help.
- **create node**: Create a new node in the current graphdb.
- **create edge**: Create a new edge in the current graphdb.
- **fetch nodes**: Fetch one or more nodes.
- **fetch edges**: Fetch one or more edges.
- **update**: Update a node or edge.
- **delete**: Delete a node or edge.
- **import graph**: Import a graph from various source formats (e.g. Excel, CSV).
- **export graph**: Import a graph to various target formats (e.g. Excel, CSV)
- **connect**: Connect to a graph backend.
- **disconnect**: Disconnect from a graph backend.
- **create dbgraph**: Create a new graph the graph backend.
- **drop dbgraph**: Drop a graph in the graph backend.
- **dbgraph exists**: Check if a graph exists in a graph backend.
- **change dbgraph**: Change the current working graph.
- **commit**: Commit the current transaction.
- **rollback**: Rollback the current transaction
- **json**: Change output format to json
- **table** Change output format to tabular format.
- **list** List output format
- **add graph**, **add**: Add result to internal, in-memory graph
- **clear graph**, **clear**: Clear the internal, in-memory graph
- **resolve edges**: Load missing nodes into graph, which are referenced by edges in the graph
- **tree**: Convert graph to tree and print as tree
- **config**: Apply configuration
- **set**: Set variable(s)
- **get**: Get variable(s)
- **.** (dot): Print the last result
- **gid**: Create new gid(s)
- **load** Load models from file
- **models** Print loaded models
- **backends** Print available backends
- **stats**, **statistics**: Print statistics
- **_**: Graph operator
- **tree**: Print input as tree

Some commands can be __piped together__ like

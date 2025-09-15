# Open issues and backlog

### PRIO A:
- Code Documentation
- Integration tests
- Option --create-graph to create graph if not exist. Graph name must be defined
- Argument validation in CLI commands (e.g. fetch edges)
- ~~Check connect options (DSN) via arguments and prompt~~
- ~~set timing on/off to print execution time, provided by statistics~~
- ~~delete graph objects~~
- ~~create edge~~
- ~~streamlined prompt parsing, e.g. tags=["1",2,3]~~
- ~~move graph into CLIGraphDB, TBC~~
- ~~Autocommit for CLI in CypherGraphDB - remove db.commit on CLI level (e.g. CLIGraphDB)~~
- ~~Feature flags for autocommit~~
- ~~ParsedCyperQuery: Simplified parse tree to get info about operations (e.g. DELETE) and labels. Foundation for node/edge level security (see e.g. Neo4J)~~
- ~~Additonal visualize directed graph as tree, like "match (t:Technology)-[s:SUBCLASS_OF]->(e) return t,s,e | tree"~~
- ~~Auto completion with prompt_toolkit~~

### PRIO B:
- Read only mode for CypherGraphDB to prevent executing of updating clauses, + cmd line option for CLI
- Introduce classes with schemas and additonal meta information (allowed relationships between objects)
- Fix colorizing [],<> etc. in rich.
- ~~Prompt to list all db-graphs (need interface in Backend for that)~~

### PRIO C:
- Check if we should remove depdency from AGE (python), required only for result type parsing
- Integration into networkx for advanced graph operations

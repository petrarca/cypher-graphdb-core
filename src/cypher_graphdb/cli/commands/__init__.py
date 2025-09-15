"""Command subsystem for the cypher-graphdb CLI.

This package contains the command implementations that are executed
by the CLI runtime in cli_app.py.

Command Registry System:
The commands package now includes a command registry (command_registry.py)
that allows for more flexible command management. Commands can be registered
manually by importing them and calling registry.register().

Example registration:
    # Import registry and command
    from cypher_graphdb.cli.cli_command_registry import registry
    from cypher_graphdb.cli.commands.some_command import SomeCommand

    # Register with auto-detected name
    registry.register(SomeCommand)  # Uses SomeCommand.command_name

    # Register with explicit name
    registry.register(SomeCommand, "custom_name")
"""

# Register commands
from cypher_graphdb.cli.command_registry import registry
from cypher_graphdb.cli.commands.add_graph_command import AddGraphCommand
from cypher_graphdb.cli.commands.apply_config_command import ApplyConfigCommand
from cypher_graphdb.cli.commands.change_dbgraph_command import ChangeDbgraphCommand
from cypher_graphdb.cli.commands.clear_graph_command import ClearGraphCommand
from cypher_graphdb.cli.commands.commit_command import CommitCommand
from cypher_graphdb.cli.commands.connect_command import ConnectCommand
from cypher_graphdb.cli.commands.create_dbgraph_command import CreateDbgraphCommand
from cypher_graphdb.cli.commands.create_edge_command import CreateEdgeCommand
from cypher_graphdb.cli.commands.create_linked_node_command import CreateLinkedNodeCommand
from cypher_graphdb.cli.commands.create_node_command import CreateNodeCommand
from cypher_graphdb.cli.commands.dbgraph_exists_command import DbgraphExistsCommand
from cypher_graphdb.cli.commands.delete_graphobj_command import DeleteGraphobjCommand
from cypher_graphdb.cli.commands.disconnect_command import DisconnectCommand
from cypher_graphdb.cli.commands.drop_dbgraph_command import DropDbgraphCommand
from cypher_graphdb.cli.commands.dump_backends_command import DumpBackendsCommand
from cypher_graphdb.cli.commands.dump_dbgraphs_command import DumpDbgraphsCommand
from cypher_graphdb.cli.commands.dump_labels_command import DumpLabelsCommand
from cypher_graphdb.cli.commands.dump_models_command import DumpModelsCommand
from cypher_graphdb.cli.commands.dump_parsed_query_command import DumpParsedQueryCommand
from cypher_graphdb.cli.commands.dump_schema_command import DumpSchemaCommand
from cypher_graphdb.cli.commands.dump_statistics_command import DumpStatisticsCommand
from cypher_graphdb.cli.commands.execute_cypher_command import ExecuteCypherCommand
from cypher_graphdb.cli.commands.execute_file_command import ExecuteFileCommand
from cypher_graphdb.cli.commands.exit_command import ExitCommand
from cypher_graphdb.cli.commands.export_graph_command import ExportGraphCommand
from cypher_graphdb.cli.commands.fetch_all_command import FetchAllCommand
from cypher_graphdb.cli.commands.fetch_edges_command import FetchEdgesCommand
from cypher_graphdb.cli.commands.fetch_nodes_command import FetchNodesCommand
from cypher_graphdb.cli.commands.format_output_command import FormatOutputCommand
from cypher_graphdb.cli.commands.get_command import GetCommand
from cypher_graphdb.cli.commands.gid_command import GidCommand
from cypher_graphdb.cli.commands.graph_op_command import GraphOpCommand
from cypher_graphdb.cli.commands.graph_to_tree_command import GraphToTreeCommand
from cypher_graphdb.cli.commands.help_command import HelpCommand
from cypher_graphdb.cli.commands.import_graph_command import ImportGraphCommand
from cypher_graphdb.cli.commands.last_result_op_command import LastResultOpCommand
from cypher_graphdb.cli.commands.load_models_command import LoadModelsCommand
from cypher_graphdb.cli.commands.resolve_edges_command import ResolveEdgesCommand
from cypher_graphdb.cli.commands.rollback_command import RollbackCommand
from cypher_graphdb.cli.commands.search_command import SearchCommand
from cypher_graphdb.cli.commands.set_command import SetCommand
from cypher_graphdb.cli.commands.sql_command import SqlCommand
from cypher_graphdb.cli.commands.update_graphobj_command import UpdateGraphobjCommand

# Register all commands
registry.register(GidCommand)
registry.register(ExitCommand)
registry.register(HelpCommand)
registry.register(SetCommand)
registry.register(GetCommand)
registry.register(SqlCommand)
registry.register(SearchCommand)
registry.register(ConnectCommand)
registry.register(DisconnectCommand)
registry.register(CommitCommand)
registry.register(RollbackCommand)
registry.register(ExecuteCypherCommand)
registry.register(AddGraphCommand)
registry.register(ClearGraphCommand)
registry.register(GraphOpCommand)
registry.register(ResolveEdgesCommand)
registry.register(FetchEdgesCommand)
registry.register(FetchNodesCommand)
registry.register(FetchAllCommand)
registry.register(CreateNodeCommand)
registry.register(CreateEdgeCommand)
registry.register(CreateLinkedNodeCommand)
registry.register(DeleteGraphobjCommand)
registry.register(UpdateGraphobjCommand)
registry.register(ExportGraphCommand)
registry.register(ImportGraphCommand)
registry.register(DumpParsedQueryCommand)
registry.register(DumpStatisticsCommand)
registry.register(DumpBackendsCommand)
registry.register(DumpLabelsCommand)
registry.register(DumpDbgraphsCommand)
registry.register(DumpModelsCommand)
registry.register(CreateDbgraphCommand)
registry.register(DropDbgraphCommand)
registry.register(ChangeDbgraphCommand)
registry.register(DbgraphExistsCommand)
registry.register(LoadModelsCommand)
registry.register(ExecuteFileCommand)
registry.register(FormatOutputCommand)
registry.register(ApplyConfigCommand)
registry.register(DumpSchemaCommand)
registry.register(GraphToTreeCommand)
registry.register(LastResultOpCommand)

"""Dump indexes command implementation for database inspection."""

from typing import TYPE_CHECKING

from cypher_graphdb.cli.commands.base_command import BaseCommand

if TYPE_CHECKING:
    from cypher_graphdb.cli.promptparser import PromptParserCmd


class DumpIndexesCommand(BaseCommand):
    """Command to list all indexes on the current graph.

    Usage:
        indexes              -- show user-created indexes
        indexes all          -- include backend-internal indexes
    """

    command_name = "dump_indexes"

    command_map_entry = BaseCommand.create_command_map_entry(pattern="[[dump_indexes", tokens=["indexes"])

    def execute(self, parsed_cmd: PromptParserCmd) -> bool:
        """Execute the dump indexes command."""
        include_internal = parsed_cmd.args is not None and "all" in parsed_cmd.args

        indexes = self.graphdb.db.list_indexes(include_internal=include_internal)
        result = [
            (idx.label, idx.index_type.value, ", ".join(idx.property_names) if idx.property_names else "*", idx.index_name or "")
            for idx in indexes
        ]

        return self._post_processing_cmd(
            parsed_cmd,
            result,
            append_to_graph=False,
            render_kwargs={"col_headers": ["Label", "Type", "Properties", "Name"]},
        )

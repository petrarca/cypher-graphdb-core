"""CLI command mapping module: Maps CLI commands to their implementations.

This module creates the command mapping structure dynamically from the
command registry, associating CLI command names with their corresponding
handler functions and parameters.
"""

from typing import Any

# Import commands to trigger registration
from cypher_graphdb.cli import commands  # noqa: F401
from cypher_graphdb.cli.command_registry import registry


def _build_cmd_map() -> dict[str, dict[str, Any]]:
    """Build CMD_MAP dynamically from registered commands.

    This function iterates through all registered commands and extracts their
    command_map_entry attributes to build the command mapping dictionary.

    Returns:
        dict: The command mapping dictionary with pattern keys and command data
    """
    cmd_map = {}

    for _command_name, command_class in registry.get_all_commands().items():
        # Get the command_map_entry from the command class if it exists
        if hasattr(command_class, "command_map_entry"):
            command_map_entry = command_class.command_map_entry

            # The command_map_entry is a dict with pattern as key
            # and the command data (tokens, object_type) as value
            if isinstance(command_map_entry, dict):
                cmd_map.update(command_map_entry)

    return cmd_map


# Build the CMD_MAP dynamically from registered commands
CMD_MAP = _build_cmd_map()


def get_cmd_map_info() -> str:
    """Get debug information about the dynamically built CMD_MAP.

    Returns:
        str: Information about the CMD_MAP for debugging purposes
    """
    info = f"CMD_MAP built dynamically with {len(CMD_MAP)} entries:\n"
    for pattern in sorted(CMD_MAP.keys())[:5]:  # Show first 5 entries
        info += f"  {pattern}: {CMD_MAP[pattern]}\n"
    if len(CMD_MAP) > 5:
        info += f"  ... and {len(CMD_MAP) - 5} more entries\n"
    return info

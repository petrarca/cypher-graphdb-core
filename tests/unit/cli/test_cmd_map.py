"""Tests for CLI command mapping functionality."""

from cypher_graphdb.cli.command_map import (
    CMD_MAP,
    _build_cmd_map,
    get_cmd_map_info,
)
from cypher_graphdb.cli.command_registry import registry
from cypher_graphdb.cli.commandposition import CommandPosition
from cypher_graphdb.models import GraphObjectType


def test_cmd_map_is_built():
    """Test that CMD_MAP is properly built and not empty."""
    assert isinstance(CMD_MAP, dict)
    assert len(CMD_MAP) > 0


def test_cmd_map_has_expected_entries():
    """Test that CMD_MAP contains expected command patterns."""
    # Test some key command patterns that should exist
    expected_actions = [
        "help",
        "exit",
        "gid",
        "create_node",
        "create_edge",
    ]
    for action in expected_actions:
        assert action in CMD_MAP, f"Expected action '{action}' not found in CMD_MAP"


def test_cmd_map_structure():
    """Test that CMD_MAP has correct structure."""
    for pattern, entry in CMD_MAP.items():
        assert isinstance(pattern, str), f"Pattern '{pattern}' should be a string"
        assert isinstance(entry, dict), f"Entry for '{pattern}' should be a dict"
        assert "tokens" in entry, f"Entry for '{pattern}' should have 'tokens' key"
        assert isinstance(entry["tokens"], list), f"Tokens for '{pattern}' should be a list"


def test_build_cmd_map_function():
    """Test the _build_cmd_map function works correctly."""
    built_map = _build_cmd_map()
    assert isinstance(built_map, dict)
    assert len(built_map) > 0
    # Should match the current CMD_MAP
    assert len(built_map) == len(CMD_MAP)


def test_get_cmd_map_info():
    """Test the debug info function."""
    info = get_cmd_map_info()
    assert isinstance(info, str)
    assert "CMD_MAP built dynamically" in info
    assert str(len(CMD_MAP)) in info


def test_cmd_map_matches_registry_count():
    """Test that CMD_MAP includes entries from all registered commands with command_map_entry."""
    # Count commands in registry that have valid command_map_entry
    commands_with_map_entry = 0
    for command_class in registry.get_all_commands().values():
        if hasattr(command_class, "command_map_entry") and command_class.command_map_entry is not None:
            commands_with_map_entry += 1

    # CMD_MAP should have entries for all commands with valid command_map_entry
    # Note: Some commands might not have command_map_entry (like execute_cypher)
    assert len(CMD_MAP) > 0
    assert len(CMD_MAP) == commands_with_map_entry


def test_cmd_map_patterns_unique():
    """Test that all patterns in CMD_MAP are unique."""
    patterns = list(CMD_MAP.keys())
    assert len(patterns) == len(set(patterns))


def test_cmd_map_entries_have_valid_tokens():
    """Test that all CMD_MAP entries have valid token lists."""
    for pattern, entry in CMD_MAP.items():
        tokens = entry["tokens"]
        assert isinstance(tokens, list)
        assert len(tokens) > 0, f"Pattern '{pattern}' has empty tokens list"

        for token in tokens:
            assert token is None or isinstance(token, str), f"Token '{token}' in '{pattern}' should be string or None"


def test_cmd_map_specific_commands():
    """Test specific commands are present."""
    assert "help" in CMD_MAP
    assert "exit" in CMD_MAP


def test_cmd_map_object_type_validation():
    """Test that object-specific commands have correct object types."""
    node_commands = ["[[create_node", "[[fetch_nodes", "[[create_linked_node"]
    edge_commands = ["[[create_edge", "[[fetch_edges"]

    for pattern in node_commands:
        if pattern in CMD_MAP:
            assert CMD_MAP[pattern].get("object_type") == GraphObjectType.NODE

    for pattern in edge_commands:
        if pattern in CMD_MAP:
            assert CMD_MAP[pattern].get("object_type") == GraphObjectType.EDGE


def test_cmd_map_declares_behaviour_explicitly():
    """Every entry carries its behaviour as fields, not as punctuation."""
    for action, entry in CMD_MAP.items():
        assert "position" in entry, f"'{action}' has no position"
        assert "requires_backend" in entry, f"'{action}' has no requires_backend"
        assert "splits_args" in entry, f"'{action}' has no splits_args"
        assert isinstance(entry["requires_backend"], bool)
        assert isinstance(entry["splits_args"], bool)
        assert CommandPosition(entry["position"]) in CommandPosition


def test_action_names_carry_no_metacharacters():
    """The old encoding ([[cmd]], cmd_, cmd*) must not come back."""
    for action in CMD_MAP:
        assert not any(ch in action for ch in "[]*"), f"'{action}' still encodes flags in its name"
        assert not action.endswith("_") or action == "_", f"'{action}' still encodes a trailing flag"


def test_cmd_map_integration_with_registry():
    """Test that CMD_MAP integrates correctly with command registry."""
    # All patterns should correspond to registered commands
    for pattern, entry in CMD_MAP.items():
        # Extract potential command name from pattern
        # Pattern format is typically [[command_name or similar
        if pattern.startswith("[["):
            # This is integration testing - just verify structure is consistent
            assert "tokens" in entry
            assert isinstance(entry["tokens"], list)


def test_behaviour_flags_match_their_commands():
    """Spot-check the flags that used to be punctuation."""
    # 'sql' takes an opaque remainder rather than parsed args (was "*")
    assert CMD_MAP["sql"]["splits_args"] is False

    # 'export_graph' consumes a pipeline, so it must come last (was "]]")
    assert CMD_MAP["export_graph"]["position"] == CommandPosition.LAST

    # 'format_output' works at either end (was "[[...]]")
    assert CMD_MAP["format_output"]["position"] == CommandPosition.FIRST_OR_LAST

    # 'help' needs no database connection (was trailing "_")
    assert CMD_MAP["help"]["requires_backend"] is False

    # 'fetch_nodes' is an ordinary first-position command needing a backend
    assert CMD_MAP["fetch_nodes"]["position"] == CommandPosition.FIRST
    assert CMD_MAP["fetch_nodes"]["requires_backend"] is True


def test_cmd_map_categorization():
    """Test that commands can be categorized correctly."""
    categories = {
        "help": ["help"],
        "create": ["create_node", "create_edge", "create_linked_node"],
        "fetch": ["fetch_nodes", "fetch_edges", "fetch_all"],
        "database": ["connect", "disconnect", "commit", "rollback"],
        "export": ["export_graph", "import_graph"],
        "graph_ops": ["clear_graph", "add_graph", "resolve_edges"],
    }

    for category, expected_actions in categories.items():
        found = [a for a in expected_actions if a in CMD_MAP]
        assert len(found) > 0, f"No commands found for category '{category}'"

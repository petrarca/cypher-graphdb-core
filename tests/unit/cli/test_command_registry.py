"""Tests for CLI command registry functionality."""

from cypher_graphdb.cli.command_registry import CommandRegistry
from cypher_graphdb.cli.commands.base_command import BaseCommand


class MockCommand(BaseCommand):
    """Mock command for testing."""

    command_name = "test_command"

    def execute(self, parsed_cmd):
        """Mock execute method."""
        return True


class MockCommandWithoutName(BaseCommand):
    """Mock command without command_name for testing."""

    def execute(self, parsed_cmd):
        """Mock execute method."""
        return True


def test_registry_initialization():
    """Test that a new registry initializes correctly."""
    registry = CommandRegistry()
    all_commands = registry.get_all_commands()
    assert isinstance(all_commands, dict)
    assert len(all_commands) == 0


def test_register_command_with_class_name():
    """Test registering a command using its class command_name."""
    registry = CommandRegistry()
    registry.register(MockCommand)

    assert registry.is_registered("test_command")
    assert registry.get_command_class("test_command") == MockCommand


def test_register_command_with_explicit_name():
    """Test registering a command with an explicit name."""
    registry = CommandRegistry()
    registry.register(MockCommand, "custom_name")

    assert registry.is_registered("custom_name")
    assert registry.get_command_class("custom_name") == MockCommand


def test_register_command_without_name_raises_error():
    """Test that registering a command without name raises ValueError."""
    registry = CommandRegistry()

    try:
        registry.register(MockCommandWithoutName)
        raise AssertionError("Expected ValueError to be raised")
    except ValueError as e:
        assert "No command name available" in str(e)


def test_register_duplicate_command_raises_error():
    """Test that registering a duplicate command raises ValueError."""
    registry = CommandRegistry()
    registry.register(MockCommand)

    try:
        registry.register(MockCommand)
        raise AssertionError("Expected ValueError to be raised")
    except ValueError as e:
        assert "is already registered" in str(e)


def test_get_command_class_existing():
    """Test getting an existing command class."""
    registry = CommandRegistry()
    registry.register(MockCommand)

    result = registry.get_command_class("test_command")
    assert result == MockCommand


def test_get_command_class_nonexistent():
    """Test getting a non-existent command class returns None."""
    registry = CommandRegistry()

    result = registry.get_command_class("nonexistent")
    assert result is None


def test_get_all_commands():
    """Test getting all registered commands."""
    registry = CommandRegistry()
    registry.register(MockCommand)

    all_commands = registry.get_all_commands()
    assert isinstance(all_commands, dict)
    assert "test_command" in all_commands
    assert all_commands["test_command"] == MockCommand

    # Test that it returns a copy (can modify without affecting registry)
    initial_len = len(registry.get_all_commands())
    all_commands.clear()
    final_len = len(registry.get_all_commands())
    assert initial_len == final_len


def test_is_registered_true():
    """Test is_registered returns True for registered commands."""
    registry = CommandRegistry()
    registry.register(MockCommand)

    assert registry.is_registered("test_command") is True


def test_is_registered_false():
    """Test is_registered returns False for unregistered commands."""
    registry = CommandRegistry()

    assert registry.is_registered("nonexistent") is False


def test_unregister_existing_command():
    """Test unregistering an existing command."""
    registry = CommandRegistry()
    registry.register(MockCommand)

    result = registry.unregister("test_command")
    assert result is True
    assert not registry.is_registered("test_command")


def test_unregister_nonexistent_command():
    """Test unregistering a non-existent command."""
    registry = CommandRegistry()

    result = registry.unregister("nonexistent")
    assert result is False


def test_clear_registry():
    """Test clearing all commands from registry."""
    registry = CommandRegistry()
    registry.register(MockCommand)

    registry.clear()
    assert len(registry.get_all_commands()) == 0


def test_registry_with_multiple_commands():
    """Test registry with multiple commands."""
    registry = CommandRegistry()

    # Create mock commands
    class Command1(BaseCommand):
        command_name = "cmd1"

        def execute(self, parsed_cmd):
            return True

    class Command2(BaseCommand):
        command_name = "cmd2"

        def execute(self, parsed_cmd):
            return True

    registry.register(Command1)
    registry.register(Command2)

    assert len(registry.get_all_commands()) == 2
    assert registry.is_registered("cmd1")
    assert registry.is_registered("cmd2")
    assert registry.get_command_class("cmd1") == Command1
    assert registry.get_command_class("cmd2") == Command2


def test_registry_with_real_commands():
    """Test the registry with real command imports."""
    # Import the real registry to test with actual commands
    from cypher_graphdb.cli.command_registry import registry

    # Should have commands registered from imports
    all_commands = registry.get_all_commands()
    assert len(all_commands) > 0

    # Check that some expected commands are present
    expected_commands = ["help", "exit", "gid", "create_node", "create_edge"]
    for cmd_name in expected_commands:
        assert registry.is_registered(cmd_name), f"Command '{cmd_name}' not registered"


def test_registry_command_classes_have_required_attributes():
    """Test that all registered commands have required attributes."""
    from cypher_graphdb.cli.command_registry import registry

    for command_name, command_class in registry.get_all_commands().items():
        # Should have command_name attribute
        assert hasattr(command_class, "command_name")
        assert command_class.command_name == command_name

        # Should have execute method
        assert hasattr(command_class, "execute")
        assert callable(command_class.execute)

        # Should inherit from BaseCommand
        assert issubclass(command_class, BaseCommand)


def test_registry_consistency_with_imports():
    """Test that registry state is consistent after imports."""
    from cypher_graphdb.cli.command_registry import registry

    # Get initial state
    initial_commands = registry.get_all_commands()
    initial_count = len(initial_commands)

    # Re-import commands (should not duplicate)
    # Import is needed for test completeness
    import cypher_graphdb.cli.commands  # noqa: F401

    final_commands = registry.get_all_commands()
    final_count = len(final_commands)

    # Count should remain the same (no duplicates)
    assert final_count == initial_count

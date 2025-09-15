# Test Suite Documentation

## Overview

This directory contains the comprehensive test suite for the cypher-graphdb project. The tests are organized into logical subdirectories based on functionality areas to improve maintainability and test discovery.

## Directory Structure

```
tests/
├── README.md                   # This documentation
├── __init__.py                 # Test package initialization
├── mock_backend.py             # Shared mock backend for testing
├── cli/                        # CLI system tests
│   ├── __init__.py
│   ├── README.md              # CLI test documentation
│   ├── test_cmd_map.py        # Command mapping tests
│   └── test_command_registry.py # Command registry tests
├── tools/                      # Import/export tools tests
│   ├── __init__.py
│   ├── README.md              # Tools test documentation
│   ├── test_exporters.py      # CSV/Excel export tests
│   ├── test_resource_management.py # Resource cleanup tests
│   └── test_tabular_import.py  # CSV import tests
└── test_*.py                   # Core functionality tests
```

## Test Categories

### Core Functionality Tests (Root Level)

These tests cover the core graph database functionality:

- **test_backend_capabilities.py** - Backend capability validation
- **test_command_reader.py** - Command parsing and reading
- **test_cypherbuilder.py** - Cypher query building functionality
- **test_graph_id_zero.py** - Graph ID handling edge cases
- **test_graphops.py** - Graph operations and algorithms
- **test_modelinfo.py** - Model information and metadata
- **test_models.py** - Core data models (Graph, Node, Edge)
- **test_utils.py** - Utility functions and helpers

### CLI Tests (`cli/`)

Tests for the command-line interface system:

- **31 total tests** covering command registry and mapping
- Dynamic command registration system
- Command map generation from registry
- Pattern matching and validation
- Integration with actual command classes

### Tools Tests (`tools/`)

Tests for import/export functionality:

- **9 total tests** covering data exchange tools
- CSV and Excel export functionality
- CSV import with validation
- Resource management and cleanup
- DuckDB integration testing

## Running Tests

### All Tests
```bash
# Run complete test suite
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=cypher_graphdb --cov-report=html

# Run with verbose output
python -m pytest tests/ -v
```

### Category-Specific Tests
```bash
# CLI tests only
python -m pytest tests/cli/

# Tools tests only  
python -m pytest tests/tools/

# Core functionality tests only
python -m pytest tests/test_*.py
```

### Individual Test Files
```bash
# Specific test file
python -m pytest tests/test_models.py

# Specific test function
python -m pytest tests/test_models.py::test_label_to_object_type
```

## Test Patterns and Conventions

### Function-Based Tests
The project uses **plain test functions** rather than test classes, following pytest best practices:

```python
def test_feature_name():
    """Test description."""
    # Arrange
    setup_data = create_test_data()
    
    # Act
    result = function_under_test(setup_data)
    
    # Assert
    assert result == expected_value
```

### Fixtures and Test Data
Common test utilities and fixtures:

- **mock_backend.py** - Shared mock backend for database testing
- **@pytest.fixture** - Used for reusable test data and setup
- **Temporary files** - For file I/O testing with proper cleanup

### Naming Conventions
- Test files: `test_<module_name>.py`
- Test functions: `test_<feature_description>()`
- Clear, descriptive test names explaining what is being tested

## Test Quality Standards

### Coverage Expectations
- **Core models**: 100% line coverage expected
- **CLI system**: High coverage of public APIs
- **Tools**: Coverage of main import/export paths
- **Error handling**: Exception paths should be tested

### Test Organization
- **Arrange-Act-Assert** pattern
- **Single responsibility** - one concept per test
- **Independent tests** - no dependencies between tests
- **Fast execution** - prefer mocks over real I/O when possible

### Documentation
- Clear docstrings for test functions
- Comments explaining complex test scenarios
- README files for test subdirectories

## Mock and Test Utilities

### MockBackend (`mock_backend.py`)
Provides a test-friendly backend implementation:
- In-memory graph storage
- Predictable ID generation
- No external dependencies
- Shared across multiple test files

### Common Patterns
```python
# Using the mock backend
from .mock_backend import build_db

def test_graph_operations():
    db = build_db()
    # Test operations...

# Temporary file testing
def test_file_export(tmp_path):
    output_file = tmp_path / "test_output.csv"
    # Test file operations...
```

## Continuous Integration

Tests are designed to run reliably in CI environments:
- **No external dependencies** required
- **Deterministic execution** with predictable test data
- **Fast execution** suitable for frequent runs
- **Cross-platform compatibility** (Windows, macOS, Linux)

## Contributing to Tests

### Adding New Tests
1. Place tests in the appropriate subdirectory
2. Follow existing naming conventions
3. Use the shared mock backend when possible
4. Include docstrings and clear assertions
5. Update this README if adding new categories

### Test Maintenance
- Keep tests focused and independent
- Update tests when changing public APIs
- Remove or update tests for deprecated functionality
- Maintain test performance for CI efficiency

## Development Workflow

### Test-Driven Development
1. Write failing test for new feature
2. Implement minimum code to pass test
3. Refactor while keeping tests green
4. Add edge cases and error conditions

### Debugging Tests
```bash
# Run single test with detailed output
python -m pytest tests/test_models.py::test_specific_function -vvv

# Drop into debugger on failure
python -m pytest tests/test_models.py --pdb

# Show local variables in traceback
python -m pytest tests/test_models.py --tb=long
```

## Test Infrastructure Status

- **Total Tests**: ~149 tests across all categories
- **Organization**: ✅ Well-organized into logical subdirectories
- **Coverage**: ✅ Good coverage of core functionality
- **CI Integration**: ✅ Runs reliably in automated environments
- **Documentation**: ✅ Comprehensive documentation and examples

This test suite provides a solid foundation for maintaining code quality and preventing regressions as the project evolves.

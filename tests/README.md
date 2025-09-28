# Test Suite Documentation

This directory contains the comprehensive test suite for the Cypher GraphDB library, with **194 tests** covering all major components and functionality.

## Structure

```
tests/
├── conftest.py              # Global pytest configuration with auto-marking
├── unit/                    # Unit tests (193 tests)
│   ├── backends/           # Database backend tests  
│   ├── cli/                # Command-line interface tests (30 tests)
│   ├── tools/              # Import/export tools tests (7 tests)
│   ├── utils/              # Utility functions tests (51 tests)
│   └── *.py               # Core library tests (105 tests)
├── integration/            # Integration tests (1 test)
└── README.md              # This file
```

## Test Categories

### Unit Tests (193 tests)
- **CLI Module** (30 tests): Command mapping, registry, and command parsing
- **Core Library** (105 tests): Models, graph operations, builders, backends  
- **Tools** (7 tests): CSV/Excel exporters, tabular import functionality
- **Utils** (51 tests): Comprehensive utility function coverage across 5 modules
  - Collection utilities (16 tests)
  - Connection utilities (13 tests) 
  - String utilities (15 tests)
  - Core utilities (5 tests)
  - Conversion utilities (2 tests)

### Integration Tests (1 test)
- **End-to-end** functionality testing with external dependencies

## Running Tests

### All Tests
```bash
task test            # Run unit tests only (default)
task test:all        # Run all tests including integration
```

### Specific Categories
```bash
# Run only unit tests
.venv/bin/python -m pytest tests/unit/ -v
# or with uv
uv run python -m pytest tests/unit/ -v

# Run only integration tests  
.venv/bin/python -m pytest tests/integration/ -v -m integration
# or with uv
uv run python -m pytest tests/integration/ -v -m integration

# Run specific module tests
.venv/bin/python -m pytest tests/unit/utils/ -v
.venv/bin/python -m pytest tests/unit/cli/ -v
.venv/bin/python -m pytest tests/unit/tools/ -v
```

### Coverage Analysis
```bash
task fct             # Format, check, and test
.venv/bin/python -m pytest tests/unit/ --cov=cypher_graphdb --cov-report=html
# or with uv
uv run python -m pytest tests/unit/ --cov=cypher_graphdb --cov-report=html
```

## Test Configuration

### Automatic Test Marking
Tests are automatically marked based on directory location via `conftest.py`:
- `tests/unit/` → `@pytest.mark.unit`  
- `tests/integration/` → `@pytest.mark.integration`
- Default → `@pytest.mark.unit`

### Dependencies
- **Unit tests**: No external dependencies
- **Integration tests**: Requires `testcontainers` for Docker-based testing

## Utils Test Coverage

The utils module has **100% function coverage** with 51 tests across all utility functions:

| Module | Tests | Functions Tested |
|--------|-------|------------------|
| `collection_utils` | 16 | Dictionary operations, nested data handling, list processing |
| `connection_utils` | 13 | URI parsing, protocol validation, logging sanitization |
| `string_utils` | 15 | String parsing, template resolution, type conversion |
| `core_utils` | 5 | Path handling, file format detection, type checking |
| `conversion_utils` | 2 | Type defaults, NaN detection |

## Recent Updates

- ✅ **Complete utils refactoring**: Split monolithic `utils.py` into modular structure
- ✅ **Test migration**: Migrated from single `test_utils.py` to modular test files
- ✅ **100% coverage**: Added tests for all 41 exported utility functions
- ✅ **Documentation**: Comprehensive function documentation with examples

## Best Practices

1. **Test Organization**: Tests mirror source code structure
2. **Naming Convention**: `test_<function_name>` for function tests
3. **Documentation**: All test functions include docstrings
4. **Coverage**: Aim for 100% function coverage in critical modules
5. **Markers**: Use pytest markers for test categorization
6. **Fixtures**: Shared test data in `conftest.py` files

## Contributing

When adding new functionality:
1. Write tests first (TDD approach)
2. Ensure tests follow existing naming patterns
3. Add docstrings to test functions
4. Run full test suite before committing
5. Update this README if adding new test categories

## Test Commands Reference

```bash
# Development workflow
task fct                    # Format, check, and test
task format                 # Format code with ruff
task check                  # Lint code with ruff
task test                   # Run unit tests only

# Specific test runs (using virtual environment)
.venv/bin/python -m pytest tests/unit/utils/ -v                    # Utils tests
.venv/bin/python -m pytest tests/unit/ -k "test_connection"        # Connection-related tests
.venv/bin/python -m pytest tests/ --collect-only                   # List all tests
.venv/bin/python -m pytest tests/ -x                               # Stop on first failure

# Alternative with uv
uv run python -m pytest tests/unit/utils/ -v                       # Utils tests
uv run python -m pytest tests/unit/ -k "test_connection"           # Connection-related tests
uv run python -m pytest tests/ --collect-only                      # List all tests
uv run python -m pytest tests/ -x                                  # Stop on first failure

# With activated virtual environment
source .venv/bin/activate && python -m pytest tests/unit/ -v       # After activation
```
# Contributing to CypherGraphDB Core

Thank you for considering contributing to CypherGraphDB Core! This document outlines the process for contributing to the project and provides guidelines to make the process smooth for everyone involved.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone.

## Getting Started

### Prerequisites

- Python 3.13+
- Task runner (`task`)
- uv package manager

### Setting Up Development Environment

1. Fork the repository
2. Clone your fork: `git clone https://github.com/petrarca/cypher-graphdb-core.git`
3. Set up the development environment:
   ```bash
   cd cypher-graphdb-core
   task install
   ```

## Development Workflow

### Branch Naming

Use descriptive branch names with prefixes:
- `feature/` for new features
- `fix/` for bug fixes
- `docs/` for documentation changes
- `refactor/` for code refactoring

Example: `feature/add-neo4j-support`

### Coding Standards

- Follow PEP 8 style guidelines
- Use ruff for linting and formatting
- Add type annotations to all functions and methods
- Write tests for new functionality
- Document your code using Google-style docstrings (see [Documentation Guide](docs/documentation-guide.md))

### Testing

Run tests before submitting a pull request:

```bash
task test
```

Ensure all tests pass and aim for good test coverage.

### Documentation

- Update documentation for any changes to the API
- Follow the [Documentation Guide](docs/documentation-guide.md) for docstring format
- Build and check documentation locally:
  ```bash
  task build:docs
  task serve:docs
  ```

## Pull Request Process

1. Update the README.md and documentation with details of changes if applicable
2. Run the linter and tests to ensure code quality: `task fct`
3. Submit a pull request with a clear description of the changes
4. Address any feedback from code reviews

## Commit Messages

Write clear, concise commit messages that explain the changes made. Follow the conventional commits format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types include: feat, fix, docs, style, refactor, test, chore

Example: `feat(cli): add support for interactive mode`

## Reporting Bugs

When reporting bugs, include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Environment details (OS, Python version, etc.)

## Feature Requests

Feature requests are welcome! Please provide:

- A clear description of the feature
- The motivation for the feature
- Any potential implementation details

## License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.

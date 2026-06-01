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

## Releasing

The package version is derived from Git tags via `setuptools-scm`. There is no
version field to edit in `pyproject.toml`. Creating a tag on `main` is the release.

Pushing the tag triggers two GitHub Actions workflows automatically:
- `publish.yml` -- lints, runs unit tests, builds, and publishes to PyPI (OIDC trusted publishing)
- `release.yml` -- creates a GitHub Release with auto-generated notes

### Version numbering

Follow [Semantic Versioning](https://semver.org/):

| Change | Bump | Example |
|--------|------|---------|
| Bug fixes, minor improvements, docs, tests | Patch | `v0.2.9` -> `v0.2.10` |
| New features, backward-compatible additions | Minor | `v0.2.10` -> `v0.3.0` |
| Breaking changes to API or schema | Major | `v0.3.0` -> `v1.0.0` |

### Step-by-step release process

**1. Ensure `main` is clean and all checks pass.**

```bash
git checkout main && git pull
task fct
```

**2. Check what has changed since the last release.**

```bash
git log --oneline $(git describe --tags --abbrev=0)..HEAD
git tag --sort=-v:refname | head -3
```

**3. Create and push the tag.**

```bash
git tag v0.x.y
git push origin v0.x.y
```

This triggers both CI workflows. The package is published to PyPI automatically
once lint and unit tests pass.

**4. Update the GitHub release notes.**

The release workflow auto-generates notes from commit messages. Edit them to be
user-facing -- focus on what changed and why, not implementation details:

```bash
gh release edit v0.x.y --notes "
- feat: add merge_keys to @node decorator for business-key MERGE
- feat: bulk_delete_orphans for fast orphan-node GC
- fix: preserve gid_ on business-key MERGE
"
```

**5. Wait for the publish workflow to complete.**

```bash
gh run list --limit 3
gh run view <run-id> --log   # on failure
```

**6. Verify the package is on PyPI.**

```bash
pip index versions cypher-graphdb
```

### Deleting a bad tag

```bash
git tag -d v0.x.y
git push origin :refs/tags/v0.x.y
# fix, then re-tag
git tag v0.x.y
git push origin v0.x.y
```

---

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

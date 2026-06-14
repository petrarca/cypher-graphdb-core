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

Every step is a `task` command -- no manual git/gh incantations to remember. The
release tasks share the same names across all petrarca repos
(`release:check`, `release`, `release:verify`, `release:abort`).

**1. Push your work and run integration tests.**

The pre-commit hook already runs format + lint + unit tests on every commit, so
those are not repeated here. The release adds the one gate the hooks and CI do
**not** cover -- integration tests against live backends:

```bash
git push                    # tag must point at pushed HEAD (release:check enforces this)
task test:integration       # needs docker: Memgraph + AGE (via testcontainers)
```

**2. Preflight check (no side effects).**

```bash
task release:check
```

This verifies you are on `main`, the tree is clean, and local `main` is in sync
with `origin/main`, then prints the last tag and every commit since it -- use
that to pick the next version (see the SemVer table above).

**3. Cut the release.**

```bash
task release -- v0.3.0
```

This re-runs `release:check`, validates the version (`vX.Y.Z`, not already a
tag), then creates and pushes an annotated tag. Pushing the tag triggers both CI
workflows: `publish.yml` (lint + unit + build + publish to PyPI via OIDC) and
`release.yml` (GitHub Release with auto-generated notes).

**4. Verify it published.**

```bash
task release:verify -- v0.3.0
```

Watches the publish workflow and shows the versions available on PyPI.

**5. (Optional) Polish the GitHub release notes.**

The release workflow auto-generates notes from commit messages. Edit them to be
user-facing -- focus on what changed and why, not implementation details:

```bash
gh release edit v0.3.0 --notes "
- feat: add merge_keys to @node decorator for business-key MERGE
- feat: bulk_delete_orphans for fast orphan-node GC
- fix: preserve gid_ on business-key MERGE
"
```

### Deleting a bad tag

```bash
task release:abort -- v0.3.0
# fix the issue, commit, push, then re-run: task release -- v0.3.0
```

> A PyPI release that has **already published** cannot be overwritten -- if the
> package made it to PyPI, bump to the next patch version instead of re-tagging.

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

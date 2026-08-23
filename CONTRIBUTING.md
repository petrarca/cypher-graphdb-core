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
- Add type annotations to all functions and methods; checked with mypy (`task typecheck`)
- Write tests for new functionality
- Document your code using Google-style docstrings (see [Docstrings](#docstrings))

### Testing

Run tests before submitting a pull request:

```bash
task test
```

Ensure all tests pass and aim for good test coverage.

### Documentation

Documentation lives in [`docs/`](docs/README.md) as plain markdown with
[OKF](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
frontmatter. There is **no documentation build** — no mkdocs, no generated API
reference, no site to publish. Read the files, edit the files.

- `docs/usage/` — task-oriented guides for people *using* the library. Update
  these when you change public behaviour.
- `docs/design/` — architecture and the *why*. Every doc carries `status` (is the
  document trustworthy?) and `implementation` (does the thing exist?). Keep both
  honest: a design that ships moves to `implementation: implemented`, and the
  body sheds whatever the code now owns.
- `docs/adr/` — decision records, for direction changes.

Because there is no generated reference, **docstrings are the API
documentation**. Treat them accordingly.

New files need OKF frontmatter; see the
[documentation standard](https://github.com/petrarca/engineering-handbook/blob/main/standards/sdlc/documentation.md).

#### Docstrings

Google-style, on every public module, class, and function. `task check`
enforces their presence on public packages, modules and classes (`D100`, `D101`,
`D104`).

```python
def fetch_nodes(criteria: dict | MatchCriteria, unnest_result: bool = False) -> Any:
    """Fetch nodes matching the given criteria.

    Extended description covering behaviour, edge cases, or backend
    differences worth knowing at the call site.

    Args:
        criteria: Match criteria — an ID, a GID, a property dict, or a
            MatchCriteria instance.
        unnest_result: Collapse trivial nesting in the result.

    Returns:
        A list of result rows, or the bare object when unnesting collapses
        a single match.

    Raises:
        QueryExecutionError: If the backend rejects the query.
    """
```

Conventions:

- **Type annotations carry the types** — do not repeat them in `Args:`.
- Document **`Raises:`** whenever a caller could reasonably be expected to catch
  it.
- Add an **`Example:`** for anything with a non-obvious calling convention.
- Say what is **backend-specific**. A behavioural difference between AGE and
  Memgraph belongs in the docstring, not only in a design doc.
- Keep them in sync with the code in the same commit.

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

`MAJOR.MINOR.PATCH`, but the major component is **not** strict SemVer:

| Change | Bump | Example |
|--------|------|---------|
| Bug fixes, docs, tests, internal cleanups | Patch | `v0.5.0` -> `v0.5.1` |
| Features, refactors, packaging and dependency changes -- **breaking changes included** | Minor | `v0.5.0` -> `v0.6.0` |
| A new generation, or a substantial body of new capability | Major | `v0.6.0` -> `v1.0.0` |

A breaking change does not force a major bump. **A patch release never breaks
anything** -- that is the hard rule. Because a minor bump carries no warning of
its own, breaking changes must be spelled out in the release notes with the
migration a consumer needs.

See the [org standard](https://github.com/petrarca/engineering-handbook/blob/main/standards/sdlc/releases.md).

### Step-by-step release process

Every step is a `task` command -- no manual git/gh incantations to remember. The
release tasks share the same names across all petrarca repos
(`release:check`, `release`, `release:verify`, `release:abort`).

**1. Push your work and run integration tests.**

The pre-commit hook formats + lints + type-checks + runs unit tests on every commit.
`task release:check` (next step) re-verifies lint and format **read-only**
(`task verify` -- it never reformats, so it cannot dirty the tree mid-release).
The one gate neither the hooks nor CI cover is integration tests against live
backends, so run them now:

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

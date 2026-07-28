#!/usr/bin/env bash
#
# Read-only lint/format/lock verification (NO mutation). CANONICAL TEMPLATE.
#
# Used by `task verify` and `task release:check`. Unlike `task fct`/`task format`
# (which run the formatter and mutate files), this only *checks* — it exits
# non-zero on a violation but never rewrites a file, so it can never dirty the
# tree mid-release.
#
# Shape: Python library (src/ + tests/), single uv project.
set -euo pipefail

# --- Python single project (server/ or a lib) ------------------------------
.venv/bin/ruff format --check src tests
.venv/bin/ruff check src tests
uv lock --no-sources --check

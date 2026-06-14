#!/usr/bin/env bash
#
# Read-only lint + format verification (NO mutation).
#
# Used by `task verify` and `task release:check`. Unlike `task format`/`task fct`
# (which run `ruff format` and mutate files), this only *checks* -- it exits
# non-zero on a violation but never rewrites a file, so it can never dirty the
# tree mid-release.
set -euo pipefail

.venv/bin/ruff format --check src tests
.venv/bin/ruff check src tests

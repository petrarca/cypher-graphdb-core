# shellcheck shell=bash
#
# Per-repo release config — the ONLY file this repo edits.
# `release.sh` and `verify.sh` are copied unchanged from the handbook
# (engineering-handbook/templates/scripts); re-copy them to take shared updates.
#
# cypher-graphdb-core is a Python library: the git tag IS the version
# (setuptools-scm), so there is nothing to bump in pyproject.toml.

# How the repo publishes on a tag:
#   pypi    — PyPI package(s) via a publish workflow
#   npm     — npm package(s) via a publish workflow
#   docker  — GHCR image(s); no package registry
RELEASE_MODE="pypi"

# The CI workflow a `v*` tag triggers to publish/build. `release:verify` watches
# the latest run of this workflow.
PUBLISH_WORKFLOW="publish.yml"

# Registry package names to report in `release:verify` (pypi/npm modes only).
PACKAGES=("cypher-graphdb")

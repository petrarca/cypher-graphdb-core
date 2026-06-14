#!/usr/bin/env bash
#
# Shared release helper (petrarca standard interface).
#
# Subcommands (called from the Taskfile -- see `task release:*`):
#   check                 git-state preflight (on main, clean, synced). No side effects.
#   tag <vX.Y.Z>          validate version, create annotated tag, push -> CI publishes.
#   verify <vX.Y.Z>       watch publish workflow + show published versions.
#   abort <vX.Y.Z>        delete a bad tag (local + remote).
#
# Only the CONFIG block below differs between repos. Read-only lint/format/lock
# verification lives in `task verify` and is invoked by the Taskfile separately,
# so this script stays package-manager agnostic.
set -euo pipefail

# --- CONFIG (per repo) ------------------------------------------------------
# Packages to check on the registry during `verify` (PyPI for Python repos).
PACKAGES=("cypher-graphdb")
REGISTRY="pypi"   # pypi | npm
# ---------------------------------------------------------------------------

err()  { echo "ERROR: $*" >&2; exit 1; }

require_version() {
  [ -n "${1:-}" ] || err "version required. Usage: task ${2} -- vX.Y.Z"
  echo "$1" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$' || err "version must look like vX.Y.Z (got '$1')."
}

cmd_check() {
  local branch ahead behind last
  branch=$(git rev-parse --abbrev-ref HEAD)
  [ "$branch" = "main" ] || err "releases must be cut from 'main' (on '$branch')."
  if [ -n "$(git status --porcelain)" ]; then
    git status --short; err "working tree is dirty. Commit or stash first."
  fi
  git fetch --quiet origin main --tags
  ahead=$(git rev-list --count origin/main..HEAD)
  behind=$(git rev-list --count HEAD..origin/main)
  [ "$behind" = "0" ] || err "local main is $behind commit(s) behind origin/main. Run 'git pull' first."
  [ "$ahead" = "0" ]  || err "local main is $ahead commit(s) ahead of origin/main. Run 'git push' first (tag must point at pushed HEAD)."
  last=$(git describe --tags --abbrev=0 2>/dev/null || echo "<none>")
  echo "OK: on main, clean, in sync with origin/main."
  echo "Last release tag: $last"
  echo "Commits since $last:"
  git log --oneline "$last"..HEAD 2>/dev/null || git log --oneline -10
  echo ""
  echo "Reminder: run 'task test:integration' before releasing (needs docker backends)."
  echo "Then: task release -- vX.Y.Z"
}

cmd_tag() {
  local ver="${1:-}"
  require_version "$ver" "release"
  git rev-parse "$ver" >/dev/null 2>&1 && err "tag '$ver' already exists. Use 'task release:abort -- $ver' first."
  echo "Tagging $ver at $(git rev-parse --short HEAD) and pushing..."
  git tag -a "$ver" -m "Release $ver"
  git push origin "$ver"
  echo "Pushed $ver. CI will publish and create the GitHub release."
  echo "Track with: task release:verify -- $ver"
}

cmd_verify() {
  local ver="${1:-}"
  require_version "$ver" "release:verify"
  echo "Recent workflow runs:"
  gh run list --limit 5 || true
  echo ""
  echo "Watching the latest publish run (Ctrl-C to stop watching)..."
  gh run watch "$(gh run list --workflow=publish.yml --limit 1 --json databaseId --jq '.[0].databaseId')" || true
  echo ""
  for name in "${PACKAGES[@]}"; do
    if [ "$REGISTRY" = "npm" ]; then
      echo "npm versions for $name:"; npm view "$name" version 2>/dev/null || echo "  (check https://www.npmjs.com/package/$name)"
    else
      echo "PyPI versions for $name:"; pip index versions "$name" 2>/dev/null || echo "  (check https://pypi.org/project/$name/)"
    fi
  done
}

cmd_abort() {
  local ver="${1:-}"
  require_version "$ver" "release:abort"
  echo "Deleting tag $ver locally and on origin..."
  git tag -d "$ver" 2>/dev/null || echo "(no local tag $ver)"
  git push origin ":refs/tags/$ver" 2>/dev/null || echo "(no remote tag $ver)"
  echo "Done. Note: a published release CANNOT be overwritten -- bump to the next version instead."
}

case "${1:-}" in
  check)  cmd_check ;;
  tag)    cmd_tag "${2:-}" ;;
  verify) cmd_verify "${2:-}" ;;
  abort)  cmd_abort "${2:-}" ;;
  *) err "unknown subcommand '${1:-}'. Use: check | tag | verify | abort" ;;
esac

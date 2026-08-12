---
type: Concept
title: Packaging — a lean core with optional extras
description: Why the terminal-UI stack and Excel support are optional, the invariant that importing the library must not pull them in, and the two ways eager re-exports broke it.
tags: [cypher-graphdb, packaging, extras, dependencies, cli, imports]
status: stable
implementation: implemented
---

# Packaging — a lean core with optional extras

> **Implemented.** Extras declared in `pyproject.toml`; guards in `_extras.py`,
> used by `args.py`, `cli/renderer.py` and the lazy `__getattr__` in
> `cli/__init__.py` and `tools/__init__.py`.

## Problem

The library is embedded in services — a FastAPI server, an MCP server, an
indexing pipeline — that never render a table or read a keystroke. They were
nonetheless installing `typer`, `rich`, `prompt_toolkit`, `art` and `openpyxl`,
because a single dependency list served both the library and its CLI.

`numpy` was there too, for one line: `isinstance(value, float) and
np.isnan(value)`. Twenty-four megabytes of wheel for an expression the standard
library's `math.isnan` computes identically, given the `isinstance` guard.

## Design

| Install | Adds | For |
|---|---|---|
| `cypher-graphdb` | core + both backend drivers | embedding the library |
| `cypher-graphdb[excel]` | `openpyxl` | Excel import/export |
| `cypher-graphdb[cli]` | `typer`, `rich`, `prompt_toolkit`, `art`, `lark` (+ `[excel]`) | `cypher-graphdb` |

Both backend drivers stay in core: there is no per-backend extra, because a
backend is the reason to install the library at all. `[cli]` depends on
`[excel]`, so nothing changes for a CLI user.

### The invariant

**`import cypher_graphdb` must not import `typer`, `rich`, `prompt_toolkit`,
`art` or `openpyxl`.**

This is the whole contract, and it is easy to break invisibly: an editable
development install always has those packages, so a violation shows up only in a
published core-only install. It is asserted directly:

```python
import sys; before = set(sys.modules); import cypher_graphdb
assert not {"rich", "typer", "prompt_toolkit", "art"} & {m.split(".")[0] for m in set(sys.modules) - before}
```

### Failing usefully

A missing extra should name the install, not a package the user never heard of.
`_extras.require_extra(extra, exc)` builds that message, and it is raised at the
points a consumer actually reaches: the console-script entry point, and the
modules that consumers import directly.

### Eager re-exports are the trap

Twice, a package `__init__` re-exporting its headline symbol pulled in far more
than the caller asked for:

- `cli/__init__.py` eagerly imported `CypherGraphCLI` from `.app`. Three
  downstream projects import `cypher_graphdb.cli.renderer`, which needs only
  `rich` — but the eager re-export dragged in `prompt_toolkit` and `art` too.
- `tools/__init__.py` eagerly re-exported the Excel classes, so the CSV path
  required `openpyxl`.

Both are now lazy (PEP 562 `__getattr__`). The rule generalises: **importing a
leaf module should cost what that leaf needs**, so a package `__init__` in a
tree with optional dependencies must not eagerly import its heaviest member.

Laziness has a cost worth knowing: a lazy `__init__` no longer raises on a
submodule's behalf, so a module on the consumer-facing surface — `cli/renderer.py`
— carries its own guard.

## Consequences

A core-only install is 22 packages with no terminal-UI stack; `[cli]` is 34.
Consumers embedding the library get a smaller image, and `numpy` is gone
entirely.

The cost is that a consumer importing `cypher_graphdb.cli.renderer` — several
do, for its `ResultRenderer` — must now declare `cypher-graphdb[cli]`. That is
accurate rather than incidental: they are using CLI code. Promoting the renderer
into the core was rejected, since it would return `rich` — the heaviest of the
four — to every install. If a headless renderer is ever wanted, the answer is a
`rich`-free formatting layer underneath it, not relocating the Rich one.

This is a breaking change for anyone who installed `cypher-graphdb` and expected
`cypher-graphdb`.

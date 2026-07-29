---
type: Guide
title: The cgdb-cli command line
description: Running the interactive REPL and non-interactive scripts with cgdb-cli — flags, output formats, the command set, and standalone schema generation.
tags: [cypher-graphdb, cli, cgdb-cli, repl, interactive, scripting]
status: stable
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# The `cgdb-cli` command line

Installing the library puts **`cgdb-cli`** on your `PATH`. It works identically
across backends.

## Three ways to run it

```bash
# 1. Interactive REPL
cgdb-cli --graph my_graph

# 2. Execute and exit — multiple commands separated by ';'
cgdb-cli --graph my_graph -e "labels"

# 3. Run a file of commands
cgdb-cli --graph my_graph -f setup.cgdb --verbose
```

Connection details come from the environment, a `.env` file, or flags:

```bash
CGDB_CINFO=postgresql://postgres:postgres@localhost:8432/graphdb \
CGDB_BACKEND=age \
  cgdb-cli --graph my_graph --table -e "labels"
```

## Flags

| Flag | Meaning |
|---|---|
| `-b, --backend` | `age` or `memgraph` |
| `-c, --cinfo` | Connection string |
| `-g, --graph` | Graph name |
| `-e, --execute` | Run the given command(s) and exit |
| `-f, --file` | Run commands from a file |
| `-j, --json` | JSON output |
| `-t, --table` | Table output |
| `-r, --read-only` | Reject writes |
| `-m, --model-path` | Load typed models from a file or package on startup |
| `--ignore-model-path` | Ignore `CGDB_MODEL_PATH` |
| `-y, --yes` | Auto-confirm destructive operations |
| `-l, --log-level` | `INFO`, `DEBUG`, `TRACE`, … |
| `--verbose` | Echo each command and print a summary (file mode) |
| `--no-progress` | No progress bar (file mode) |
| `-v, --version` | Print version and exit |

Output defaults to **table** interactively and **JSON** non-interactively.
`json`, `table` and `list` also work as runtime commands inside the REPL.

Anything that is not a recognised command is executed as Cypher.

## Commands

**Connection & transactions** — `connect`, `disconnect`, `commit`, `rollback`

**Graphs** — `graphs`, `create graph <name>`, `drop graph <name>`,
`use graph <name>` (or `use <name>`), `graph exists <name>`

**Inspection** — `labels`, `indexes`, `stats` / `statistics`, `backends`,
`config`

**Reading** — `fetch nodes`, `fetch edges`, `fetch all`, `fetch`,
`resolve edges`, `search`, `tree`

**Writing** — `create node`, `create edge`, `create linked node`, `update`,
`delete`

**Models & schema** — `models`, `load`, `schema`

**Import / export** — `import`, `import graph`, `export`, `export graph`

**Escape hatches** — `sql` (raw backend SQL), `exec` / `execute`

**Session** — `set`, `get`, `add`, `clear`, `help`, `exit` (`quit`, `q`, `bye`),
`!` / `!!` (shell), `$`, `.`, `_`

`help` lists everything; `help <command>` explains one.

Destructive commands (`create graph`, `drop graph`, …) prompt for confirmation
and require `--yes` when running non-interactively.

## Tab completion

The REPL completes commands, graph names, labels and label properties. Commands
declare what they complete, so completion covers new commands automatically.

## Typed models in the CLI

Point the CLI at your models and results hydrate into them:

```bash
cgdb-cli --model-path ./graph_models/ --graph mygraph
```

The path is a Python file or a package directory; providers declared in the
loaded modules are discovered and adopted. `CGDB_MODEL_PATH` does the same, and
`--ignore-model-path` suppresses it. See
[typed-models.md](typed-models.md).

## Schema generation without a database

`schema generate` is a subcommand, not a REPL command, and needs no connection:

```bash
cgdb-cli schema generate -m ./graph_models/ -o ./schemas/
```

See [typed-models.md](typed-models.md) for the options.

## Scripting

Non-interactive mode makes the CLI usable from shell scripts and CI:

```bash
cgdb-cli --graph my_graph --json -e "indexes" | jq '.[].label'
cgdb-cli --graph my_graph -e "create graph staging" --yes
```

Prefer `--json` when the output is being parsed — table output is for humans and
its column widths are not a contract.

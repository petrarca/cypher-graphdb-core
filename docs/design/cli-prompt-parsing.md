---
type: Concept
title: Parsing the CLI prompt
description: How the prompt line and argument list are parsed by grammar, why command-token matching stays hand-written, and how a command declares where it may sit in a pipeline.
tags: [cypher-graphdb, cli, parser, lark, grammar, completion, pipeline]
status: stable
implementation: implemented
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Parsing the CLI prompt

> **Built.** `cli/promptgrammar.py` (two Lark grammars), `cli/promptparser.py`
> (token matching, sequence validation), `cli/commandposition.py`.

## The language

```
line      := pipeline [ "||" shell_command ]
pipeline  := command { "|" command }
command   := <multi-word token, longest-prefix match> [ arglist ]
arglist   := item { "," item }
item      := value | key "=" value
value     := python literal | bare word
```

## Problem

All of it was hand-parsed, across three quote-and-escape state machines that had
diverged: one splitting on `||`, one splitting on `|`, one parsing arguments.
They disagreed, and the disagreement was a live bug — a backslash inside a
quoted value was handled by the argument scanner but **consumed** by the
splitter, so `parse_prompt` corrupted input that `parse_cmd` handled correctly.

Separately, each command's behaviour was encoded as punctuation around its
action name: `"[[format_output]]_"` meant first-or-last position and no backend
required. Nothing type-checked that spelling, and five sites across three
functions decoded it.

## Design

### Behaviour is declared, not spelled

Three orthogonal flags, explicit on the command class:

| Flag | Default | Meaning |
|---|---|---|
| `position` | `FIRST` | Where it may sit in a pipeline |
| `requires_backend` | `True` | Whether a connection must exist |
| `splits_args` | `True` | `False` keeps the remainder opaque (raw SQL) |

`CommandPosition` — `ANY`, `FIRST`, `LAST`, `FIRST_OR_LAST` — expresses the
pipeline rule directly: a producer is `FIRST`, a consumer is `LAST`, a renderer
is `ANY`. A lone command satisfies every position. It lives in its own module
because the parser enforces it and `base_command` declares it, and importing
either from the other is circular.

### Grammars where a grammar helps

Two Lark grammars replace the scanners: one splits the line into pipeline
segments plus a shell command, the other parses an argument list. Quoting,
escaping and nested collections become rules rather than four mutable flags,
which is what stops the three implementations from drifting apart again.

**Splitting is lossless.** Segments are returned as raw text, escapes intact —
unescaping belongs to the value parser alone. That separation is what the old
code got wrong.

### What deliberately stays hand-written

Matching the leading command tokens. The completer parses **half-typed input**,
where a grammar cannot lex a command name that is not finished: `creat` is not
yet `create node`. Lark's `parse_interactive`/`accepts()` reports which
*terminals* are acceptable at a clause boundary, which helps for "is a kwarg
legal here?" but cannot complete a partial token. Prefix matching answers that
directly, so it stays.

For the same reason the completer's cursor scanning stays. A grammar supplies
structure; the value providers — which labels, which properties, which graph
names exist — are domain logic no grammar reaches.

## Two invariants that are easy to break

Both were introduced during the migration to Lark, and neither was caught by the
unit tests. They were found by diffing the CLI's output against the previous
build on a live graph.

1. **Positional arguments stay raw strings.** `create edge WORKS_FOR, <id>,
   <id>` passes internal ids positionally, and `PromptParserCmd.args` is
   `list[str]`; evaluating them as literals turns ids into `int` and fails the
   command. Only keyword values — and the elements of a collection inside one —
   are evaluated. The characterization corpus missed this because every
   positional in it was a bare word, which looks the same either way.
2. **A kwarg with no value yet is dropped.** `fetch nodes Person, empty=`
   reaches the parser while the user is typing; binding it to `""` turns a
   half-typed filter into one that matches nothing.

## Consequences

254 lines of hand-rolled scanning became two grammars, the five punctuation
decode sites became zero, and the divergent-scanner bug class is gone by
construction.

It also removed an accidental coupling: entries were ordered by *action-name*
length for longest-prefix matching, which only worked because names mirrored
their tokens. Ordering is now by longest token, so renaming a command cannot
silently break `create linked node`.

The costs are a `lark` dependency in `[cli]`, and a parser that must stay
tolerant of incomplete input because the completer depends on it.

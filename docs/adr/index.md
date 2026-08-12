---
type: ADR Index
title: Architecture Decision Records
description: Index of decision records for cypher-graphdb-core — the why behind direction changes.
tags: [cypher-graphdb, adr, decisions, index]
status: stable
---

# Architecture Decision Records

ADRs record **direction changes** — a decision that alters how the library
works, and the reasoning that justified it. A design doc says *what* the design
is; the ADR says *why* it changed.

- Numbered sequentially, kebab-case: `NNNN-title.md`.
- `decision`: `proposed` · `accepted` · `superseded` · `rejected`.
- A superseded ADR keeps its file and names its replacement in `superseded_by`.

Org-wide decisions that govern every repo live in the
[engineering handbook](https://github.com/petrarca/engineering-handbook/blob/main/adrs/README.md)
(internal); this directory is for decisions local to `cypher-graphdb-core`.

## Index

| ADR | Title | Decision |
|---|---|---|
| [0001](0001-explicit-model-providers.md) | Explicit `ModelProvider` instances instead of a global model registry | accepted |

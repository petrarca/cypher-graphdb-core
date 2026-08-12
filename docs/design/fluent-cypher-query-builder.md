---
type: Design
title: Fluent Cypher query builder
description: An optional, opt-in fluent builder for constructing Cypher queries that terminates in the (cypher, params) pair the library already accepts.
tags: [cypher-graphdb, query-builder, fluent, cypher, parameterized]
status: stable
implementation: implemented
---

# Fluent Cypher query builder

> **Implemented.** Lives in `src/cypher_graphdb/cypherquery/` (`builder.py`,
> `patterns.py`, `params.py`). It is an internal layer — deliberately not
> exported from the package root — so the public surface stays Cypher-first.
> The method set is documented by its docstrings; this document keeps the
> reasoning and the invariants.
>
> One part was designed but not built: [backend-aware dialect
> emission](#backend-aware-dialect-emission-not-built).

`CypherQuery` is an **optional** chainable builder, analogous to SQLAlchemy
Core. It sits *above* the execution machinery and terminates in the contract the
library already accepts: a `(cypher, params)` pair, or a `ParsedCypherQuery`. It
is purely additive — raw Cypher strings, `MatchCriteria` objects and the typed
`create_or_merge()` API are unchanged and nobody has to adopt it.

## Problem

Query construction had three shapes, none of them chainable: raw Cypher strings
passed to `execute()`, flat `MatchNodeCriteria` / `MatchEdgeCriteria` models set
all at once by constructor kwargs, and the internal `CypherBuilder` — classmethod
factories covering single-node and single-edge patterns only, unable to compose
an incremental `WHERE`, an ordering or a multi-hop traversal.

When the *shape* of a query depended on runtime input — an optional filter, a
direction, a variable list of relationship types — consumers fell back to
f-string assembly. That forced each call site to re-implement Cypher literal
escaping, interpolate identifiers instead of binding them, and join `WHERE`
fragments with `" AND ".join(...)`:

```python
escaped = ", ".join(f"'{t.replace(chr(39), chr(39) * 2)}'" for t in rel_types)
query = f"MATCH {pattern} WHERE id(center) = {node_id} AND type(e) IN [{escaped}] ..."
```

Three defects in two lines: `node_id` is interpolated rather than bound,
`rel_types` needs hand-rolled escaping, and the predicate is concatenated. Every
project that hit this re-invented a small query builder ad hoc, each with its own
regex "safety" guards.

## Prior art

**SQLAlchemy Core is the primary reference**, and four of its decisions are
adopted directly:

- **Generative API.** `Select.where()` returns a *new* selectable joined to the
  prior clause with `AND`. Ours does the same.
- **Statement separated from rendering.** `str(stmt)` renders with placeholders
  intact; `compile_kwargs={"literal_binds": True}` produces the inlined,
  human-readable form. That is the precedent for `str(q)` versus
  `build(literal_binds=True)`, and for a future dialect-aware terminal.
- **Bound parameters plus auto-wrap.** `bindparam()` is first-class, yet a plain
  value in a comparison auto-binds. Hence `param(name)` alongside automatic
  binding in `where_eq` / `where_id` / `where_in`.
- **A raw escape hatch.** `text("… WHERE id = :user_id")` composes into a larger
  statement; `raw(fragment, **binds)` is the direct analogue.

Cypher-specific builders were cross-checked for what to *avoid*:

| Library | Borrowed | Avoided |
|---|---|---|
| **cypher-dsl** (Neo4j, Java) | Immutable/generative model; expression tree separated from rendering; bound-parameter bias | Full typed node/relationship object model — too heavy as the only input form |
| **cymple** (Python) | `build() -> (text, params)` terminal; named params via `param(...)` | Mandatory entity-type definitions to get ergonomics |
| **pypher** (Python) | Lightweight, Pythonic feel; auto-named params | Dynamic-attribute "magic" and a mutable token stream, which make reuse and introspection fragile |
| **neomodel** (OGM) | Django-style `.filter().order_by()` reads cleanly | OGM coupling; its escape hatch is raw strings with no structured binding |

The builder takes **raw pattern fragments** as its base input rather than
requiring a pattern object model. `cypherparser.py` already validates when
validation is wanted (via `to_parsed()`), a raw-fragment surface is far smaller,
and it matches how consumers already think about patterns. Typed `node()` /
`rel()` patterns were later added as an *additional* accepted input type, not a
replacement.

## The design

Clauses accumulate in order, every value binds as a parameter, and the terminal
methods hand back exactly what `execute()` already consumes:

```python
from cypher_graphdb.cypherquery import CypherQuery

q = (
    CypherQuery()
    .match("(center)-[e]->(connected)")
    .where_id("center", node_id)        # binds node_id  -> $_p0
    .where_in("type(e)", rel_types)     # binds the list -> $_p1, no escaping
    .return_("connected", "e")
    .limit(50)
)

rows = cdb.execute(q)               # builders are accepted directly
cypher, params = q.build()          # or the explicit form
parsed = q.to_parsed()              # or via the parser/pagination layer
```

The builder needs **no privileged access** to internals. `execute()`,
`execute_with_stats()`, `execute_cypher_stream()` and `execute_cypher_page()`
accept it because it produces the `(str, dict)` they already took.

### Invariants

These are the contracts a change to `cypherquery/` must not break.

- **Every value is bound.** Values reaching `where_id`, `where_in`, `where_eq`,
  `skip`, `limit` and the `**binds` of `where` / `raw` become parameters under
  the **reserved auto-name prefix `_p`** (`_p0`, `_p1`, …). The leading
  underscore keeps auto-names disjoint from caller-supplied ones — callers must
  not use that prefix.
- **Patterns and identifiers stay raw.** `match()`, `return_()` and `order_by()`
  take Cypher fragments verbatim; the builder does not parse or validate them.
  That is `cypherparser.py`'s job, reachable through `to_parsed()`. This is what
  keeps the builder small enough not to become a second Cypher grammar.
- **Generative and immutable.** Every chained call returns a new builder and
  never mutates the receiver. This is the deciding property for query reuse —
  branching one base query into two specialised forms without corrupting either:

  ```python
  base  = cdb.query().match("(p:Person)").where_eq("p.active", True)
  page  = base.return_("p").order_by("p.name").limit(10)   # base untouched
  total = base.return_("count(p) AS n")                    # base untouched
  ```

- **Repeated `match()` accumulates.** Each call appends an independent `MATCH`.
  Standard Cypher, and required by the write pattern where a `MERGE` follows two
  `MATCH` lookups.
- **`build()` means standard parameterised Cypher, permanently.** Any dialect
  emission must arrive as a *separate* terminal, never by changing `build()`.
- **`str(q)` shows placeholders, never values** — so it is safe to log. The
  inlined form is `build(literal_binds=True)`, which is **debug output only**: it
  uses `json.dumps`, not agtype-correct escaping, and must never be executed.
- **Bulk payloads bind at execute time, not build time.** `unwind()` takes a
  *parameter reference* (`"$rows"`), not a Python list, so the heavy payload
  stays where it belongs:

  ```python
  q = (
      CypherQuery()
      .unwind("$rows", "row")
      .match("(c:Component {id: row.src})")
      .merge("(c)-[r:USES_LANGUAGE]->(l)")
      .set("r.usage_pct = row.usage_pct")
  )
  cdb.execute(q, params={"rows": rows})
  ```

- **Caller params win on collision**, and the conflict is logged. `execute(q,
  params=...)` keys override builder-bound ones.

### Named parameters

`param(name, value)` exists for the case auto-naming cannot serve: the same
placeholder appearing in more than one clause, or a caller wanting a stable,
readable name. Omitting `value` defers it to execute time — the `$name`
placeholder is emitted but the entry is absent from `q.params`, and the caller
must supply it:

```python
q = CypherQuery().match("(p:Person)").where("p.age > $min_age").return_("p")
cdb.execute(q, params={"min_age": 30})
```

### Where it sits

| Layer | Status |
|---|---|
| Raw Cypher strings | Unchanged, fully supported |
| `MatchCriteria` (`fetch` / `delete`) | Unchanged, fully supported |
| `CypherBuilder` | Unchanged, stays internal |
| `CypherQuery` | Optional convenience layer, emits `(str, params)` |
| `cypherparser.py` | Reused via `to_parsed()` |

The builder is exported from `cypher_graphdb.cypherquery`, **not** from the
top-level package: the bare `node` / `rel` pattern constructors would otherwise
collide with the top-level `@node` model decorator. Callers needing both alias on
import.

## Backend-aware dialect emission (not built)

Emitting backend-specific Cypher was planned and is **deferred**. Investigation
narrowed the scope far below the original framing, and the payoff shrank with it.

The original claim — "AGE does not accept `$params` in MERGE/UNWIND" — was too
broad:

- `MATCH` / `WHERE` / `MERGE` / `SET` / `CREATE` with `$name` params **do work on
  AGE**, via its prepared-statement path (AGE passes all params as one `$1`
  agtype blob and the Cypher body references them by name). The builder's
  standard output already executes there; the write-clause integration tests
  confirm it.
- The one genuine limitation is **`UNWIND` over a list payload**: AGE cannot bind
  a server-side list for `UNWIND` to iterate, so the list must be an inline
  Cypher literal. That is already handled outside the builder, in
  `backends/age/ageserializer.py`.
- `ON CREATE SET` / `ON MATCH SET` are **unsupported syntax** on AGE, not a
  parameter-dialect problem. Dialect emission cannot supply a missing language
  feature; such queries are simply AGE-incompatible.

So the whole feature collapses to one case: emit an inline AGE list literal for
`UNWIND` instead of a `$param` reference. Building it would cost a new public
`BackendCapability` member (nothing existing covers parameter handling), a way
for the builder to hold the payload *values* at build time — which its clean
`unwind()` design deliberately avoids — an executable inline path routed through
`ageserializer` rather than the `json.dumps` debug path, and a dispatch point
consulting the capability.

It is deferred because the highest-value case it would serve — property-carrying
bulk writes on AGE — is already solved better by `bulk_create_nodes` /
`bulk_create_edges`, which write via direct SQL and bypass Cypher entirely. A
builder-driven AGE `UNWIND` would be strictly worse for that workload.

**If the surprise becomes a problem before then**, the cheap mitigation is to
fail loudly rather than emit: executing an `UNWIND $param` builder query against
a backend that cannot bind it should raise a clear `CypherQueryError` pointing at
`bulk_create_*`, instead of surfacing a cryptic backend error. That captures most
of the user value without any dialect machinery.

## Open question

**`return_` ergonomics beyond strings.** `return_()` accepts strings only
(`"x AS y"`). Whether to add aliasing helpers such as `alias("x", "y")` is
deferred; strings cover every current use case.

## References

- [Cypher parameters](https://neo4j.com/docs/cypher-manual/current/syntax/parameters/)
- [SQLAlchemy Core — constructing SQL expressions](https://docs.sqlalchemy.org/en/20/core/tutorial.html)
- [SQLAlchemy `Select.compile` / `literal_binds`](https://docs.sqlalchemy.org/en/20/core/selectable.html)
- [SQLAlchemy `bindparam` / `text`](https://docs.sqlalchemy.org/en/20/core/sqlelement.html)
- Prior art surveyed: Neo4j cypher-dsl (Java), cymple, pypher, neomodel
</content>

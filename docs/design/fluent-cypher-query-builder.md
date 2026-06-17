# Design Document: Fluent Cypher Query Builder

**Date:** 2026-06-17
**Status:** Proposal
**Author:** Auto-generated from query-construction analysis

## Executive Summary

This document proposes an **optional, opt-in fluent query builder** for
constructing Cypher queries in `cypher-graphdb-core`, analogous to how
SQLAlchemy Core constructs SQL. The builder is a convenience layer that sits
**above** the existing execution machinery and terminates in the contract the
library already accepts: a `(cypher_str, params)` pair (and, optionally, a
`ParsedCypherQuery`).

The builder is purely additive. Raw Cypher strings, `MatchCriteria` objects,
and the typed `create_or_merge()` API all remain first-class and unchanged.
Nobody is forced to adopt it.

The proposal is split into:

- **v1:** a read/projection builder with parameter binding (`match / where /
  with_ / return_ / order_by / skip / limit`), plus a `raw()` escape hatch. Goal:
  eliminate manual f-string assembly and hand-rolled string escaping for dynamic
  read queries. **Implemented.**
- **v2a — write clauses** (`UNWIND / CREATE / MERGE / SET / DELETE / REMOVE /
  ON CREATE SET / ON MATCH SET`). **Implemented.**
- **v2c — typed pattern objects** (`node()` / `rel()`). **Implemented.**
- **v2b — backend-aware dialect emission** (AGE vs Memgraph parameter handling).
  **Deferred** — see the v2b section for the corrected (narrow) scope and the
  steps to implement it.

## Problem Statement

### Current state

Query construction happens at three layers today, none of which is chainable:

1. **Raw Cypher strings** passed to `execute()` / `execute_with_stats()` /
   `execute_cypher_stream()` / `execute_cypher_page()`. This is the most-used
   path.
2. **Declarative criteria objects** (`MatchNodeCriteria`, `MatchEdgeCriteria`)
   passed to `fetch()` / `fetch_nodes()` / `fetch_edges()` / `delete()`. These
   are flat Pydantic models, set all-at-once via constructor kwargs.
3. **An internal `CypherBuilder`** (`cypherbuilder.py`) — a flat collection of
   `@classmethod` factories returning `(query, params)` tuples. It is **not
   exported** and is used only by the facade. It covers single-node and
   single-edge patterns only; it cannot compose incremental `WHERE` clauses,
   ordering, limits, or multi-hop traversal.

### The friction

When consumers need a query whose shape depends on runtime inputs (optional
filters, direction, variable label/relationship-type lists), they fall back to
**f-string assembly**, which forces them to:

- Re-implement Cypher-literal escaping by hand (e.g. doubling single quotes
  inside string literals).
- Interpolate identifiers such as `id(n) = {node_id}` directly into the query
  string instead of binding parameters.
- Maintain regex "safety" guards to mitigate injection where labels or keys are
  interpolated.
- Copy-paste near-identical query skeletons that differ only by direction or
  label.
- Conditionally assemble `WHERE` fragments into a list and `" AND ".join(...)`
  them — re-inventing a small query builder ad hoc at each call site.

### Representative pain pattern

A typical dynamic graph-exploration query, built by hand today:

```python
# Manual escaping helper, re-implemented per project
def _rel_type_filter(rel_types: list[str] | None) -> str:
    if not rel_types:
        return ""
    escaped = ", ".join(f"'{t.replace(chr(39), chr(39) * 2)}'" for t in rel_types)
    return f" AND type(e) IN [{escaped}]"

def _expand_pattern(direction) -> str:
    if direction == "out":
        return "(center)-[e]->(connected)"
    if direction == "in":
        return "(center)<-[e]-(connected)"
    return "(center)-[e]-(connected)"

# Assembled query: pattern + interpolated id + optional filter fragment
pattern = _expand_pattern(direction)
rel_filter = _rel_type_filter(rel_types)
query = f"MATCH {pattern} WHERE id(center) = {node_id}{rel_filter} RETURN connected, e"
result = cdb.execute(query)
```

Issues: `node_id` is interpolated (not bound), `rel_types` requires manual
literal escaping, and the WHERE clause is assembled by string concatenation.

## Goals and Non-Goals

### v1 Goals

- Provide a chainable builder for **read/projection** queries with **automatic
  parameter binding**.
- Eliminate manual literal escaping and identifier interpolation for the common
  dynamic-read patterns.
- Terminate in the **existing contract**: `(cypher_str, params)` and
  optionally `ParsedCypherQuery`. No new execution path.
- Be **100% additive** — no behavioural change to existing APIs.
- Reuse the parameter-prefixing approach from `cypherbuilder.py`, under a
  reserved auto-name prefix (`$_pN`) that cannot collide with caller-named
  params.

### v1 Non-Goals (deferred to v2)

- Write clauses: `UNWIND`, `MERGE`, `CREATE`, `SET`, `DELETE`.
- Backend-specific dialect emission (AGE vs Memgraph nuances).
- A full Cypher AST or validating grammar. The builder emits text; the existing
  `cypherparser.py` remains the validation layer when needed.
- Replacing `MatchCriteria` or `CypherBuilder`. They stay.

## Prior Art and Reference Alignment

The reference for this design is **SQLAlchemy Core**, with cross-checks against
existing Cypher builders. This section documents what we deliberately adopt or
reject before stating the design, so every API decision is traceable.

### SQLAlchemy Core (primary reference)

- **Generative API.** `Select.where()` / `order_by()` / `limit()` each *return
  a new selectable* joined to the prior clause (WHERE joins via `AND`). We
  mirror this exactly: immutable builder, repeated `where()` → `AND`.
- **Compile vs. dialect.** SQLAlchemy separates the statement from rendering:
  `stmt.compile(dialect=...)` for dialect-specific output, and
  `compile_kwargs={"literal_binds": True}` for a human-readable inlined form.
  `str(stmt)` renders with **placeholders intact**, not inlined values. This is
  the precedent for our `build()` (standard params) vs. v2 `build_for(backend)`
  (dialect), and for `build(literal_binds=True)` debug output. Note: `str(q)`
  behaves like SQLAlchemy's `str(stmt)` — it shows the query with `$param`
  placeholders, never inlined values.
- **Bound parameters + auto-wrap.** `bindparam()` is first-class, yet a plain
  Python value used in a comparison auto-binds. We copy both: an explicit
  `param(name)` helper for named/reusable parameters, and automatic binding of
  plain values passed to `where_eq`, `where_id`, etc.
- **Raw escape hatch.** `text("SELECT … WHERE id = :user_id")` injects raw SQL
  with named binds and composes into larger statements. Our
  `raw(fragment, **binds)` is the direct analogue.

### Cypher-specific builders (cross-checked)

| Library | What we borrow | What we avoid |
|---------|----------------|---------------|
| **cypher-dsl** (Neo4j, Java) | Immutable/generative model; clean separation of expression tree from rendering; bound-parameter bias | Full typed node/relationship object model — too heavy for a v1 that intentionally takes raw pattern strings |
| **cymple** (Python, typed) | `build() -> (text, params)` terminal; functional/immutable chaining; named params via `param(...)` | Mandatory entity-type definitions to get ergonomics |
| **pypher** (Python) | Lightweight, Pythonic feel; auto-named params | `__`/dynamic-attribute "magic" and a mutable token-stream that makes reuse and introspection fragile |
| **neomodel** (OGM) | Django-style `.filter().order_by()` reads cleanly | OGM coupling; its escape hatch is raw strings with no structured binding |

### Where v1 intentionally diverges from the heavier builders

v1 takes **raw pattern/return fragments** rather than a full node/relationship
object model. Rationale: the existing `cypherparser.py` already provides
validation when needed (via `to_parsed()`), and a raw-fragment surface is far
smaller to ship, easier to reason about, and matches how consumers already think
about patterns. The forward-compatibility design (append-only clause table,
frozen `build()`, generative chaining) leaves the door open to add a richer
pattern-object layer in v2c (`node()` / `rel()` constructors, as cypher-dsl and
cymple offer) **without** breaking v1 string-based call sites — they would be
an additional input type to `match()`, not a replacement.

## Proposed Solution (v1)

### Overview

A new public class `CypherQuery` accumulates clauses in order and binds every
value as a parameter. A factory `cdb.query()` returns a fresh builder; terminal
methods produce text + params or hand off to the facade for execution.

```python
from cypher_graphdb.cypherquery import CypherQuery

q = (
    CypherQuery()
    .match("(center)-[e]->(connected)")
    .where_id("center", node_id)            # binds node_id -> $_p0
    .where_in("type(e)", rel_types)         # binds list   -> $_p1, no escaping
    .return_("connected", "e")
    .order_by("connected.name")
    .limit(50)
)

str(q)      # -> "MATCH (center)-[e]->(connected)\nWHERE id(center) = $_p0 ..."
            #    placeholders intact — safe for logging
q.params    # -> {"_p0": <node_id>, "_p1": <rel_types>}
```

### Execution integration

`execute()` already accepts `str | ParsedCypherQuery`. We extend the accepted
union to include `CypherQuery`, so a builder can be executed directly:

```python
rows = cdb.execute(q)                       # builder accepted directly
rows = cdb.execute(q, unnest_result="r")    # all existing kwargs still apply
page = cdb.execute_cypher_page(q, limit=100)
stream = cdb.execute_cypher_stream(q)
```

Equivalent explicit forms remain available:

```python
cypher, params = q.build()                  # -> (str, dict) with $param placeholders
rows = cdb.execute(cypher, params=params)

cypher_debug = q.build(literal_binds=True)  # inlined values, for logging ONLY
                                             # never pass to execute()

parsed = q.to_parsed()                      # -> ParsedCypherQuery (reuses parser)
rows = cdb.execute(parsed)
```

The builder needs **no privileged access** to internals: it produces exactly
what `execute()` already consumes.

### Public API surface (v1)

| Method | Purpose | Notes |
|--------|---------|-------|
| `CypherQuery()` / `cdb.query()` | Construct a builder | Both forms valid; facade factory is sugar |
| `.match(pattern)` | Append a `MATCH` clause | Multiple calls each emit a separate `MATCH`; all accumulate |
| `.optional_match(pattern)` | Append `OPTIONAL MATCH` | Accumulates like `match()` |
| `.where(expr, **binds)` | Append a boolean condition | `expr` is a raw Cypher fragment; `**binds` maps named placeholders in it to values; multiple calls join with `AND` |
| `.where_id(var, value)` | `id(var) = $_pN` | Convenience; binds `value` automatically |
| `.where_in(expr, values)` | `expr IN $_pN` | Binds `values` as a single list param; no manual escaping |
| `.where_eq(prop, value)` | `prop = $_pN` | Convenience; binds `value` automatically |
| `.with_(*items)` | Append a `WITH` clause | Enables chained sub-pipelines |
| `.return_(*items)` | Set `RETURN` clause | `.return_distinct(...)` variant available |
| `.order_by(*items)` | Set `ORDER BY` clause | Accepts `"x DESC"` etc. |
| `.skip(n)` / `.limit(n)` | Pagination clauses | `n` bound as a parameter |
| `.raw(fragment, **binds)` | Append a verbatim Cypher fragment | Escape hatch; `**binds` maps named placeholders to values |
| `param(name, value=UNSET)` | Named bound parameter | Module-level helper; `value` is optional — omit to defer to execute-time; use when the same `$name` appears in multiple clauses |
| `.build(literal_binds=False)` | Terminal → `(str, dict)` | `literal_binds=True` inlines values for **debugging only**; never execute the inlined form |
| `.to_parsed()` | Terminal → `ParsedCypherQuery` | Feeds the existing parser/validator/pagination layer |
| `str(q)` | String representation | Equivalent to `q.build()[0]` — Cypher with `$param` placeholders intact; safe for logging |
| `.params` | Property → `dict` | The bound parameter dict; same as `q.build()[1]` |

### Design rules

- **Every value is bound.** Values passed to `where_id`, `where_in`, `where_eq`,
  `limit`, `skip`, and the `**binds` of `where` / `raw` become parameters with
  auto-generated names under a **reserved prefix** (`_p0`, `_p1`, …). The
  leading underscore keeps auto-names disjoint from caller-supplied params and
  from `param("name")` names (callers must not use the `_p` prefix).
- **Patterns and identifiers are raw.** `match()`, `return_()`, `order_by()`
  take Cypher fragments verbatim. The builder does **not** try to parse or
  validate them — that is `cypherparser.py`'s job, available via `to_parsed()`.
  This keeps v1 small and avoids re-implementing a grammar.
- **Multiple `match()` calls accumulate.** Each call appends an independent
  `MATCH` clause to the output. This is standard Cypher (multiple `MATCH`
  clauses are common) and is required by the v2 write pattern where a `MERGE`
  is preceded by two `MATCH` lookups.
- **Generative / immutable.** Each chained call returns a *new* builder; the
  receiver is never mutated. This mirrors SQLAlchemy Core and is the deciding
  factor for safe **query reuse** — branching a base query into two specialised
  forms without corrupting either:

  ```python
  base = cdb.query().match("(p:Person)").where_eq("p.active", True)
  page  = base.return_("p").order_by("p.created_at").limit(10)  # base untouched
  total = base.return_("count(p)")                               # base untouched
  ```

- **Plain values auto-bind.** A plain Python value passed to a convenience
  method is automatically wrapped as a bound parameter. `where_eq("p.x", 5)`
  binds `5` to an auto-named `$_p0`; the caller never writes a placeholder by
  hand.
- **`param(name)` for named/reusable placeholders.** When the same placeholder
  must appear in multiple clauses, or when a caller wants a stable, readable
  name, use the module-level `param()` helper:

  ```python
  from cypher_graphdb.cypherquery import CypherQuery, param

  q = (
      CypherQuery()
      .match("(p:Person)")
      .where("p.age > $min_age AND p.score > $min_age", min_age=param("min_age", 30))
      .return_("p")
  )
  # $min_age appears once in params: {"min_age": 30}
  ```

  `value` is optional in `param(name)` — omit it to defer the actual value to
  execute time. A **bound** `param(name, value)` contributes its value to
  `q.params` under its own name. A **deferred** `param(name)` (no value) emits
  the `$name` placeholder into the Cypher but is **omitted from `q.params`**;
  the caller must supply it at execute time
  (`cdb.execute(q, params={"min_age": 30})`). When passed as a `**binds` kwarg,
  the kwarg key must equal the param name (e.g. `min_age=param("min_age")`).

- **`str(q)` shows placeholders, not inlined values.** Like SQLAlchemy's
  `str(stmt)`, it renders the Cypher with `$param` intact — safe for logging.
  Use `build(literal_binds=True)` for a human-readable inlined form, but
  **never execute that string** — it bypasses parameter binding.

### Worked examples

**1. Directional expand with optional rel-type filter** (replaces the manual
`_expand_pattern` + `_rel_type_filter` helpers):

```python
PATTERNS = {
    "out":  "(center)-[e]->(connected)",
    "in":   "(center)<-[e]-(connected)",
    "both": "(center)-[e]-(connected)",
}

q = CypherQuery().match(PATTERNS[direction]).where_id("center", node_id)
if rel_types:
    q = q.where_in("type(e)", rel_types)   # list bound as single param, no escaping
q = q.return_("connected", "e")

rows = cdb.execute(q)
```

`node_id` is bound, `rel_types` is bound as a list parameter, the optional
filter is a plain `if` instead of string concatenation, and no escaping helper
is needed.

**2. Distinct relationship types from a node:**

```python
q = (
    CypherQuery()
    .match("(n)-[e]-()")
    .where_id("n", node_id)
    .return_distinct("type(e) AS t")
)
rows = cdb.execute(q)
```

**3. Conditional WHERE assembly** (replaces `where_parts.append()` +
`" AND ".join(...)`):

```python
q = CypherQuery().match("(a)-[e]->(b)").where_id("a", id_a)
if id_b is not None:
    q = q.where_id("b", id_b)
q = q.return_("e")
```

**4. Named parameter reused across clauses:**

```python
from cypher_graphdb.cypherquery import CypherQuery, param

threshold = param("threshold", 100)

q = (
    CypherQuery()
    .match("(n:Sensor)")
    .where("n.value > $threshold", threshold=threshold)
    .return_("n")
)
cypher, params = q.build()
# params = {"threshold": 100}  — one entry, readable name
```

**5. Base query branched into two specialised forms** (demonstrates immutability):

```python
base = cdb.query().match("(p:Person)").where_eq("p.active", True)
page  = base.return_("p").order_by("p.name").limit(10)
total = base.return_("count(p) AS n")

rows  = cdb.execute(page)
count = cdb.execute(total, unnest_result="rc")
```

**6. `where()` with a raw expression fragment and named binds:**

```python
q = (
    CypherQuery()
    .match("(p:Person)")
    .where("p.name STARTS WITH $prefix AND p.age >= $min", prefix="Al", min=21)
    .return_("p.name", "p.age")
    .order_by("p.name")
)
# Emits: WHERE p.name STARTS WITH $prefix AND p.age >= $min
# params: {"prefix": "Al", "min": 21}
```

The `**binds` in `where()` map placeholder names that appear literally in the
`expr` fragment to their Python values. Auto-generated `_pN` names are never
used when `**binds` are supplied for that fragment.

### Why this fits the existing architecture

- The library already standardises on `(query, params)` tuples internally
  (`cypherbuilder.py`) and on a single execution funnel (`_parse_and_execute`).
  The builder targets that contract from outside, requiring no funnel changes.
- A `ParsedCypherQuery` analysis layer already exists for validation and
  pagination planning; `to_parsed()` plugs the builder into it for free.
- Parameter-prefixing conventions are already established and injection-safe in
  `cypherbuilder.py`; the builder reuses the same approach under its reserved
  `$_pN` auto-name prefix.

## Detailed Changes (v1)

### 1. New module `cypherquery.py`

Add `src/cypher_graphdb/cypherquery.py` containing the `CypherQuery` class and
the module-level `param()` helper. The class holds ordered clause buffers
(`matches`, `wheres`, `withs`, `returns`, `order_bys`, `skip_val`,
`limit_val`) and a params dict. `build()` joins clauses in canonical Cypher
order and returns `(text, params)`. Each mutating operation returns a new
instance (shallow-copy the buffers, append to the copy).

This module **does not** import the facade — it depends only on the param
conventions (optionally factored out of `cypherbuilder.py` into a small shared
helper) and, lazily, on `parse_cypher_query` for `to_parsed()`.

### 2. Facade integration (`cyphergraphdb/cyphergraphdb.py`)

- Add a `query()` factory method returning `CypherQuery()`.
- Widen the accepted type of `execute()`, `execute_with_stats()`,
  `execute_cypher_stream()`, and `execute_cypher_page()` from
  `str | ParsedCypherQuery` to also accept `CypherQuery`. When a builder is
  received, call `.build()` and merge its params with any caller-passed `params`
  (caller params win on name conflict — see Design Decisions).

### 3. Public API (subpackage-only)

The builder API (`CypherQuery`, `param`, `Param`, `node`, `rel`,
`CypherQueryError`) is exported from the `cypher_graphdb.cypherquery`
subpackage, **not** the top-level `cypher_graphdb` package. This keeps the bare
`node` / `rel` pattern constructors from colliding with the top-level `@node`
model decorator; callers who need both alias on import. The top-level
`__init__.py` is unchanged.

### 4. No backend changes

Backends already accept `(query_string, params)`; the builder produces nothing
new at that layer.

## v2: Write Clauses, Typed Patterns, Backend Dialects

v2a (write clauses) and v2c (typed patterns) are **implemented**; v2b
(backend-aware dialect emission) is **deferred future work**. The v1 internals
(ordered clause segments + param binding + generative chaining) were designed so
these extended cleanly rather than requiring a rewrite.

### v2a — Write-clause support — IMPLEMENTED

Extend the builder with mutation clauses that the read-only v1 cannot express:

- `.unwind(param_ref, as_)` — `UNWIND $rows AS row`
- `.create(pattern)` / `.merge(pattern)`
- `.set(assignments)` — including **edge** property assignments
- `.delete(var, detach=False)` / `.remove(...)`
- `.on_create_set(...)` / `.on_match_set(...)` for `MERGE` branches

**Important distinction — builder-bound params vs. caller-supplied bulk
params.** In write queries the heavy payload (e.g. a list of row dicts) is
almost always passed by the caller at execute time via `params=`, not bound
inside the builder. `unwind()` therefore takes a **parameter reference string**
(e.g. `"$rows"`) rather than a Python value — it names a placeholder that the
caller will supply. The remaining structural clauses (`match`, `merge`, `set`)
continue to use builder-side binding for individual values as in v1.

```python
# rows list is caller-supplied at execute time — NOT builder-bound
q = (
    CypherQuery()
    .unwind("$rows", "row")                          # references caller param $rows
    .match("(c:Component {id: row.src})")
    .match("(l:Language {name: row.dst})")
    .merge("(c)-[r:USES_LANGUAGE]->(l)")
    .set("r.usage_pct = row.usage_pct")
)
cdb.execute(q, params={"rows": rows})                # rows bound at execute time
```

This pattern removes copy-pasted UNWIND skeletons and the f-string label
interpolation, while keeping the already-parameterised `rows` payload at the
execute layer where it belongs.

### v2b — Backend-aware dialect emission — FUTURE WORK (deferred)

Status: **not implemented; deferred as future work.** Investigation narrowed the
real scope considerably from the original framing, and the payoff is small — see
"Why deferred" below before picking this up.

#### Actual scope (corrected after investigation)

The original framing ("AGE does not accept `$params` in MERGE/UNWIND") was too
broad. In practice:

- **`MATCH` / `WHERE` / `MERGE` / `SET` / `CREATE` with `$name` params work on
  AGE** via its prepared-statement path (AGE passes all params as a single `$1`
  agtype JSON blob; the Cypher body references them as `$name`). The builder's
  standard parameterised output already executes correctly on AGE for these —
  confirmed by the write-clause integration tests.
- **The one genuine AGE limitation is `UNWIND` with a list payload.** AGE cannot
  bind a server-side list parameter for `UNWIND` to iterate; the list must be an
  **inline Cypher literal**. This is already handled outside the builder by
  `backends/age/ageserializer.py` (`to_cypher_list` / `escape_value`).
- **`ON CREATE SET` / `ON MATCH SET` are unsupported *syntax* on AGE**, not a
  parameter-dialect issue. Dialect emission cannot fix a missing language
  feature; such queries are simply AGE-incompatible.

So backend-aware emission collapses to essentially one case: emit an **inline
AGE list literal** for `UNWIND` (via the existing serializer) instead of a
`$param` reference, when the target backend is AGE.

#### What implementing it would require

1. **A new `BackendCapability` member**, e.g. `PARAM_IN_UNWIND` (or
   `INLINE_UNWIND_LITERALS`). None of the existing capabilities cover parameter
   handling. AGE → `False`, Memgraph → `True`. This is a public enum change in
   the core `backend.py`.
2. **A way for the builder to hold the UNWIND payload values at build time.**
   Today `unwind("$rows", "row")` takes a *parameter reference*; the list is
   supplied at execute time via `params=`. AGE inline emission needs the values
   during emission. Options: add an `unwind_values(rows, "row")` method, or have
   the execute-layer coercion inline a referenced `params` key for AGE-only.
3. **An executable inline path.** The builder's current `build(literal_binds=True)`
   uses `json.dumps` and is documented **debug-only / non-executable**. An AGE
   dialect must instead call `ageserializer.to_cypher_list` / `escape_value`
   (agtype-correct escaping: null-byte stripping, `true/false`, bare numerics).
   Do **not** reuse the `json.dumps` debug path for execution.
4. **A dispatch point**: either `q.build_for(backend)` or `cdb.execute(q)`
   consulting `backend.has_capability(PARAM_IN_UNWIND)` to choose the
   parameterised vs inline form per clause.

#### Why deferred

- The real surface is one construct (`UNWIND` list payloads), not a broad class
  of clauses.
- The highest-value real-world case — property-carrying **bulk** writes on AGE —
  is already solved, and solved better, by `bulk_create_nodes` /
  `bulk_create_edges` (direct-SQL writer, bypasses Cypher). A builder-driven AGE
  `UNWIND` would be strictly worse for that case.
- It costs a public `BackendCapability` change and complicates the clean
  `unwind()` design, for a narrow benefit.

#### Interim mitigation (lightweight, also not yet implemented)

If surprises become a problem before full emission is built, the cheap option is
to **fail loudly**: when an `UNWIND $param` builder query is executed on a
backend without `PARAM_IN_UNWIND`, raise a clear `CypherQueryError` pointing the
caller at `bulk_create_*` or an inline list — instead of surfacing a cryptic
backend error. This captures most of the user value (no surprises) without the
full dialect-emission machinery.

### v2c — Optional typed pattern objects (richer, still additive) — IMPLEMENTED

The heavier reference builders (cypher-dsl, cymple) model nodes and
relationships as **objects** rather than raw strings, which gives IDE
completion and makes structurally invalid patterns harder to construct. v1
deliberately uses raw strings to stay small. The optional `node()` / `rel()`
constructor set layers on top (named with a `c` prefix to avoid colliding with
the existing `@node` model decorator):

```python
from cypher_graphdb.cypherquery import CypherQuery, node, rel

p = node("Person", alias="p")
knows = rel("KNOWS", alias="k")
friend = node("Person", alias="friend")

q = (
    cdb.query()
    .match(p.to(knows, friend))        # pattern object, not a string
    .where(p["age"] > 30)              # property access + auto-bind
    .return_("p.name", "friend.name")
)
```

This is forward-compatible because `match()` accepts a pattern object as
an **additional** input type alongside the existing string form — the v1 string
API is never removed or changed.

### Implementation status

- **v1** (read builder + parameter binding) — **implemented**.
- **v2a** (write clauses: `unwind`/`create`/`merge`/`set`/`delete`/`remove`/
  `on_create_set`/`on_match_set`) — **implemented**.
- **v2c** (typed `node()`/`rel()` pattern objects) — **implemented**.
- **v2b** (backend-aware dialect emission) — **deferred** (see that section for
  the corrected scope and the steps required to pick it up).

The v1 clause model was designed so v2a/v2c were additive extensions rather than
a rewrite; the ordered-segment storage and deferred-binding pattern objects
slotted in without changing the v1 public surface.

### Forward-compatibility guarantee: v2 must not break v1 usage

The whole point of shipping v1 first is that **code written against v1 keeps
working unchanged after v2 ships**. This requires v1's public surface to be a
strict subset of v2's. The table below checks each v1 surface against the v2
additions and states the rule that keeps it non-breaking.

| v1 surface | What v2 adds nearby | Compatibility rule (must hold) |
|------------|---------------------|-------------------------------|
| `.match / .where / .return_ / .order_by / .skip / .limit` | `.unwind / .create / .merge / .set / .delete` | v2 **only adds** new methods. No existing method's name, parameters, or return type changes. A v1 read query never calls the new methods, so it is unaffected. |
| `.build() -> (str, params)` | `.build_for(backend) -> (str, params)` | `build()` keeps emitting **standard parameterised Cypher** forever. Dialect emission is a **separate new method** (`build_for`), never a changed `build()`. Existing `cdb.execute(q)` / `q.build()` call sites are untouched. |
| `q.params -> dict` (all values bound) | Inline-literal emission for backends that reject `$params` | The inline-literal path is reachable **only** through `build_for(backend)`. Plain `build()` / `params` always stay fully parameterised. v2 must not retroactively turn v1 bound params into inline literals. |
| `cdb.execute(q)` accepts a builder | `execute` consults backend capabilities for dialect | `execute()` already routes through one funnel. v2 makes that funnel capability-aware internally; the **call signature is unchanged**. A v1 read builder produces no write/dialect-sensitive clauses, so the capability-aware path is a no-op for it and yields identical Cypher. |
| Canonical clause ordering (`MATCH→WHERE→…→LIMIT`) | Write-clause ordering (`UNWIND→MATCH→MERGE→SET`) | v2 **extends** the ordering table with new clause kinds; it does not reorder existing read clauses. A query using only read clauses serialises identically before and after v2. |
| `.to_parsed() -> ParsedCypherQuery` | (unchanged) | Still parses whatever `build()` emits. Read queries round-trip identically. |
| `CypherQuery()` / `cdb.query()` factory | (unchanged) | Constructor takes no required args in v1 and must continue to; any v2 options (e.g. target backend) are keyword-only with defaults so `CypherQuery()` keeps working. |
| `match(pattern: str)` | `match(pattern: str \| PatternNode)` | Pattern objects are an **additional** accepted type, not a replacement. Existing string call sites are unaffected. |

Concrete invariants the v2 implementation must preserve:

1. **No method removed or renamed; no parameter removed or reordered.** v2 is
   purely additive. Any new parameters on existing methods are keyword-only with
   a default that reproduces v1 behaviour.
2. **`build()` is frozen to mean "standard parameterised Cypher".** Dialect
   emission lives in a new terminal (`build_for`), never by mutating `build()`.
3. **`execute(q)` semantics for a read-only builder are byte-identical** before
   and after v2. The capability-aware path must short-circuit to the standard
   form when no write/dialect-sensitive clause is present.
4. **Parameter binding is never silently downgraded.** v1 promises bound params;
   v2 inline-literal behaviour is opt-in via the dialect path only.
5. **Clause-order table is append-only.** Existing read-clause positions are
   fixed.

A focused regression test locks invariant (3): build a representative set of v1
read queries, snapshot their `build()` output, and assert the snapshots are
unchanged after v2 methods are added (and that `execute()` against both backends
still produces them). See Testing Strategy.

## Migration / Compatibility

- **No breaking changes.** All existing APIs (`execute`, `fetch`, `MatchCriteria`,
  `create_or_merge`, `CypherBuilder`) are untouched.
- Adoption is per-call-site and entirely optional.
- A minor version bump suffices (additive feature, new public export).

## Testing Strategy

### Unit tests (no backend required)

`CypherQuery` is deterministic text + params generation, so most tests assert on
`build()` output:

```python
def test_where_id_binds_parameter():
    q = CypherQuery().match("(n)").where_id("n", 42).return_("n")
    cypher, params = q.build()
    assert "id(n) = $" in cypher
    assert 42 in params.values()
    assert "42" not in cypher           # value is bound, not interpolated

def test_where_in_no_manual_escaping():
    q = CypherQuery().match("(n)-[e]-()").where_in("type(e)", ["A", "B'C"]).return_("e")
    cypher, params = q.build()
    assert "IN $" in cypher
    assert ["A", "B'C"] in params.values()  # awkward quote handled by binding

def test_multiple_match_clauses_accumulate():
    q = CypherQuery().match("(a)").match("(b)").return_("a", "b")
    cypher, _ = q.build()
    assert cypher.count("MATCH") == 2

def test_clause_order_is_canonical():
    q = (CypherQuery().match("(n)").where_eq("n.x", 1)
         .return_("n").order_by("n.y").limit(10))
    cypher, _ = q.build()
    assert cypher.index("MATCH") < cypher.index("WHERE") < cypher.index("RETURN") \
        < cypher.index("ORDER BY") < cypher.index("LIMIT")

def test_immutability_base_query_unchanged():
    base = CypherQuery().match("(p)").where_eq("p.active", True)
    q1 = base.return_("p").limit(10)
    q2 = base.return_("count(p)")
    assert "LIMIT" not in base.build()[0]   # base is untouched
    assert "LIMIT" in q1.build()[0]
    assert "count" in q2.build()[0]

def test_str_shows_placeholders_not_values():
    q = CypherQuery().match("(n)").where_eq("n.x", 99).return_("n")
    assert "99" not in str(q)
    assert "$" in str(q)

def test_literal_binds_inlines_values():
    q = CypherQuery().match("(n)").where_eq("n.x", 99).return_("n")
    debug = q.build(literal_binds=True)
    assert "99" in debug

def test_where_raw_with_named_binds():
    q = CypherQuery().match("(p)").where("p.name STARTS WITH $pfx", pfx="Al").return_("p")
    cypher, params = q.build()
    assert "$pfx" in cypher
    assert params["pfx"] == "Al"

def test_param_helper_named_and_deferred():
    p = param("threshold")   # no value — deferred to execute time
    q = CypherQuery().match("(n)").where("n.v > $threshold", threshold=p).return_("n")
    cypher, params = q.build()
    assert "threshold" not in params   # value not yet bound
    # execute with: cdb.execute(q, params={"threshold": 50})

def test_to_parsed_roundtrips_through_parser():
    q = CypherQuery().match("(n)").return_("n")
    parsed = q.to_parsed()
    assert parsed.submitted_query == str(q)
```

### Integration tests (against real backends)

- Execute builder-produced queries via `cdb.execute(q)` against AGE and
  Memgraph; assert results match the equivalent hand-written query.
- Verify `execute_cypher_page(q, ...)` and `execute_cypher_stream(q, ...)`
  accept a builder.
- Verify param-name collision behaviour when caller also passes `params=`.
- Snapshot v1 read-query `build()` outputs before v2 is added; assert they are
  byte-identical after v2 ships (regression lock on invariant 3).

### Edge cases

| Input | Expected behaviour |
|-------|--------------------|
| Empty builder (no `match`) | Raise a clear `CypherQueryError` on `build()` |
| Multiple `where()` calls | All joined with `AND` in call order |
| Multiple `match()` calls | Each emits a separate `MATCH` clause in call order |
| Value containing quotes via `where_eq` | Bound as param, never interpolated |
| List value via `where_in` | Bound as a single list param |
| `raw()` fragment with `**binds` | Fragment inserted verbatim, binds added to params |
| `param(name)` with no value, not supplied at execute | Backend raises a missing-param error (expected) |
| `param(name)` key collides with auto-generated `_pN` | Reserved `_p` prefix ensures no collision |
| Caller `params=` key collides with builder param name | Caller wins; log a warning |
| `build(literal_binds=True)` result passed to `execute()` | Not auto-detectable (it is a plain `str`); documented as debug-only. Mitigation: `literal_binds=True` is only reachable via `build()`, never via `str(q)`, and the docstring warns against executing it |

## Implementation Plan

### Phase 1 — v1 core (1–2 days)

1. Add `cypherquery.py` with `CypherQuery` (immutable clause buffers, param
   binder, `build()`, `str()`, `params` property).
2. Implement all v1 methods: `match`, `optional_match`, `where`, `where_id`,
   `where_in`, `where_eq`, `with_`, `return_`, `return_distinct`, `order_by`,
   `skip`, `limit`, `raw`.
3. Add module-level `param(name, value=UNSET)` helper.
4. Add `to_parsed()` (lazy import of `parse_cypher_query`).

### Phase 2 — Facade integration (0.5 day)

1. Add `cdb.query()` factory.
2. Widen `execute()`, `execute_with_stats()`, `execute_cypher_stream()`, and
   `execute_cypher_page()` type unions to accept `CypherQuery`; merge params
   (caller wins on name conflict).
3. Export `CypherQuery` and `param` from `__init__.py`.

### Phase 3 — Tests & docs (1 day)

1. Unit tests (build-output assertions, immutability, edge cases).
2. Integration tests on AGE and Memgraph.
3. Snapshot tests for v1→v2 regression lock.
4. Update `CHANGELOG.md` and add a `docs/usage/` guide showing the builder.

### Phase 4 — v2 (separate, future)

1. Write clauses (`unwind`, `create`, `merge`, `set`, `delete`,
   `on_create_set`, `on_match_set`).
2. Backend-aware dialect emission via `BackendCapability` + existing serializer
   (`build_for(backend)`).
3. Optional typed pattern objects (`node()`, `rel()`) as additional input type
   to `match()`.

## Design Decisions & Open Questions

### Resolved decisions

**Generative (immutable) chaining — resolved.**
Each chained call returns a *new* builder; the receiver is never mutated. This
mirrors SQLAlchemy Core and is required for safe query reuse (branching a base
query into specialised forms without corrupting the base). It is a v1
requirement, not a "revisit later" item. See worked example 5.

**`str(q)` shows placeholders, not inlined values — resolved.**
Aligns with SQLAlchemy's `str(stmt)`. Safe for logging. `build(literal_binds=True)`
is the separate debug-only inlined form, reachable only via an explicit `build()`
flag (never via `str(q)`). The inlined output is a plain `str`, so `execute()`
cannot auto-detect and reject it; the docstring documents it as debug-only and
warns against executing it.

**Multiple `match()` calls accumulate — resolved.**
Each call appends an independent `MATCH` clause. Required for both multi-hop
read queries and the v2 write pattern (two `MATCH` lookups before a `MERGE`).

**`where()` takes a raw fragment + named `**binds` — resolved.**
`expr` is a verbatim Cypher boolean fragment; `**binds` maps named placeholders
that appear in `expr` to their Python values. This is the analogue of
SQLAlchemy's `text("… :name")` escape hatch. Convenience methods (`where_id`,
`where_in`, `where_eq`) cover the common structured cases; `where()` handles
everything else without dropping to raw strings.

**Auto-name prefix and collision policy — resolved.**
Auto-generated parameter names use the reserved prefix `_p` (`_p0`, `_p1`, …).
The leading underscore keeps them disjoint from caller-supplied params and from
`param("name")` names; callers must not use the `_p` prefix for their own names.
When a caller passes `params=` to `execute(q, params=...)`, caller-supplied keys
win on any collision and a warning is logged.

### Open questions

1. **`return_` ergonomics beyond strings.** v1 accepts strings only (`"x AS y"`).
   Whether to add lightweight aliasing helpers (e.g. `alias("x", "y")`) is
   deferred; strings are sufficient for all current use cases.

## Appendix

### A. Affected files (v1)

| File | Change |
|------|--------|
| `cypherquery.py` (new) | `CypherQuery` class + `param()` helper |
| `cyphergraphdb/cyphergraphdb.py` | `query()` factory; widen `execute*` type unions; merge params |
| `cypherquery/__init__.py` | Export `CypherQuery`, `param`, `Param`, `node`, `rel`, `CypherQueryError` (subpackage-only) |
| `cypherbuilder.py` | Optionally factor out param-prefix helper for reuse (no behavioural change) |
| `backends/*` | None |

### B. Relationship to existing layers

| Layer | Status after v1 |
|-------|-----------------|
| Raw Cypher strings | Unchanged, fully supported |
| `MatchCriteria` (`fetch` / `delete`) | Unchanged, fully supported |
| `CypherBuilder` (internal) | Unchanged, stays internal |
| `CypherQuery` (new) | Optional public convenience layer, emits `(str, params)` |
| `cypherparser.py` | Reused via `to_parsed()` |

### C. References

- [Cypher Parameters](https://neo4j.com/docs/cypher-manual/current/syntax/parameters/)
- [SQLAlchemy Core – constructing SQL expressions](https://docs.sqlalchemy.org/en/20/core/tutorial.html)
- [SQLAlchemy `Select.compile` / `literal_binds`](https://docs.sqlalchemy.org/en/20/core/selectable.html)
- [SQLAlchemy `bindparam` / `text`](https://docs.sqlalchemy.org/en/20/core/sqlelement.html)
- Prior art surveyed: Neo4j cypher-dsl (Java), cymple (Python typed), pypher
  (Python), neomodel (Python OGM)
- Internal: `cypherbuilder.py` (param conventions), `cypherparser.py`
  (`ParsedCypherQuery`), `backend.py` (`BackendCapability`)

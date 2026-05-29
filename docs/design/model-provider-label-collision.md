# ModelProvider -- Label Collision in Multi-Graph Environments

Status: open
Date: 2026-05-29

## Problem

`ModelProvider` uses the graph label as the sole registration key:

```python
self._models: dict[str, GraphModelInfo]  # key = label, e.g. "Product"
```

This works correctly when a single process connects to a single graph, or
to multiple graphs with **disjoint label sets**. It breaks when two or more
graphs in the same process use the same label for different node types.

### Example

Graph A defines:

```python
@node(label="Product")
class ProductA(GraphNode):
    key: str
    name: str
    revenue: float        # A-specific field
```

Graph B defines:

```python
@node(label="Product")
class ProductB(GraphNode):
    key: str
    name: str
    fitness_score: float  # B-specific field
```

When both are imported, the second registration silently overwrites the
first. A connection to graph A will parse `Product` results using graph
B's class (wrong fields, wrong type). A connection to graph B has the
correct class only by accident of import order.

### Root cause

The label is simultaneously used as:
1. The **registration key** in the provider dict
2. The **graph identity** of a node (what AGE/Memgraph store as the label)

These are two different concerns. Two independent domain models can
legitimately use the same label to mean different things in their
respective graphs.

### Scope

The issue is latent when all graphs in a process have disjoint label
sets -- this is true today for most deployments. It becomes an active
bug when:

- Multiple application domains define nodes with the same name (e.g.
  `Product`, `Component`, `Status`, `Tag`)
- A framework hosts plugins from different vendors, each defining their
  own graph schema
- Unit tests use fixture models alongside production models in the same
  provider

## Edge relation resolution (secondary issue)

Relations reference target types by label string:

```python
@relation(rel_type="USES", to_type="Technology", cardinality=ONE_TO_MANY)
```

`to_type="Technology"` is resolved via `provider.get("Technology")`.
With multiple `Technology` classes registered under the same label, the
resolver has no basis for choosing between them.

## Potential solutions

### Option 1: Qualified class name as registration key

Register under the fully qualified Python class name
(`module.ClassName`), keeping label as a searchable attribute:

```python
self._models: dict[str, GraphModelInfo]
# key = "plugin_a.graph_models.ProductA", not "Product"
```

**Result parsing** -- when the graph returns a node with label `Product`,
the provider must resolve which class to use. Resolution strategy:

1. **Connection-scoped provider** (current): each pool/connection already
   has its own `ModelProvider`. If only one `Product` is registered in
   that provider, resolution is unambiguous.
2. **Namespace-scoped resolution**: if multiple classes share a label,
   prefer the one in the same module or package as the declaring context.

**Relation `to_type` resolution** -- `to_type="Technology"` becomes
ambiguous when multiple `Technology` classes exist. Resolve by:

1. Same module as the declaring class (most specific)
2. Same package as the declaring class
3. Unique label across all registrations (only one `Technology` exists)
4. Error if ambiguous and no context resolves it

**Impact:**
- Breaking change to `ModelProvider.register()`, `get()`, and all
  callers that look up by label
- Relation decorator resolution logic must change
- `load_from_json_schemas()` uses labels only (no qualified names in
  JSON Schema) -- dynamic classes would still be keyed by label; only
  statically defined classes benefit
- Schema generation and LLM formatting iterate providers by label --
  these stay label-based

This is the correct long-term fix but requires significant refactoring
across the decorator stack, schema utilities, and result parsers.

### Option 2: Label namespacing convention

Require plugins/domains to prefix their labels:
`ta_Product` (tech_assessment), `ts_Product` (tech_stack). Enforced
by convention, not by the provider.

**Pro:** No code changes. Works today.
**Con:** Ugly graph labels. Breaks with third-party schemas that use
natural labels. Pushes the problem to the caller.

### Option 3: Explicit pool isolation (current partial mitigation)

Each named pool has its own `ModelProvider`. Callers load only the
models relevant to that pool. Two pools can each have a `Product`
without conflict because they never share a provider instance.

This works correctly **if** model classes are registered into the pool
provider in isolation (via `use_model_provider` context before import).
It breaks when module-level imports fire into the global provider before
any context is established -- the global provider accumulates all
labels from all modules, and label collision occurs there.

The per-pool isolation is the **right architectural direction** but
incomplete without solving the global provider pollution problem.

**Pro:** Already partially implemented. No protocol changes.
**Con:** Relies on import discipline that module-level imports make hard
to enforce. The global provider remains vulnerable.

### Option 4: Lazy registration via graph name annotation

Annotate the `@node` decorator with an explicit graph affinity:

```python
@node(label="Product", graph="tech_assessment")
class Product(GraphNode): ...
```

Registration stores `(graph, label)` as the composite key. Resolution
for result parsing uses the connection's graph name to disambiguate.

**Pro:** Explicit, no import-order dependency, backward-compatible
(graph defaults to None for existing code).
**Con:** Requires callers to know the graph name at class definition time.
Breaks if the same class is used in multiple graphs.

## Solution (implemented)

The global provider is irrelevant in pool-based usage. The solution
is to treat each named pool's ``ModelProvider`` as the only provider
that matters, and populate it explicitly from the plugin's module object.

``ModelProvider.register_from_module(module)`` scans a module for all
``@node``/``@edge`` decorated classes (identified by the ``graph_info_``
attribute set at decoration time) and registers each into this provider.
Because ``graph_info_.graph_model`` always references the original Python
class, two plugins both defining ``Product`` are correctly isolated:
each pool provider gets its own class directly from its own module --
the global provider collision is bypassed entirely.

```python
import plugins.tech_assessment.graph_models as ta_models
import plugins.tech_stack.graph_models as ts_models

provider_ta = ModelProvider()
provider_ts = ModelProvider()

provider_ta.register_from_module(ta_models)  # gets ProductA (fitness_score)
provider_ts.register_from_module(ts_models)  # gets ProductB (revenue)
```

Import order does not matter. The global provider state does not matter.
Each pool provider is populated declaratively from the module object.

**Relation declarations** (``@relation(to_type="Technology")``) store
the target as a plain string label. They are purely declarative --
used only for schema documentation and LLM output. They are never
resolved to a class at runtime. Each graph object is independent.
No cross-class resolution is needed.

**Remaining convention**: label uniqueness is still recommended within
a single pool's provider (two classes with the same label in the same
pool would overwrite each other). Across pools, labels can freely
collide -- each pool is isolated.

## Remaining open questions

1. Should `register()` warn or raise on label collision within the
   same provider (two classes with the same label in the same pool)?
   Currently silent overwrite. A warning would surface mistakes early.
2. Should the global provider be formally deprecated in favour of
   always using named pools? The global provider is still useful for
   single-graph scripts and tools outside the pool pattern.

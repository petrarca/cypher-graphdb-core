# ModelProvider Isolation -- Per-Graph Model Separation

> Design document | Status: implemented

This document describes how `cypher-graphdb` keeps the graph models of
different graphs separated within a single process, and the technique a
consumer uses to generate or register a schema for exactly one graph without
picking up other graphs' types.

It complements [model-provider-label-collision.md](model-provider-label-collision.md),
which covers the related label-uniqueness concern. Here the focus is the
*provider* as the unit of isolation and how registration and schema generation
both resolve which provider they act on.

## The model: a `ModelProvider` is the unit of isolation

A `ModelProvider` is a registry of typed graph-model classes keyed by label. It
is the boundary across which models do **not** leak: two providers can each hold
their own `Product` (or any disjoint set of types) with no interaction.

There are two kinds of provider in play:

- **The global provider** -- a module-level singleton (`model_provider`). It
  exists for single-graph scripts and tools that never adopt the pool pattern.
- **Per-graph providers** -- one `ModelProvider` per named graph/pool. This is
  the unit a multi-graph application should use: each graph parses its results,
  generates its schema, and resolves its relations against its **own** provider.

The architectural rule is simple: *in multi-graph usage, the global provider is
irrelevant; every graph acts on its own provider.* The rest of this document is
about how the API makes "act on a specific provider" reliable, given that
`@node`/`@edge` are import-time side effects.

## The core mechanism: registration resolves the *active* provider

`@node`/`@edge` register a class into a provider as a side effect of importing
the module that defines it. The provider they register into is **not** hardcoded
to the global singleton -- it is resolved at decoration time via
`get_active_provider()`:

```python
def get_active_provider() -> ModelProvider:
    # the provider on top of the thread-local activation stack,
    # else the global singleton
    ...
```

A provider becomes "active" for the current thread by entering its
`activate()` context manager (a thread-local stack, so concurrent threads each
have their own active provider):

```python
provider = ModelProvider()
with provider.activate():
    import my.graph_models      # @node/@edge here register into `provider`
# stack popped; the global singleton is active again
```

This is the primitive that makes registration target a specific provider. The
two higher-level techniques below build on it.

## Technique 1 -- register from an already-imported module

When the model module is *already* a Python object (already imported, or you
hold a reference to it), the most direct and import-order-independent way to
populate a provider is `register_from_module`:

```python
import my.graph_models as models

provider = ModelProvider()
provider.register_from_module(models)   # copies every @node/@edge class into `provider`
```

`register_from_module` scans the module for decorated classes (identified by the
`graph_info_` attribute set at decoration time) and registers each into *this*
provider directly. Because it reads classes that already exist, it does **not**
depend on import timing or on what is globally active: even if the module was
first imported into a different provider, its classes are copied here correctly.

This is the recommended technique whenever the caller already has the module
object -- it is the most robust path and the one to reach for by default.

## Technique 2 -- generate a schema scoped to one provider

Schema generation from a path (`SchemaGenerator` /
`ModelProvider.generate_schemas_from_path`) honors the same active-provider
rule as the decorators, so registration and generation are symmetric.

`SchemaGenerator` resolves its provider the same way the decorators do:

```python
SchemaGenerator()                  # uses the active provider (or global if none active)
SchemaGenerator(provider=my_prov)  # uses an explicit, isolated provider
```

Internally, `generate_schemas_from_path` **activates `self`** while it imports
the model file, so the freshly imported module's `@node`/`@edge` decorators
register into that same provider rather than into whatever happened to be active
before. The consequence:

```python
prov = ModelProvider()
schema = SchemaGenerator(provider=prov).generate_schemas("my/models.py", combine=True)
# `schema` contains exactly the types defined under my/models.py,
# and the global provider is NOT polluted by the import.
```

Passing an explicit provider therefore gives a clean, isolated schema **and**
keeps the global provider untouched -- the two write-side operations
(registration and generation) now obey one rule: *the provider you act on is the
explicit one if given, else the active one, else the global one.*

### When generation falls back to already-registered models

If the path's module was already imported elsewhere (Python caches imports, so
the decorators do not fire again), `generate_schemas_from_path` falls back to
already-registered models -- but only those whose **defining file lives under
the requested path**. It never returns every `source="model"` entry in the
provider. This scoping ensures a path-based request cannot pull in unrelated
graphs' types that happen to share the same provider.

For this already-imported case, prefer Technique 1 (`register_from_module`),
which is unaffected by the import cache.

## Result parsing uses the connection's provider

On the read side, a `CypherGraphDB` connection carries the provider it was
created with (`cdb.model_provider`). When a pool is created with a per-graph
provider, query results are hydrated into that graph's own types. This is a
separate channel from registration/generation: a pool's provider governs how
*results are typed*, while `activate()` / explicit-provider governs where
*models register and schemas generate*. A complete per-graph setup wires both to
the same provider instance.

## Summary

| Concern | Resolves the provider via | Recommended API |
|---|---|---|
| `@node`/`@edge` registration | active provider (`get_active_provider`) | `use_model_provider` / `activate`, or `register_from_module` |
| Schema generation from a path | explicit `provider=`, else active | `SchemaGenerator(provider=...)` / `save_from_path(provider=...)` |
| Result hydration | the connection's `model_provider` | per-pool provider set at pool creation |

The single guiding rule: an isolated graph acts on an **explicit, dedicated
`ModelProvider`** -- populated by `register_from_module` (have the module) or
generated via `SchemaGenerator(provider=...)` (have a path) -- so its model is
independent of import order and of whatever else is registered in the process.

---
type: Concept
title: ModelProvider isolation — per-graph model separation
description: How cypher-graphdb keeps the graph models of different graphs separate in one process, with no global registry and no import side effects.
tags: [cypher-graphdb, model-provider, isolation, multi-graph, registry]
status: stable
implementation: implemented
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# ModelProvider isolation — per-graph model separation

This document describes how `cypher-graphdb` keeps the graph models of different
graphs separated within a single process.

It complements [model-provider-label-collision.md](model-provider-label-collision.md),
which covers the related label-uniqueness concern. Here the focus is the
*provider* as the unit of isolation.

## The model: a `ModelProvider` is the unit of isolation

A `ModelProvider` is a registry of typed graph-model classes keyed by label. It
is the boundary across which models do **not** leak: two providers can each hold
their own `Product` (or any disjoint set of types) with no interaction.

**There is no global provider.** A provider is an ordinary value that a module
creates and models register on explicitly:

```python
from cypher_graphdb import GraphNode, ModelProvider

mp = ModelProvider()

@mp.node()
class Product(GraphNode):
    key: str
```

Isolation is therefore *structural*, not something a caller has to arrange:
importing a model module has no process-wide side effect, so nothing can leak
between graphs and no cleanup, redirection, or copying step is required.

## Why explicit registration rather than an ambient default

Registration happens as a side effect of importing a module. If the target
registry were ambient -- a global singleton, or a thread-local "currently
active" provider -- then *when* a module is first imported would determine
*where* its models land. That coupling is the root of the problems this design
removes:

- Two graphs defining the same label collide in the shared registry.
- A provider can only capture a module's models if it is active **before** the
  first import; Python's import cache means a second attempt silently registers
  nothing.
- Consumers must defend against eager imports (lazy `__getattr__` in package
  `__init__`, careful import ordering), or repair the damage afterwards by
  copying models out of the global registry into a dedicated one.

Naming the provider at the definition site makes the binding static: a model
belongs to the provider it was declared on, regardless of who imports it, when,
or from how many threads.

## Loading models from a path: provider discovery

Loading models from a file, package, or directory (`cypher-graphdb --model-path`,
`SchemaGenerator`, `ModelProvider.generate_schemas_from_path`) cannot rely on
decorators firing, because the module may already be imported. Instead, loading
**discovers** the providers declared in the loaded modules and adopts their
models:

```python
provider = ModelProvider()
provider.try_to_load_models(None, "my/graph_models")
```

`ModelProvider.discover_providers(module)` reads module-level `ModelProvider`
values back off the imported module. This has three consequences worth naming:

- **Import-order independent.** An already-imported module still yields its
  provider, so repeat loads are idempotent rather than silently empty.
- **Multiple providers per module are supported.** A module may declare several
  providers (e.g. `taxonomy` and `domain`); all are discovered.
- **Packages are walked recursively.** A package `__init__.py` doing
  `from .a import *` / `from .b import *` leaves only the *last* `mp` visible at
  top level, so submodules are scanned too and providers de-duplicated by
  identity.

A same-named package cached from a different directory is dropped from
`sys.modules` before import, so the requested path is the one actually loaded.

## Result parsing uses the connection's provider

On the read side, a `CypherGraphDB` connection carries the provider it was
created with (`cdb.model_provider`), and query results are hydrated into that
provider's types. A connection created without one gets a fresh empty provider
-- results come back as generic `GraphNode` / `GraphEdge` rather than reaching
into shared state.

The provider can also be rebound after construction (`cdb.model_provider = mp`),
which is how a pool hands a graph's models to the connections it lends out.

## Summary

| Concern | Provider used |
|---|---|
| `@mp.node` / `@mp.edge` registration | the provider named at the definition site |
| Loading / schema generation from a path | the provider you call it on; models are discovered and adopted into it |
| Result hydration | the connection's `model_provider` |

The single guiding rule: **a graph's models live on a provider that is named
explicitly**, so the model set is a property of the code that declares it rather
than of import order or process-wide state.

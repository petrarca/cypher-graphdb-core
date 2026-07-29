---
type: ADR
title: Explicit ModelProvider instances instead of a global model registry
description: Graph models register on a ModelProvider the consumer constructs, replacing the process-wide registry populated by import side effects.
tags: [cypher-graphdb, model-provider, adr, registry, multi-graph]
status: stable
decision: accepted
supersedes: []
superseded_by: []
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Explicit ModelProvider instances instead of a global model registry

## Context

Typed graph models were registered with module-level decorators — `@node`,
`@edge`, `@relation` — onto a single process-wide registry. Registration was an
**import side effect**: importing a module mutated global state.

The registry keyed models by **graph label alone**. That is safe only while one
process talks to one graph, which stopped being true. A service hosting a
technology graph and a code graph has two legitimate, different classes both
labelled `Component`, with different fields. The second import silently
overwrote the first, and the damage surfaced far away as wrong hydration or
validation failures against fields the other class never had. This was not
hypothetical — it was found in `lauro`, where `codegraph.Component` and
`tech_stack.Component` collided and the winner depended on import order.

Import-order-dependent global state is also hostile to testing: test isolation
required either subprocesses or reaching into private registry internals.

Related analysis:
[model-provider-label-collision](../design/model-provider-label-collision.md).

## Decision

Models register on an **explicit `ModelProvider` instance** that the consumer
constructs, and a provider is wired to a connection deliberately:

```python
mp = ModelProvider()

@mp.node(label="Component")
class Component(GraphNode): ...

db = CypherGraphDB(backend="age", model_provider=mp)
```

Consequently:

- **There is no global provider**, and no backward-compatible shim. The
  `@node` / `@edge` / `@relation` module-level decorators were removed rather
  than deprecated.
- **Importing a model module has no process-wide effect.** A module declares
  `mp = ModelProvider()` at module level; nothing lands anywhere implicitly.
- **A connection without a provider gets a fresh empty one**, never a shared
  global, so an untyped connection cannot leak models into a typed one.
- **Path-based loading discovers providers** rather than scraping a registry:
  `--model-path`, `SchemaGenerator` and `generate_schemas_from_path` walk the
  loaded modules (recursively, de-duplicated by identity) and adopt the
  providers they declare. This is import-order independent and immune to the
  module cache, which matters because an `__init__.py` doing `from .x import *`
  would otherwise shadow `mp`.

## Consequences

**Easier.** Two graphs can use the same label with different classes, which was
previously data corruption. Tests construct a provider per test with no global
teardown. Model ownership is explicit and greppable: a provider is a value that
is passed, not ambient state. Discovery-based loading removes a class of
import-ordering bugs.

**Harder.** Every consumer had to be migrated — the provider must be threaded
from the model module to the connection or pool, which is more wiring than an
implicit global. Registration is no longer possible from a module nobody
imports, so a model that is never reached is simply absent rather than
mysteriously present.

**Accepted trade-offs.** The break is hard, with no compatibility layer: this
was a coordinated change across `cypher-graphdb-core`, `sonnet-graph`,
`coco-graph` and `lauro`, released as `cypher-graphdb 0.5.0`. We accept that
convenience for the single-graph case (where a global registry is genuinely
less typing) is traded for correctness in the multi-graph case.

Realized in [model-provider-isolation](../design/model-provider-isolation.md);
usage in [typed-models](../usage/typed-models.md).

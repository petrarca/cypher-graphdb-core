---
type: Guide
title: Typed models with a ModelProvider
description: Register Pydantic node and edge classes on an explicit ModelProvider so query results hydrate into your own types, and keep two graphs from colliding on a label.
tags: [cypher-graphdb, model-provider, typed-models, pydantic, relations, schema]
status: stable
generated: { by: opencode/claude-opus-5, at: 2026-07-29T00:00:00Z }
---

# Typed models

The top layer of the stack: your own Pydantic classes instead of generic
`GraphNode` / `GraphEdge`. Entirely optional — everything below still works
untyped.

## The provider is the registry

Models are registered on an **explicit** `ModelProvider`. There is no global
registry, so importing a model module has no process-wide side effect.

```python
from cypher_graphdb import GraphNode, GraphEdge, ModelProvider

mp = ModelProvider()

@mp.node(label="Product")
@mp.relation(rel_type="USES_TECHNOLOGY", to_type="Technology")
class Product(GraphNode):
    name: str
    multi_tenancy: bool | None = None

@mp.node(label="Technology")
class Technology(GraphNode):
    name: str

@mp.edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    version: str | None = None
```

**Always declare a module-level `mp = ModelProvider()`** in a graph-models
module. Never rely on a model landing somewhere by import side effect.

`label=` is optional — omit it and the class name is used, which is the common
case:

```python
@mp.node()
@mp.relation(rel_type="CONTAINS", to_type="Module", cardinality=Cardinality.ONE_TO_MANY)
class Package(GraphNode):
    symbol: str
    name: str
```

### Why explicit providers matter

Two graphs may legitimately use the same label for different things — a
`Component` in a code graph and a `Component` in a technology graph, with
different fields. With a global registry the second import silently overwrites
the first, and the failure surfaces much later as wrong data. Separate providers
cannot collide.

This is the subject of
[model-provider-isolation](../design/model-provider-isolation.md).

## Registration API

All are instance methods on the provider:

| Method | Registers |
|---|---|
| `mp.node(label=…)` | A node class |
| `mp.edge(label=…)` | An edge class |
| `mp.relation(rel_type=…, to_type=…, cardinality=…)` | A declared relationship from this node type to another |
| `mp.extend_relation(…)` / `mp.extend_relations(…)` | Additional relations onto an already-registered class |

`Cardinality` (e.g. `ONE_TO_MANY`) documents the shape of a relation and feeds
schema generation. Stack multiple `@mp.relation` decorators for several
relationships from the same class.

## Wiring the provider to a connection

A connection hydrates results using the provider you give it:

```python
from cypher_graphdb import CypherGraphDB

db = CypherGraphDB(backend="memgraph",
                   connect_url="bolt://localhost:7687",
                   model_provider=mp)
```

or afterwards:

```python
db.model_provider = mp
```

**Without a provider, results stay generic.** A connection with no provider gets
a fresh empty one — never a shared global — so nothing leaks between
connections, and queries return `GraphNode` / `GraphEdge`.

## Using typed objects

```python
product = Product(name="CypherGraph Demo", multi_tenancy=True)
product = db.create_or_merge(product)

tech = db.create_or_merge(Technology(name="Python"))

db.create_or_merge(UsesTechnology.build(product, tech, version="3.14"))
db.commit()

for (p,) in db.fetch_nodes({"label_": Product}):
    print(p.name, p.multi_tenancy)      # real attributes, validated types
```

Note the `(p,)` — `fetch_nodes` returns **rows**, and each row is a tuple. See
[the unnesting rules](queries.md#unnesting-collapses-aggressively) before
reaching for `unnest_result=True` here.

Pydantic validates on construction, so a wrong type fails where you built the
object rather than silently reaching the graph.

Pass `validate=True` to `create_or_merge` to additionally check properties
against the registered model at write time.

### Getting the label from a class

Registered classes expose their graph metadata, which is how generic helpers stay
typed:

```python
label = Product.graph_info_.label_       # "Product"
db.create_property_index(label, "name")
```

## Loading models from a path

Some entry points take a path instead of an imported module — the CLI's
`--model-path`, `SchemaGenerator`, `generate_schemas_from_path`.

These **discover** the providers declared in the loaded modules and adopt their
models. A module may declare several providers, and packages are walked
recursively, so `__init__.py` re-exports cannot shadow anything. Discovery is
import-order independent and immune to the module cache.

```bash
cypher-graphdb --model-path ./graph_models/ --graph mygraph
```

A directory with an `__init__.py` is loaded as a package, so relative imports
between model files work.

## Generating JSON schemas

Model definitions can be exported as JSON schema without a database connection:

```bash
cypher-graphdb schema generate -m ./graph_models/ -o ./schemas/
```

| Option | Meaning |
|---|---|
| `-m, --models` | Python file or directory holding the models |
| `-o, --output` | Output path; a directory gets `graph.schema.json` |
| `-v, --verbose` | Detailed output |
| `--overwrite` | Replace an existing file without asking |
| `-y, --yes` | Skip the confirmation prompt |

## Multiple graphs in one process

Give each graph its own provider and its own connection, and the same label can
mean different things in each:

```python
code_mp = ModelProvider()     # Component: code-graph flavour
tech_mp = ModelProvider()     # Component: tech-stack flavour

code_db = CypherGraphDB("age", model_provider=code_mp)
tech_db = CypherGraphDB("age", model_provider=tech_mp)
```

Combine with `Settings.with_prefix()` to configure them independently — see
[connecting.md](connecting.md).

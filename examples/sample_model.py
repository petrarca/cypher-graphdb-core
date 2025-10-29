from cypher_graphdb import Cardinality, GraphEdge, GraphNode, edge, node, relation


@node()
@relation(
    rel_type="USES_TECHNOLOGY",
    to_type="Technology",
    cardinality=Cardinality.ONE_TO_MANY,
    form_field=True,
)
@relation(
    rel_type="USES_ARCH_PATTERN",
    to_type="ArchitecturePattern",
    cardinality=Cardinality.ONE_TO_MANY,
    form_field=True,
)
@relation(
    rel_type="HAS_ARCH_CHARACTERISTIC",
    to_type="ArchitectureCharacteristic",
    cardinality=Cardinality.ONE_TO_MANY,
    form_field=True,
)
class Product(GraphNode):
    name: str
    multi_tenancy: bool | None = None


@node(label="Technology")
@relation(
    rel_type="BELONGS_TO",
    to_type="TechnologyCategory",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
class Technology(GraphNode):
    name: str


@edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    version: str | None = None


@node(label="ArchitecturePattern")
class ArchitecturePattern(GraphNode):
    name: str
    description: str | None = None


@node(label="ArchitectureCharacteristic")
class ArchitectureCharacteristic(GraphNode):
    name: str
    description: str | None = None


@edge(label="USES_ARCH_PATTERN")
class UsesArchPattern(GraphEdge):
    pass


@edge(label="HAS_ARCH_CHARACTERISTIC")
class HasArchCharacteristic(GraphEdge):
    pass


@node(label="TechnologyCategory")
@relation(
    rel_type="BELONGS_TO",
    to_type="TechnologyRoot",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
class TechnologyCategory(GraphNode):
    name: str
    description: str | None = None


@node(label="TechnologyRoot")
class TechnologyRoot(GraphNode):
    name: str


@edge(label="BELONGS_TO")
class BelongsTo(GraphEdge):
    pass

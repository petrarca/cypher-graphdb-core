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
@relation(
    rel_type="IP_OWNED_BY",
    to_type="Company",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
@relation(
    rel_type="DEPLOYED_IN",
    to_type="Country",
    cardinality=Cardinality.ONE_TO_MANY,
    form_field=True,
)
@relation(
    rel_type="SUPPORTS_LANGUAGE",
    to_type="Language",
    cardinality=Cardinality.ONE_TO_MANY,
    form_field=True,
)
class Product(GraphNode):
    name: str


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
@relation(
    rel_type="BELONGS_TO",
    to_type="ArchitectureCategory",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
class ArchitecturePattern(GraphNode):
    name: str
    description: str | None = None


@node(label="ArchitectureCharacteristic")
@relation(
    rel_type="BELONGS_TO",
    to_type="ArchitectureCategory",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
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
@relation(
    rel_type="BELONGS_TO",
    to_type="TechnologyCategory",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
class TechnologyCategory(GraphNode):
    """Technology category node. Categories can be nested by relating to another TechnologyCategory."""

    name: str
    description: str | None = None


@node(label="TechnologyRoot")
class TechnologyRoot(GraphNode):
    name: str


@node(label="ArchitectureRoot")
class ArchitectureRoot(GraphNode):
    name: str


@node(label="ArchitectureCategory")
@relation(
    rel_type="BELONGS_TO",
    to_type="ArchitectureRoot",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
@relation(
    rel_type="BELONGS_TO",
    to_type="ArchitectureCategory",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
class ArchitectureCategory(GraphNode):
    """Architecture category node. Categories can be nested by relating to another ArchitectureCategory."""

    name: str
    description: str | None = None


@node(label="ArchitectureStyle")
@relation(
    rel_type="BELONGS_TO",
    to_type="ArchitectureCategory",
    cardinality=Cardinality.ONE_TO_ONE,
    form_field=True,
)
class ArchitectureStyle(GraphNode):
    name: str
    description: str | None = None


@edge(label="BELONGS_TO")
class BelongsTo(GraphEdge):
    pass


@node(label="Company")
class Company(GraphNode):
    name: str
    description: str | None = None


@node(label="Country")
class Country(GraphNode):
    name: str
    description: str | None = None


@node(label="Language")
class Language(GraphNode):
    name: str
    description: str | None = None


@edge(label="IP_OWNED_BY")
class IpOwnedBy(GraphEdge):
    pass


@edge(label="DEPLOYED_IN")
class DeployedIn(GraphEdge):
    pass


@edge(label="SUPPORTS_LANGUAGE")
class SupportsLanguage(GraphEdge):
    pass

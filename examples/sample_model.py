from cypher_graphdb import GraphEdge, GraphNode, edge, node, relation


@node()
@relation(
    rel_type="USES_TECHNOLOGY",
    to_type="Technology",
    cardinality="ONE_TO_MANY",
    form_field=True,
)
class Product(GraphNode):
    name: str
    multi_tenancy: bool | None = None


@node(label="Technology")
class Technology(GraphNode):
    name: str


@edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    version: str | None = None

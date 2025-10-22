from cypher_graphdb import GraphEdge, GraphNode, edge, node, relation


@node()
@relation(rel_type="USES_TECHNOLOGY", to_type="Technology")
class Product(GraphNode):
    name: str
    multi_tenancy: bool | None = None


@node(label="Technology")
class Technology(GraphNode):
    name: str


@edge(label="USES_TECHNOLOGY")
class UsesTechnology(GraphEdge):
    version: str | None = None

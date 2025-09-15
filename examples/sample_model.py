from cypher_graphdb import GraphEdge, GraphNode, edge, node, relation


@node(metadata={"color": "blue", "label": "product_key", "tags": [1, 2, 3]})
@relation(rel_type="BELONGS_TO", to_type="Category")
class Product(GraphNode):
    product_key: str
    product_family: str = None


@node(label="Category")
class Category(GraphNode):
    category_key: str
    name: str
    description: str = None


@edge(label="BELONGS_TO")
class BelongsTo(GraphEdge):
    val: str = None

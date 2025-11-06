import sys
from importlib import import_module
from pprint import pprint

from loguru import logger

import cypher_graphdb
from cypher_graphdb import CypherGraphDB, MatchCriteria, MatchEdgeCriteria, MatchNodeCriteria

module_prefix = f"{__package__}." if __package__ else ""
graph_model = import_module(f"{module_prefix}graph_model")
Product = graph_model.Product
Technology = graph_model.Technology
UsesTechnology = graph_model.UsesTechnology

# configure logging and enable logging for cypher_graphdb
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.enable(cypher_graphdb.__name__)

cdb = CypherGraphDB().connect()

products = cdb.fetch_nodes({"label_": Product}, unnest_result="c")
pprint(products)
cypher_product = cdb.fetch_nodes({"label_": Product, "name": "CypherGraph"}, unnest_result=True)

# p = cdb.fetch_nodes({"label_": Product, "name": "CypherGraph Demo"}, unnest_result="rc")
# cdb.create_or_merge(p)

# p = Product(name="Legacy Product")
# cdb.create_or_merge(p)

# Fetch nodes
product_overview = cdb.fetch(MatchNodeCriteria(properties_={}, label_=Product, projection_=["n.name", "id(n)"]))
pprint(product_overview)
tech_overview = cdb.fetch(MatchNodeCriteria(label_=Technology, projection_=["n.name", "id(n)"]))
pprint(tech_overview)

# Fetch all nodes
result = cdb.fetch(MatchNodeCriteria(properties_={}))

# Fetch edges
edges = cdb.fetch(
    MatchEdgeCriteria(
        label_=UsesTechnology,
        fetch_nodes_=True,
        start_criteria_=MatchNodeCriteria(label_=Product, properties_={"name": "CypherGraph"}),
    )
)
pprint(edges[:3])

sc = MatchCriteria(
    projection_=["count(x)"],
    prefix_="x",
)
edge_stats = cdb.fetch(MatchEdgeCriteria(label_=UsesTechnology, fetch_nodes_=True, start_criteria_=sc))
pprint(edge_stats)

""" Merge or create operations, with typed graph objects"""

demo_product = Product(name="CypherGraph Demo")
cdb.create_or_merge(demo_product)
pprint(demo_product)

demo_technology = Technology(name="Python Demo")
cdb.create_or_merge(demo_technology)
pprint(demo_technology)

demo_relation = UsesTechnology.build(demo_product, demo_technology)
cdb.create_or_merge(demo_relation)
pprint(demo_relation)

created_link = cdb.fetch(
    MatchEdgeCriteria(
        label_=UsesTechnology,
        start_criteria_=MatchNodeCriteria(label_=Product, properties_={"name": "CypherGraph Demo"}),
        end_criteria_=MatchNodeCriteria(label_=Technology, properties_={"name": "Python Demo"}),
        fetch_nodes_=True,
    )
)
pprint(created_link)

# Delete operations
cdb.delete(MatchNodeCriteria(label_="Product", properties_={"name": "CypherGraph Demo"}), detach=True)
cdb.delete(MatchNodeCriteria(label_="Technology", properties_={"name": "Python Demo"}), detach=True)

cdb.commit()

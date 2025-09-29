import sys
from pprint import pprint

from loguru import logger
from sample_model import Product

import cypher_graphdb
from cypher_graphdb import CypherGraphDB, MatchCriteria, MatchEdgeCriteria, MatchNodeCriteria

# configure logging and enable logging for cypher_graphdb
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.enable(cypher_graphdb.__name__)

cdb = CypherGraphDB().connect()

p = cdb.fetch_nodes(4711)
p = cdb.fetch_nodes({"label_": "Product"}, unnest_result="c")
pprint(p)
if p:
    p = cdb.fetch_nodes(p[0].id_, unnest_result=True)
    p.id_ = None
    cdb.create_or_merge(p)
p = cdb.fetch_nodes({"label_": "Product", "product_key": "ABC"}, unnest_result="rc")

# p = cdb.fetch_nodes(product_key= "ALBIS", label_="Product", unnest_result="rc")
# cdb.create_or_merge(p)

# p = Product(product_family="abc",product_key = "ABC")
# cdb.create_or_merge(p)

# Fetch nodes
p = cdb.fetch(MatchNodeCriteria(id_=844424930131979))
pprint(p)
p = cdb.fetch(MatchNodeCriteria(properties_={"name": "AIS"}, label_="Category"))

# Fetch all nodes
result = cdb.fetch(MatchNodeCriteria(properties_={}))

# Fetch edges
e = cdb.fetch(MatchEdgeCriteria(label_="BELONGS_TO", fetch_nodes_=True))
e = cdb.fetch(MatchEdgeCriteria(id_=1407374883553283))
e = cdb.fetch(MatchEdgeCriteria(properties_={"val": 73}))
e = cdb.fetch(MatchEdgeCriteria(properties_={"val": 73}, label_="BELONGS_TO"))

sc = MatchCriteria(
    projection_=["count(x)"],
    prefix_="x",
)
e = cdb.fetch(MatchEdgeCriteria(label_="BELONGS_TO", fetch_nodes_=True, start_criteria_=sc))

""" Merge or create operations, with typed graph objects"""

p = Product(product_key="ABC", product_family="ABC")
cdb.create_or_merge(p)
pprint(p)
p = cdb.fetch(MatchNodeCriteria(id=p.id_))
pprint(p)
# p = p[0][0]
# p.properties_["abc"] = random.randint(1, 100)
# p.product_family = "DEF"
# cdb.create_or_merge(p)
# c = Category(category_key="AIS", name="AIS")
# cdb.create_or_merge(c)

# b = BelongsTo.build(p, c, val=4711)
# cdb.create_or_merge(b)
# pprint(b)
# b.val = random.randint(1, 100)
# cdb.create_or_merge(b)
# pprint(b)
# b = cdb.fetch(MatchEdgeCriteria(id=b.id_))
# pprint(b)

# Delete operations
cdb.delete(MatchNodeCriteria(label_="", properties_={"product_key": "ALBIS"}), detach=True)
c = MatchNodeCriteria(label_="Product", properties_={"name": "abc"})
p = cdb.fetch(c, unnest_result="rc")
if p is not None:
    cdb.delete(p)

cdb.commit()

"""Fluent Cypher query builder.

An optional, opt-in chainable builder for constructing Cypher queries with
automatic parameter binding, analogous to SQLAlchemy Core.

This API is intentionally **not** re-exported at the top-level ``cypher_graphdb``
package; import it explicitly from here::

    from cypher_graphdb.cypherquery import CypherQuery, node, rel, param

The bare ``node`` / ``rel`` names live here (not top level) so they do not
collide with the top-level ``@node`` model decorator. Callers who need both in
one scope alias on import (e.g. ``from cypher_graphdb.cypherquery import node as
cnode``).
"""

from ..exceptions import CypherQueryError
from .builder import CypherQuery
from .params import Param, param
from .patterns import NodePattern, PathPattern, Predicate, PropertyRef, RelPattern, node, rel

__all__ = [
    "CypherQuery",
    "CypherQueryError",
    "Param",
    "param",
    "node",
    "rel",
    "NodePattern",
    "RelPattern",
    "PathPattern",
    "PropertyRef",
    "Predicate",
]

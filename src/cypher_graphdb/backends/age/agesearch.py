"""AGE search module: Full-text search functionality for Apache AGE.

This module provides specialized Cypher query parsing and processing for
full-text search operations in Apache AGE graph databases.
"""

from antlr4.Token import CommonToken
from antlr4.tree.Tree import TerminalNodeImpl

from cypher_graphdb.cypher.CypherParser import CypherParser
from cypher_graphdb.cypherparser import CypherQueryListener, ParsedCypherQuery, parse_cypher_query


class AGECypherListener(CypherQueryListener):
    """Specialized Cypher query listener for Apache AGE.

    Extends the base CypherQueryListener to add AGE-specific query processing,
    particularly for handling type casting of return values.
    """

    def exitOC_ProjectionItems(self, ctx: CypherParser.OC_ProjectionItemsContext):
        # add implicit returns, casted to text with AGE helper function "ag_catalog.agtype_out()"
        return_expressions = list(self.return_arguments.values())

        for expr in return_expressions:
            cast_expr = f"ag_catalog.agtype_out({expr})"

            token = CommonToken()
            token.text = f",{cast_expr}"
            node = TerminalNodeImpl(token)
            ctx.addChild(node)

            self._add_return(cast_expr)


def convert_to_fts_query(cypher_query: ParsedCypherQuery) -> ParsedCypherQuery:
    return parse_cypher_query(cypher_query.submitted_query, AGECypherListener())

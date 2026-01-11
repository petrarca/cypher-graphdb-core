"""cypherparser module: Cypher query parsing and analysis.

Provides classes and functions for parsing Cypher queries using ANTLR,
extracting query structure, clauses, and metadata for analysis and processing.
"""

import re

from antlr4 import CommonTokenStream, InputStream, ParseTreeWalker
from pydantic import BaseModel, Field

from .cypher.CypherLexer import CypherLexer
from .cypher.CypherListener import CypherListener
from .cypher.CypherParser import CypherParser

# Pre-compiled regex for parameter extraction
_PARAM_PATTERN = re.compile(r"\$\w+")


class CypherClausePart(BaseModel):
    """Represents a part of a Cypher clause (node, edge, etc.).

    Attributes:
        query_part: The raw query text for this part.
        part_type: Type of the part (NODE, EDGE, SET_ITEM, etc.).
        varname: Variable name used in the query.
        labels: List of labels associated with this part.
        parameters: List of parameter names (e.g., ['$key', '$name']) extracted from properties.

    """

    query_part: str
    part_type: str
    varname: str = None
    labels: list[str] = []
    parameters: list[str] = []


class CypherClause(BaseModel):
    """Represents a complete Cypher clause.

    Attributes:
        query_clause: The raw query text for this clause.
        clause: The clause type identifier.
        updating_clause: Whether this is an updating clause.
        parts: List of clause parts within this clause.
        labels: List of labels used in this clause.

    """

    query_clause: str
    clause: str
    updating_clause: bool
    parts: list[CypherClausePart] = []
    labels: list[str] | None = []


class ParsedCypherQuery(BaseModel):
    """Represents a fully parsed Cypher query with metadata.

    Attributes:
        submitted_query: The original query string submitted.
        clauses: List of parsed clauses in the query.
        return_arguments: Dictionary of return clause arguments.
        var_to_labels: Mapping of variables to their labels.
        parameters: List of parameter names found in the query (e.g., ['$name', '$age']).
        parse_tree: The ANTLR parse tree (excluded from serialization).

    """

    submitted_query: str
    clauses: list[CypherClause]
    return_arguments: dict = {}
    var_to_labels: dict = {}
    parameters: list[str] = []
    parse_tree: object = Field(exclude=True)

    @property
    def parsed_query(self) -> str:
        """Get the parsed query text without EOF marker.

        Returns:
            Clean query text from the parse tree.

        """
        return re.sub(r"<EOF>$", "", self.parse_tree.getText())

    def __str__(self):
        """Return string representation of the parsed query."""
        return self.parsed_query

    def has_updating_clause(self) -> bool:
        """Check if the query contains any updating clauses.

        Returns:
            True if query has CREATE, SET, DELETE, or other updating clauses.

        """
        return any(c.updating_clause for c in self.clauses)

    def get_parameters(self) -> list[str]:
        """Extract all parameter names from the query.

        Returns:
            List of parameter names (e.g., ['$name', '$age']) found in the query.
            Parameters are deduplicated while preserving order.

        """
        parameters = []
        for clause in self.clauses:
            for part in clause.parts:
                if part.parameters:
                    parameters.extend(part.parameters)

        # Remove duplicates while preserving order
        seen = set()
        unique_parameters = [p for p in parameters if not (p in seen or seen.add(p))]
        return unique_parameters

    def find_clause_parts(self, part_types: tuple[str]) -> tuple[CypherClausePart]:
        """Find clause parts of specified types.

        Args:
            part_types: Tuple of part type names to search for.

        Returns:
            Generator of matching clause parts.

        """
        return (p for c in self.clauses for p in c.parts if p.part_type in part_types)

    def resolve(self):
        """Resolve variable to label mappings in the query."""
        for c in self.clauses:
            for p in c.parts:
                if p.labels and p.varname:
                    self.var_to_labels[p.varname] = p.labels

        for set_item_part in self.find_clause_parts(("SET_ITEM", "DELETE_ITEM")):
            varname = set_item_part.varname.partition(".")[:1][0]
            labels = self.var_to_labels.get(varname, [])
            set_item_part.labels = labels

        for c in self.clauses:
            labels = set()
            for p in c.parts:
                labels = set(list(labels) + p.labels)
            c.labels = list(labels)

        # Extract and populate parameters
        self.parameters = self.get_parameters()

    def is_valid_syntax(self) -> bool:
        """Check if the parsed query has valid Cypher syntax.

        ANTLR is lenient and parses invalid syntax into empty structures.
        This method determines if the query actually contains meaningful
        Cypher clauses or if it's essentially invalid.

        Returns:
            True if the query contains valid Cypher clauses, False otherwise.
        """
        # Consider invalid if no clauses were parsed but query isn't empty
        if not self.submitted_query.strip():
            return False

        # Valid if we found at least one clause or return arguments
        return len(self.clauses) > 0 or bool(self.return_arguments)


class CypherQueryListener(CypherListener):
    """ANTLR listener for parsing Cypher queries.

    Walks the parse tree and extracts clause information, variables,
    labels, and other metadata from Cypher queries.

    Note: This class overrides ANTLR-generated listener methods and does not
    expose additional public API that requires individual method documentation.
    """

    def __init__(self):
        """Initialize the listener with empty state."""
        self.clauses = []
        self.return_arguments = {}

        self._inside_return = False
        self._expression = ""
        self._var_counter = 0

        self._current_clause = None
        self._current_clause_part = None
        self._regex_clause = re.compile(r"^.*?(?= | \(|$)")

    @property
    def inside_return(self):
        return self._inside_return

    def enterOC_Return(self, ctx: CypherParser.OC_ReturnContext):
        self._inside_return = True
        self._var_counter = 0

    def exitOC_Return(self, ctx: CypherParser.OC_ReturnContext):
        self._inside_return = False

    def enterOC_ProjectionItems(self, ctx: CypherParser.OC_ProjectionItemsContext):
        if self._inside_return:
            self.return_arguments.clear()
            # Check if this is RETURN * by looking for the wildcard token
            text = ctx.getText()
            if text.startswith("*"):
                # Add wildcard as a special return argument
                self._add_return("*", "*")

    def exitOC_ProjectionItem(self, ctx: CypherParser.OC_ProjectionItemContext):
        if self._inside_return:
            # Check if there's an AS alias
            alias = None
            if ctx.AS() and ctx.oC_Variable():
                alias = ctx.oC_Variable().getText()
            self._add_return(self._expression, alias)

    def exitOC_Expression(self, ctx: CypherParser.OC_VariableContext):
        if self._inside_return:
            self._expression = ctx.getText()
            return

        if self._current_clause and self._current_clause.clause == "DELETE":
            varname = ctx.getText()
            self._current_clause_part = CypherClausePart(query_part=ctx.getText(), part_type="DELETE_ITEM", varname=varname)
            self._exit_clause_part()

    def enterOC_ReadingClause(self, ctx: CypherParser.OC_UpdatingClauseContext):
        self._enter_clause(ctx.getText(), False)

    def exitOC_ReadingClause(self, ctx: CypherParser.OC_ReadingClauseContext):
        self._exit_clause()

    def enterOC_UpdatingClause(self, ctx: CypherParser.OC_UpdatingClauseContext):
        self._enter_clause(ctx.getText(), True)

    def exitOC_UpdatingClause(self, ctx: CypherParser.OC_UpdatingClauseContext):
        self._exit_clause()

    def enterOC_NodePattern(self, ctx: CypherParser.OC_NodePatternContext):
        if self._current_clause:
            self._current_clause_part = CypherClausePart(part_type="NODE", query_part=ctx.getText())

    def exitOC_NodePattern(self, ctx: CypherParser.OC_NodePatternContext):
        assert self._current_clause and self._current_clause_part
        self._exit_clause_part()

    def enterOC_RelationshipDetail(self, ctx: CypherParser.OC_RelationshipDetailContext):
        if self._current_clause:
            self._current_clause_part = CypherClausePart(part_type="EDGE", query_part=ctx.getText())

    def exitOC_RelationshipDetail(self, ctx: CypherParser.OC_RelationshipDetailContext):
        assert self._current_clause and self._current_clause_part

        self._exit_clause_part()

    def exitOC_RelTypeName(self, ctx: CypherParser.OC_RelTypeNameContext):
        if self._current_clause_part:
            self._current_clause_part.labels.append(ctx.getText().strip())

    def exitOC_NodeLabel(self, ctx: CypherParser.OC_NodeLabelContext):
        if self._current_clause_part:
            self._current_clause_part.labels.append(ctx.getText().strip())

    def exitOC_Variable(self, ctx: CypherParser.OC_ExpressionContext):
        if self._current_clause_part and not self._current_clause_part.varname:
            self._current_clause_part.varname = ctx.getText()

    def exitOC_Properties(self, ctx: CypherParser.OC_PropertiesContext):
        if self._current_clause_part:
            self._current_clause_part.parameters = _PARAM_PATTERN.findall(ctx.getText())

    def enterOC_SetItem(self, ctx: CypherParser.OC_SetItemContext):
        if self._current_clause:
            self._current_clause_part = CypherClausePart(part_type="SET_ITEM", query_part=ctx.getText())

    def exitOC_SetItem(self, ctx: CypherParser.OC_SetItemContext):
        self._exit_clause_part()

    def exitOC_PropertyExpression(self, ctx: CypherParser.OC_PropertyExpressionContext):
        if self._current_clause_part:
            self._current_clause_part.varname = ctx.getText().strip()

    def _add_return(self, expression, alias=None):
        # Use alias if provided, otherwise use expression as key
        if alias:
            key = alias
        else:
            key = expression
        self.return_arguments[key] = expression

    def _enter_clause(self, text: str, updating_clause: bool):
        clause = self._regex_clause.match(text)
        assert clause

        self._current_clause = CypherClause(
            query_clause=text,
            clause=clause.group().upper(),
            updating_clause=updating_clause,
        )

    def _exit_clause(self):
        self.clauses.append(self._current_clause)
        self._current_clause = None

    def _exit_clause_part(self):
        self._current_clause.parts.append(self._current_clause_part)
        self._current_clause_part = None


def parse_cypher_query(cypher_str: str, listener: CypherQueryListener | None = None) -> "ParsedCypherQuery":
    """Parse a Cypher query string into a structured representation.

    Args:
        cypher_str: The Cypher query string to parse.
        listener: Optional custom listener for parsing. If None, creates a default listener.

    Returns:
        ParsedCypherQuery object containing the parsed query structure with clauses,
        variables, labels, and metadata.

    """
    if listener is None:
        listener = CypherQueryListener()

    input_stream = InputStream(cypher_str)
    lexer = CypherLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = CypherParser(stream)
    tree = parser.oC_Cypher()

    ParseTreeWalker().walk(listener, tree)

    result = ParsedCypherQuery(
        submitted_query=cypher_str, clauses=listener.clauses, return_arguments=listener.return_arguments, parse_tree=tree
    )
    result.resolve()

    return result

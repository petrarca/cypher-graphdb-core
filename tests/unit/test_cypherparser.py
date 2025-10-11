"""Tests for Cypher query parser functionality."""

from cypher_graphdb.cypherparser import parse_cypher_query


def test_parse_simple_return_without_alias():
    """Test parsing RETURN clause without AS aliases.

    Parser uses the expression itself as key when no alias is provided.
    """
    query = "MATCH (p:Person) RETURN p.name, p.age"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "p.name": "p.name",
        "p.age": "p.age",
    }


def test_parse_return_with_aliases():
    """Test parsing RETURN clause with AS aliases."""
    query = "MATCH (p:Person) RETURN p.name AS name, p.age AS age"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "name": "p.name",
        "age": "p.age",
    }


def test_parse_return_mixed_aliases():
    """Test parsing RETURN clause with mixed aliases and non-aliases.

    Aliases are used when provided, otherwise expression is used as key.
    """
    query = "MATCH (p:Person) RETURN p.name AS name, p.age, p.email AS email"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "name": "p.name",
        "p.age": "p.age",
        "email": "p.email",
    }


def test_parse_return_with_function():
    """Test parsing RETURN clause with aggregation function."""
    query = "MATCH (p:Person) RETURN count(p) AS total"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "total": "count(p)",
    }


def test_parse_return_function_without_alias():
    """Test parsing RETURN clause with function but no alias.

    Expression (including function call) is used as key when no alias.
    """
    query = "MATCH (p:Person) RETURN count(p)"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "count(p)": "count(p)",
    }


def test_parse_return_node_reference():
    """Test parsing RETURN clause with node reference.

    Node reference is used as key when no alias is provided.
    """
    query = "MATCH (p:Person) RETURN p"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "p": "p",
    }


def test_parse_return_multiple_nodes():
    """Test parsing RETURN clause with multiple node references.

    Each node reference is used as its own key.
    """
    query = "MATCH (p:Person)-[r:KNOWS]->(f:Person) RETURN p, r, f"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "p": "p",
        "r": "r",
        "f": "f",
    }


def test_parse_return_complex_expression():
    """Test parsing RETURN clause with complex expressions.

    Parser preserves whitespace in expressions and uses expression as key
    when no alias is provided.
    """
    query = "MATCH (p:Person) RETURN p.age + 1 AS next_age, p.name"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "next_age": "p.age + 1",
        "p.name": "p.name",
    }


def test_parse_return_distinct():
    """Test parsing RETURN DISTINCT clause."""
    query = "MATCH (p:Person) RETURN DISTINCT p.name AS name"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "name": "p.name",
    }


def test_parse_return_with_order_by():
    """Test parsing RETURN clause with ORDER BY.

    Uses alias when provided, expression otherwise.
    """
    query = "MATCH (p:Person) RETURN p.name AS name, p.age ORDER BY p.age DESC"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "name": "p.name",
        "p.age": "p.age",
    }


def test_parse_return_with_limit():
    """Test parsing RETURN clause with LIMIT."""
    query = "MATCH (p:Person) RETURN p.name AS name LIMIT 10"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "name": "p.name",
    }


def test_parse_return_with_skip_and_limit():
    """Test parsing RETURN clause with SKIP and LIMIT."""
    query = "MATCH (p:Person) RETURN p.name AS name SKIP 5 LIMIT 10"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "name": "p.name",
    }


def test_parse_return_case_sensitive_aliases():
    """Test that aliases preserve case sensitivity."""
    query = "MATCH (p:Person) RETURN p.name AS Name, p.age AS AGE"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "Name": "p.name",
        "AGE": "p.age",
    }


def test_parse_return_with_properties():
    """Test parsing RETURN with property access."""
    query = "MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) RETURN p.name AS person, r.role AS role, c.name AS company"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "person": "p.name",
        "role": "r.role",
        "company": "c.name",
    }


def test_parse_create_query():
    """Test parsing CREATE query (no RETURN clause)."""
    query = "CREATE (p:Person {name: 'Alice', age: 30})"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {}
    assert parsed.has_updating_clause() is True


def test_parse_match_without_return():
    """Test parsing MATCH without RETURN clause."""
    query = "MATCH (p:Person)"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {}


def test_parse_submitted_query():
    """Test that submitted query is preserved."""
    query = "MATCH (p:Person) RETURN p.name AS name"
    parsed = parse_cypher_query(query)

    assert parsed.submitted_query == query


def test_parse_parsed_query_property():
    """Test that parsed_query property returns original query."""
    query = "MATCH (p:Person) RETURN p.name AS name"
    parsed = parse_cypher_query(query)

    assert parsed.parsed_query == query


def test_parse_clauses_extracted():
    """Test that clauses are properly extracted."""
    query = "MATCH (p:Person) RETURN p.name AS name"
    parsed = parse_cypher_query(query)

    assert len(parsed.clauses) > 0
    clause_types = [c.clause for c in parsed.clauses]
    assert "MATCH" in clause_types


def test_parse_aggregation_functions():
    """Test parsing various aggregation functions."""
    queries = [
        ("MATCH (p:Person) RETURN count(p) AS total", {"total": "count(p)"}),
        (
            "MATCH (p:Person) RETURN sum(p.age) AS total_age",
            {"total_age": "sum(p.age)"},
        ),
        (
            "MATCH (p:Person) RETURN avg(p.age) AS avg_age",
            {"avg_age": "avg(p.age)"},
        ),
        (
            "MATCH (p:Person) RETURN min(p.age) AS min_age",
            {"min_age": "min(p.age)"},
        ),
        (
            "MATCH (p:Person) RETURN max(p.age) AS max_age",
            {"max_age": "max(p.age)"},
        ),
    ]

    for query, expected in queries:
        parsed = parse_cypher_query(query)
        assert parsed.return_arguments == expected


def test_parse_return_with_collect():
    """Test parsing RETURN with collect function."""
    query = "MATCH (p:Person) RETURN collect(p.name) AS names"
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "names": "collect(p.name)",
    }


def test_parse_return_star():
    """Test parsing RETURN * (returns all variables)."""
    query = "MATCH (p:Person) RETURN *"
    parsed = parse_cypher_query(query)

    # RETURN * is captured as a wildcard
    assert parsed.return_arguments == {"*": "*"}


def test_parse_return_star_with_additional_columns():
    """Test parsing RETURN * with additional explicit columns.

    Wildcard plus explicit columns. Non-aliased columns use expression as key.
    """
    query = "MATCH (p:Person) RETURN *, p.name AS name, p.age"
    parsed = parse_cypher_query(query)

    # Wildcard plus explicit columns
    assert parsed.return_arguments == {
        "*": "*",
        "name": "p.name",
        "p.age": "p.age",
    }


def test_parse_complex_query_with_multiple_clauses():
    """Test parsing complex query with multiple clauses."""
    query = """
        MATCH (p:Person)-[r:WORKS_FOR]->(c:Company)
        WHERE p.age > 25 AND c.name = 'TechCorp'
        RETURN p.name AS employee, r.role AS role, c.name AS company
        ORDER BY p.name
        LIMIT 10
    """
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {
        "employee": "p.name",
        "role": "r.role",
        "company": "c.name",
    }


def test_parse_union_query():
    """Test parsing UNION query."""
    query = """
        MATCH (p:Person) RETURN p.name AS name
        UNION
        MATCH (c:Company) RETURN c.name AS name
    """
    parsed = parse_cypher_query(query)

    # Both RETURN clauses should be captured (last one takes precedence)
    assert "name" in parsed.return_arguments


def test_parse_with_clause():
    """Test parsing WITH clause (similar to RETURN but intermediate).

    Should capture the final RETURN clause. Variable reference uses
    itself as key.
    """
    query = "MATCH (p:Person) WITH p.name AS name RETURN name"
    parsed = parse_cypher_query(query)

    # Should capture the final RETURN clause
    assert parsed.return_arguments == {
        "name": "name",
    }


def test_parse_empty_query():
    """Test parsing empty query string."""
    query = ""
    parsed = parse_cypher_query(query)

    assert parsed.return_arguments == {}


def test_parse_return_with_coalesce():
    """Test parsing RETURN with COALESCE function."""
    query = "MATCH (p:Person) RETURN COALESCE(p.nickname, p.name) AS display_name"
    parsed = parse_cypher_query(query)

    # Parser preserves whitespace in function calls
    assert parsed.return_arguments == {
        "display_name": "COALESCE(p.nickname, p.name)",
    }


def test_parse_return_with_case_expression():
    """Test parsing RETURN with CASE expression."""
    query = """
        MATCH (p:Person)
        RETURN p.name AS name,
               CASE WHEN p.age >= 18 THEN 'adult' ELSE 'minor' END AS category
    """
    parsed = parse_cypher_query(query)

    assert "name" in parsed.return_arguments
    assert "category" in parsed.return_arguments
    assert parsed.return_arguments["name"] == "p.name"


def test_parse_return_with_list_comprehension():
    """Test parsing RETURN with list comprehension."""
    query = "MATCH (p:Person) RETURN [x IN p.hobbies WHERE x <> ''] AS valid_hobbies"
    parsed = parse_cypher_query(query)

    assert "valid_hobbies" in parsed.return_arguments


def test_parse_multiple_sequential_returns():
    """Test that parser captures the last RETURN clause in complex queries."""
    query = """
        MATCH (p:Person)
        WITH p.name AS name, p.age AS age
        WHERE age > 18
        RETURN name, age AS person_age
    """
    parsed = parse_cypher_query(query)

    # Should capture the final RETURN
    has_p1 = "p1" in parsed.return_arguments
    has_name = "name" in parsed.return_arguments
    has_age = "person_age" in parsed.return_arguments
    has_age = has_age or "age" in parsed.return_arguments
    assert has_p1 or has_name
    assert has_age

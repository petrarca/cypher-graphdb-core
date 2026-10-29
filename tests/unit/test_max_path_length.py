"""The parser reports a query's longest path; execute(..., max_path_length=) rejects a longer one before it runs."""

from __future__ import annotations

import pytest

from cypher_graphdb import PathLengthExceededError, QueryExecutionError
from cypher_graphdb.cyphergraphdb import CypherGraphDB
from cypher_graphdb.cypherparser import parse_cypher_query

from .mock_backend import MockBackend


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("MATCH (n) RETURN n", 0),
        ("MATCH (a)-[:R]->(b) RETURN a", 1),
        ("MATCH (a)-[:R]->(b)<-[:S]-(c) RETURN a", 2),
        ("MATCH (a)-[:R*3]->(b) RETURN a", 3),
        ("MATCH (a)-[:R*1..5]->(b)-[*..4]->(c) RETURN a", 9),
        ("MATCH (a)-[:R* 1 .. 7]->(b) RETURN a", 7),
        ("MATCH (a)-[:R*1..3]->(b) MATCH (c)-[*1..9]-(d) RETURN a", 9),
        ("MATCH (a)-[*]->(b) RETURN a", None),
        ("MATCH (a)-[:R*2..]->(b) RETURN a", None),
        ("MATCH (a) WHERE (a)-[:R*]->() RETURN a", None),
    ],
)
def test_the_parser_reports_the_longest_path(query, expected):
    assert parse_cypher_query(query).max_path_length == expected


class _ReachedBackend(MockBackend):
    """Fails every statement that reaches it -- so a QueryExecutionError means the query ran."""

    def execute_cypher(self, cypher_query, fetch_one=False, raw_data=False, params=None):
        raise RuntimeError("reached the backend")


@pytest.fixture
def cdb() -> CypherGraphDB:
    return CypherGraphDB(_ReachedBackend())


@pytest.mark.parametrize("run", ["execute", "execute_with_stats"])
@pytest.mark.parametrize(
    ("query", "found"),
    [("MATCH (a)-[*]->(b) RETURN a", None), ("MATCH (a)-[*1..11]->(b) RETURN a", 11)],
)
def test_a_longer_path_is_rejected_before_it_runs(cdb, run, query, found):
    with pytest.raises(PathLengthExceededError, match=r"bound its variable-length relationships, e\.g\. \[:REL\*1\.\.10\]") as ei:
        getattr(cdb, run)(query, max_path_length=10)
    assert (ei.value.max_path_length, ei.value.limit) == (found, 10)


def test_a_page_is_rejected_too(cdb):
    with pytest.raises(PathLengthExceededError):
        cdb.execute_cypher_page("MATCH (a)-[*]->(b) RETURN a", max_path_length=10)


@pytest.mark.parametrize("limit", [10, None])
def test_a_path_within_the_limit_or_without_one_runs(cdb, limit):
    with pytest.raises(QueryExecutionError, match="reached the backend"):
        cdb.execute("MATCH (a)-[*1..10]->(b) RETURN a" if limit else "MATCH (a)-[*]->(b) RETURN a", max_path_length=limit)

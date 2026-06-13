"""Integration tests: backend execution failures surface as QueryExecutionError.

A malformed Cypher query makes the underlying driver raise a backend-specific
exception (Memgraph's ``mgclient.DatabaseError``, AGE's ``AGEExecutionError``).
The facade must wrap those into the backend-agnostic ``QueryExecutionError`` so
no backend-specific exception escapes the library. Runs against both live
backends.
"""

import pytest

from cypher_graphdb import QueryExecutionError

pytestmark = pytest.mark.integration


@pytest.fixture
def test_db(request):
    """Parametrized fixture providing both Memgraph and AGE database connections."""
    return request.getfixturevalue(request.param)


class TestQueryExecutionError:
    """Bad queries raise the agnostic QueryExecutionError on every execution path."""

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_syntax_error_via_execute_with_stats(self, test_db):
        with pytest.raises(QueryExecutionError) as ei:
            test_db.execute_with_stats("MATCH (n RETURN n")
        # The backend's error text is preserved for surfacing to the user.
        assert str(ei.value)

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_syntax_error_via_execute(self, test_db):
        with pytest.raises(QueryExecutionError):
            test_db.execute("MATCH (n RETURN n")

    @pytest.mark.parametrize("test_db", ["memgraph_db", "age_db"], indirect=True)
    def test_not_cypher_at_all(self, test_db):
        with pytest.raises(QueryExecutionError):
            test_db.execute("THIS IS NOT CYPHER")

"""Integration tests: a statement's own timeout cancels it with QueryTimeoutError, on every backend that can apply it.

``execute(..., timeout_s=)`` and ``with cdb.query_timeout(...)`` override the connection's default for
the statements inside; the connection stays usable afterwards. Memgraph through pymgclient cannot
send a transaction timeout: an override is logged as a warning and the connection's default holds.
"""

import time

import pytest
from loguru import logger

from cypher_graphdb import CypherGraphDB, QueryTimeoutError

pytestmark = pytest.mark.integration

# Runs for minutes on every backend; streams its rows, so no memory limit stops it first.
_SLOW = "UNWIND range(1, 20000) AS a UNWIND range(1, 20000) AS b WITH a, b WHERE (a + b) % 97 = 1 RETURN count(*) AS c"


@pytest.fixture
def memgraph_neo4j_db(memgraph_container):
    db = CypherGraphDB(
        backend="memgraph",
        connect_params={"host": memgraph_container["host"], "port": memgraph_container["port"], "transport": "neo4j"},
    )
    yield db
    db.disconnect()


@pytest.fixture
def timeout_db(request):
    return request.getfixturevalue(request.param)


@pytest.mark.parametrize("timeout_db", ["age_db", "neo4j_db", "memgraph_neo4j_db"], indirect=True)
class TestStatementTimeout:
    def test_a_statement_timeout_cancels_it(self, timeout_db):
        start = time.monotonic()
        with pytest.raises(QueryTimeoutError):
            timeout_db.execute(_SLOW, timeout_s=1)
        assert time.monotonic() - start < 15
        assert timeout_db.execute("RETURN 1", unnest_result=True) == 1  # the connection is usable again

    def test_a_block_timeout_covers_its_statements(self, timeout_db):
        with timeout_db.query_timeout(1), pytest.raises(QueryTimeoutError):
            timeout_db.execute(_SLOW)
        timeout_db.rollback()
        assert timeout_db.execute("RETURN 2", unnest_result=True) == 2  # after the block: the connection's default again

    def test_a_fast_statement_is_not_affected(self, timeout_db):
        assert timeout_db.execute("RETURN 3", unnest_result=True, timeout_s=5) == 3


def test_pymgclient_logs_a_statement_timeout_it_cannot_apply(memgraph_db):
    if memgraph_db._backend.transport != "pymgclient":
        pytest.skip("only pymgclient cannot send a transaction timeout")
    messages: list[str] = []
    sink = logger.add(lambda m: messages.append(str(m)), level="WARNING")
    try:
        assert memgraph_db.execute("RETURN 4", unnest_result=True, timeout_s=5) == 4
    finally:
        logger.remove(sink)
    assert any("cannot send a transaction timeout" in m for m in messages)

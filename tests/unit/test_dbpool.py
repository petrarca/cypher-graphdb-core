import time

import pytest

from cypher_graphdb.backendprovider import backend_provider
from cypher_graphdb.cyphergraphdb import CypherGraphDB
from cypher_graphdb.dbpool import CypherGraphDBPool

from .mock_backend import MockBackend

# Ensure the mock backend is registered (idempotent)
if "mock" not in backend_provider:
    backend_provider.register("mock", MockBackend)


@pytest.mark.unit
def test_pool_basic():
    # pick first registered backend id if available
    backend_id = "mock"

    pool = CypherGraphDBPool(
        backend_id,
        connect_params=None,
        pool_size=2,
        ttl=0.5,
        auto_connect=False,
    )

    db1 = pool.get()
    assert isinstance(db1, CypherGraphDB)

    db2 = pool.get()
    assert db1 is not db2

    with pytest.raises(RuntimeError):
        pool.get()  # pool exhausted

    pool.release(db1)
    # now should be able to get again
    db3 = pool.get()
    assert db3 is db1  # reused

    pool.release(db2)
    pool.release(db3)

    # let one expire
    time.sleep(0.6)
    # internal purge occurs on get
    db4 = pool.get()
    assert isinstance(db4, CypherGraphDB)

    pool.close()
    pool.release(db4)  # after close release should just disconnect


@pytest.mark.unit
def test_pool_close_releases_all():
    backend_id = "mock"
    pool = CypherGraphDBPool(backend_id, pool_size=1)
    db = pool.get()
    pool.close()
    # releasing after close should not raise
    pool.release(db)
    with pytest.raises(RuntimeError):
        pool.get()

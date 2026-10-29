"""Benchmark a Bolt backend: bulk writes, reads as models, streams, and queries of the sizes code uses.

    task bench -- bolt://localhost:7687                       # memgraph, the Neo4j driver
    task bench -- "bolt://localhost:7687?transport=pymgclient" # memgraph, pymgclient
    task bench -- bolt://localhost:7688 neo4j                 # the neo4j backend

Writes and deletes nodes labelled ``Bench`` (and edges ``BENCH_E``) on the server it is pointed at.
"""

import statistics
import sys
import time

from loguru import logger

from cypher_graphdb import CypherGraphDB

N = 20000


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "bolt://localhost:7687"
    backend = sys.argv[2] if len(sys.argv) > 2 else "memgraph"
    logger.remove()
    db = CypherGraphDB(backend=backend, connect_url=url)
    print(f"{backend} via {db._backend.transport}: {url}")  # noqa: T201
    db.execute("MATCH (n:Bench) DETACH DELETE n")
    db.create_property_index("Bench", "id")

    def timed(label: str, fn, runs: int = 3, per: int = 1) -> None:
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            fn()
            times.append((time.perf_counter() - start) / per)
        print(f"  {label:<36} {statistics.median(times) * 1000:9.2f} ms")  # noqa: T201

    def create() -> None:
        db.execute("MATCH (n:Bench) DETACH DELETE n")
        db.bulk_create_nodes([{"id": i, "name": f"node{i}", "score": i * 0.5} for i in range(N)], label="Bench", batch_size=1000)
        db.bulk_create_edges([{"src": i, "dst": i + 1, "w": i} for i in range(N - 1)], "Bench", "Bench", label="BENCH_E", batch_size=1000)

    timed(f"bulk create {N} nodes + edges", create)
    timed(f"read {N} nodes as models", lambda: db.execute("MATCH (n:Bench) RETURN n"))
    timed(f"read {N} edges as models", lambda: db.execute("MATCH ()-[e:BENCH_E]->() RETURN e"))
    timed(f"stream {N} nodes, chunks of 1000", lambda: sum(len(c) for c in db.execute_cypher_stream("MATCH (n:Bench) RETURN n", chunk_size=1000)))
    for size, reps in ((1, 500), (10, 500), (100, 300), (1000, 100)):
        timed(
            f"query of {size} nodes (per query)",
            lambda size=size, reps=reps: [db.execute("MATCH (n:Bench) WHERE n.id < $n RETURN n", params={"n": size}) for _ in range(reps)],
            runs=1,
            per=reps,
        )
    db.execute("MATCH (n:Bench) DETACH DELETE n")
    db.drop_index("Bench", "id")
    db.disconnect()


if __name__ == "__main__":
    main()

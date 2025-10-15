#!/usr/bin/env python3
"""Quick test to verify AGE connection parameters."""

from cypher_graphdb import CypherGraphDB

# Test with direct parameters (simulating what testcontainer provides)
print("Testing AGE connection with parameters...")

try:
    db = CypherGraphDB(
        backend="age",
        connect_params={
            "host": "localhost",
            "port": 8432,  # From .env.age
            "dbname": "cgdb",
            "user": "postgres",
            "password": "postgres",
            "graph_name": "test_graph",
            "create_graph": True,
            "read_only": False,
        },
    )

    print("✅ Connection successful!")
    print(f"Backend: {db.backend.id}")
    print(f"Graph name: {db.graph_name}")

    # Test a simple query
    result = db.execute("RETURN 1 AS test", unnest_result=True)
    print(f"Test query result: {result}")

    db.disconnect()
    print("✅ Disconnected successfully!")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback

    traceback.print_exc()

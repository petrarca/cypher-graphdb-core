"""Debug script to test AGE count query."""

from cypher_graphdb import CypherGraphDB

# Connect to AGE
db = CypherGraphDB(
    backend="age",
    connect_params={
        "host": "localhost",
        "port": 5432,
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "graph_name": "test_graph",
    },
)

db.connect()

try:
    # Clean up first
    db.execute("MATCH (p:Person) DELETE p")

    # Create a test node
    db.execute("CREATE (p:Person {name: 'Test'})")
    print("✓ Created node")

    # Try to count with different approaches
    print("\n1. Testing RETURN count(p):")
    try:
        result = db.execute("MATCH (p:Person {name: 'Test'}) RETURN count(p)")
        print(f"  Raw result: {result}")
        print(f"  Type: {type(result)}")
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n2. Testing RETURN count(p) with unnest_result='rc':")
    try:
        result = db.execute(
            "MATCH (p:Person {name: 'Test'}) RETURN count(p)",
            unnest_result="rc",
        )
        print(f"  Result: {result}")
        print(f"  Type: {type(result)}")
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n3. Testing RETURN count(p) AS cnt:")
    try:
        result = db.execute("MATCH (p:Person {name: 'Test'}) RETURN count(p) AS cnt")
        print(f"  Raw result: {result}")
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n4. Testing RETURN count(p) AS cnt with unnest_result='rc':")
    try:
        result = db.execute(
            "MATCH (p:Person {name: 'Test'}) RETURN count(p) AS cnt",
            unnest_result="rc",
        )
        print(f"  Result: {result}")
        print(f"  Type: {type(result)}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # Cleanup
    db.execute("MATCH (p:Person) DELETE p")
    print("\n✓ Cleanup complete")

finally:
    db.disconnect()

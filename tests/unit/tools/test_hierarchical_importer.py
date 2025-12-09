"""Unit tests for HierarchicalImporter (YAML/JSON) using mock backend.

Tests cover:
- Node creation with GID preservation
- Relationship resolution and creation
- Two-phase processing
- Data flattening integration
- Error handling

Note: Access to _backend is intentional in tests - we need to verify
internal mock state for proper test coverage.
"""

from cypher_graphdb.tools import HierarchicalImporter

from ..mock_backend import build_db


def test_yaml_import_basic_nodes(tmp_path):
    """Test basic YAML node import with GID preservation."""
    db = build_db()

    # Create test YAML file
    yaml_content = """
Company:
- gid_: apple
  name: Apple Inc.
  founded: 1976
- gid_: google
  name: Google
  founded: 1998

Product:
- gid_: iphone15
  name: iPhone 15
  price: 999
"""

    yaml_file = tmp_path / "test.yml"
    yaml_file.write_text(yaml_content)

    # Import
    importer = HierarchicalImporter(db)
    files_processed = importer.load(str(yaml_file))

    # Verify
    assert len(files_processed) == 1
    assert len(db._backend.nodes) == 3  # noqa: W0212
    assert importer.nodes_created == 3
    assert importer.edges_created == 0

    # Check GIDs preserved
    nodes = list(db._backend.nodes.values())  # noqa: W0212
    node_gids = {node.properties_.get("gid_") for node in nodes}  # noqa: W0212
    assert node_gids == {"apple", "google", "iphone15"}


def test_yaml_import_with_relationships(tmp_path):
    """Test YAML import with embedded relationships."""
    db = build_db()

    yaml_content = """
Product:
- gid_: iphone15
  name: iPhone 15
  price: 999
  ipownedby:
  - gid_: rel_iphone_apple
    target_gid: apple
    target_label: Company
    since: 2007
  usestechnology:
  - gid_: rel_iphone_ios
    target_gid: ios
    target_label: Technology
    version: "17"

Company:
- gid_: apple
  name: Apple Inc.
  founded: 1976

Technology:
- gid_: ios
  name: iOS
  version: "17"
"""

    yaml_file = tmp_path / "test_relations.yml"
    yaml_file.write_text(yaml_content)

    # Import
    importer = HierarchicalImporter(db)
    importer.load(str(yaml_file))

    # Verify nodes and edges
    assert len(db._backend.nodes) == 3  # iPhone + Apple + iOS
    assert len(db._backend.edges) == 2  # ipOwnedBy + usesTechnology
    assert importer.nodes_created == 3
    assert importer.edges_created == 2

    # Check relationship properties
    edge_props = [e.properties_ for e in db._backend.edges.values()]
    assert any("since" in props and props["since"] == 2007 for props in edge_props)


def test_json_import_equivalent_behavior(tmp_path):
    """Test that JSON import produces equivalent results to YAML."""
    db1 = build_db()
    db2 = build_db()

    # Same data in JSON and YAML
    data = {"Company": [{"gid_": "apple", "name": "Apple Inc.", "founded": 1976}]}

    # JSON file
    import json

    json_file = tmp_path / "test.json"
    json_file.write_text(json.dumps(data, indent=2))

    # YAML file
    import yaml

    yaml_file = tmp_path / "test.yml"
    yaml_file.write_text(yaml.dump(data))

    # Import both
    importer1 = HierarchicalImporter(db1)
    importer2 = HierarchicalImporter(db2)

    importer1.load(str(json_file))
    importer2.load(str(yaml_file))

    # Should produce identical results
    assert len(db1._backend.nodes) == len(db2._backend.nodes)
    assert importer1.nodes_created == importer2.nodes_created

    # Check node properties are identical
    nodes1 = list(db1._backend.nodes.values())
    nodes2 = list(db2._backend.nodes.values())

    for n1, n2 in zip(nodes1, nodes2, strict=False):
        assert n1.label_ == n2.label_
        assert n1.properties_ == n2.properties_


def test_gid_caching_for_relationship_resolution(tmp_path):
    """Test that GID caching works correctly for relationship resolution."""
    db = build_db()

    yaml_content = """
Person:
- gid_: alice
  name: Alice
  related_to:
  - gid_: rel_alice_bob
    target_gid: bob
    target_label: Person
  - gid_: rel_alice_charlie
    target_gid: charlie
    target_label: Person
- gid_: bob
  name: Bob
- gid_: charlie
  name: Charlie
"""

    yaml_file = tmp_path / "test_caching.yml"
    yaml_file.write_text(yaml_content)

    # Import
    importer = HierarchicalImporter(db)
    importer.load(str(yaml_file))

    # Verify all nodes created
    assert len(db._backend.nodes) == 3

    # Verify relationships created (Alice knows Bob and Charlie)
    assert len(db._backend.edges) == 2

    # Check that GID cache was used
    assert len(importer.node_cache) == 3
    assert "alice" in importer.node_cache
    assert "bob" in importer.node_cache
    assert "charlie" in importer.node_cache


def test_error_handling_invalid_yaml(tmp_path):
    """Test error handling for invalid YAML files."""
    db = build_db()

    # Invalid YAML
    invalid_yaml = """
Company:
- gid_: apple
  name: Apple Inc.
  founded: 1976
  invalid: [unclosed bracket
"""

    yaml_file = tmp_path / "invalid.yml"
    yaml_file.write_text(invalid_yaml)

    # Import should handle error gracefully
    importer = HierarchicalImporter(db)
    files_processed = importer.load(str(yaml_file))

    # Should return empty list due to error
    assert len(files_processed) == 0
    assert len(db._backend.nodes) == 0


def test_data_type_preservation(tmp_path):
    """Test that data types are preserved during import."""
    db = build_db()

    yaml_content = """
Product:
- gid_: laptop
  name: MacBook Pro
  price: 1999.99        # Float
  in_stock: true         # Boolean
  quantity: 42          # Integer
  tags: [premium, apple] # List
"""

    yaml_file = tmp_path / "test_types.yml"
    yaml_file.write_text(yaml_content)

    # Import
    importer = HierarchicalImporter(db)
    importer.load(str(yaml_file))

    # Verify data types preserved
    node = list(db._backend.nodes.values())[0]
    props = node.properties_

    assert isinstance(props["price"], float)
    assert props["price"] == 1999.99
    assert isinstance(props["in_stock"], bool)
    assert props["in_stock"] is True
    assert isinstance(props["quantity"], int)
    assert props["quantity"] == 42
    assert isinstance(props["tags"], list)
    assert "premium" in props["tags"]


def test_empty_relationship_handling(tmp_path):
    """Test handling of empty or missing relationship data."""
    db = build_db()

    yaml_content = """
Company:
- gid_: apple
  name: Apple Inc.
  founded: 1976
  ipOwnedBy: []  # Empty relationship list
- gid_: startup
  name: Startup
  # No relationships at all
"""

    yaml_file = tmp_path / "test_empty.yml"
    yaml_file.write_text(yaml_content)

    # Import
    importer = HierarchicalImporter(db)
    importer.load(str(yaml_file))

    # Should create nodes but no edges
    assert len(db._backend.nodes) == 2
    assert len(db._backend.edges) == 0
    assert importer.nodes_created == 2
    assert importer.edges_created == 0

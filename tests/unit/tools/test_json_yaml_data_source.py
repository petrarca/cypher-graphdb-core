"""Unit tests for JsonYamlDataSource.

Tests cover:
- YAML file loading
- JSON file loading
- Combined data extraction
- Error handling for invalid files
- Empty file handling
"""

import json

import pytest
import yaml

from cypher_graphdb.tools.json_yaml_data_source import JsonYamlDataSource


def test_load_yaml_file(tmp_path):
    """Test loading a valid YAML file."""
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

    data = JsonYamlDataSource.load_file(str(yaml_file))

    # Verify structure
    assert "Company" in data
    assert "Product" in data
    assert len(data["Company"]) == 2
    assert len(data["Product"]) == 1

    # Verify content
    apple = data["Company"][0]
    assert apple["gid_"] == "apple"
    assert apple["name"] == "Apple Inc."
    assert apple["founded"] == 1976

    iphone = data["Product"][0]
    assert iphone["gid_"] == "iphone15"
    assert iphone["price"] == 999


def test_load_json_file(tmp_path):
    """Test loading a valid JSON file."""
    data = {
        "Company": [{"gid_": "apple", "name": "Apple Inc.", "founded": 1976}],
        "Product": [{"gid_": "iphone15", "name": "iPhone 15", "price": 999}],
    }

    json_file = tmp_path / "test.json"
    json_file.write_text(json.dumps(data, indent=2))

    loaded_data = JsonYamlDataSource.load_file(str(json_file))

    # Should be identical to original
    assert loaded_data == data


def test_load_yaml_with_relationships(tmp_path):
    """Test loading YAML with embedded relationships."""
    yaml_content = """
Product:
- gid_: iphone15
  name: iPhone 15
  ipOwnedBy:
  - gid_: apple
    name: Apple Inc.
    since: 2007
"""

    yaml_file = tmp_path / "test_relations.yml"
    yaml_file.write_text(yaml_content)

    data = JsonYamlDataSource.load_file(str(yaml_file))

    # Verify nested structure preserved
    product = data["Product"][0]
    assert product["gid_"] == "iphone15"
    assert "ipOwnedBy" in product
    assert len(product["ipOwnedBy"]) == 1
    assert product["ipOwnedBy"][0]["gid_"] == "apple"


def test_load_empty_file(tmp_path):
    """Test loading empty files."""
    # Empty YAML
    empty_yaml = tmp_path / "empty.yml"
    empty_yaml.write_text("")

    data = JsonYamlDataSource.load_file(str(empty_yaml))
    assert data == {}

    # Empty JSON
    empty_json = tmp_path / "empty.json"
    empty_json.write_text("")

    with pytest.raises((yaml.YAMLError, ValueError)):  # Should fail on invalid JSON
        JsonYamlDataSource.load_file(str(empty_json))


def test_load_yaml_with_only_comments(tmp_path):
    """Test loading YAML file with only comments."""
    yaml_content = """
# This is a comment
# Another comment
"""

    yaml_file = tmp_path / "comments.yml"
    yaml_file.write_text(yaml_content)

    data = JsonYamlDataSource.load_file(str(yaml_file))
    assert data == {}


def test_load_invalid_yaml(tmp_path):
    """Test error handling for invalid YAML."""
    invalid_yaml = """
Company:
- gid_: apple
  name: Apple Inc.
  invalid: [unclosed bracket
"""

    yaml_file = tmp_path / "invalid.yml"
    yaml_file.write_text(invalid_yaml)

    with pytest.raises((yaml.YAMLError, ValueError)):
        JsonYamlDataSource.load_file(str(yaml_file))


def test_load_invalid_json(tmp_path):
    """Test error handling for invalid JSON."""
    invalid_json = """
{
    "Company": [
        {"gid_": "apple", "name": "Apple Inc."}
    # Missing closing bracket
"""

    json_file = tmp_path / "invalid.json"
    json_file.write_text(invalid_json)

    with pytest.raises((yaml.YAMLError, ValueError)):
        JsonYamlDataSource.load_file(str(json_file))


def test_load_yaml_with_various_data_types(tmp_path):
    """Test loading YAML with various data types."""
    yaml_content = """
Test:
- gid_: types
  name: Data Types
  integer_value: 42
  float_value: 3.14
  boolean_true: true
  boolean_false: false
  null_value: null
  string_value: "hello"
  list_value: [1, 2, 3]
  nested_dict:
    key: value
    number: 123
"""

    yaml_file = tmp_path / "types.yml"
    yaml_file.write_text(yaml_content)

    data = JsonYamlDataSource.load_file(str(yaml_file))

    item = data["Test"][0]
    assert isinstance(item["integer_value"], int)
    assert isinstance(item["float_value"], float)
    assert item["boolean_true"] is True
    assert item["boolean_false"] is False
    assert item["null_value"] is None
    assert isinstance(item["string_value"], str)
    assert isinstance(item["list_value"], list)
    assert isinstance(item["nested_dict"], dict)


def test_load_nonexistent_file():
    """Test loading a file that doesn't exist."""
    with pytest.raises(FileNotFoundError):
        JsonYamlDataSource.load_file("/nonexistent/file.yml")


def test_file_extension_handling(tmp_path):
    """Test that both .yml and .yaml extensions work."""
    content = """
Test:
- gid_: test
  name: Test
"""

    # Test .yml extension
    yml_file = tmp_path / "test.yml"
    yml_file.write_text(content)

    data1 = JsonYamlDataSource.load_file(str(yml_file))
    assert "Test" in data1

    # Test .yaml extension
    yaml_file = tmp_path / "test.yaml"
    yaml_file.write_text(content)

    data2 = JsonYamlDataSource.load_file(str(yaml_file))
    assert "Test" in data2

    # Should be identical
    assert data1 == data2


def test_load_yaml_with_special_characters(tmp_path):
    """Test loading YAML with special characters and unicode."""
    yaml_content = """
International:
- gid_: café
  name: Café Restaurant
  description: "A place with émojis: 🍕🍔"
  symbols: "@#$%^&*()"
  unicode: "こんにちは世界"
"""

    yaml_file = tmp_path / "unicode.yml"
    yaml_file.write_text(yaml_content)

    data = JsonYamlDataSource.load_file(str(yaml_file))

    item = data["International"][0]
    assert item["gid_"] == "café"
    assert "émojis" in item["description"]
    assert "🍕" in item["description"]
    assert item["unicode"] == "こんにちは世界"

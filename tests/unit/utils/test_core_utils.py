"""Tests for core utility functions."""

import cypher_graphdb.utils as utils


def test_split_path():
    assert utils.split_path("/dir1/file.ext") == ("/dir1", "file", ".ext")
    assert utils.split_path("/dir1/dir2/file.ext") == ("/dir1/dir2", "file", ".ext")
    assert utils.split_path("file.ext") == ("", "file", ".ext")
    assert utils.split_path("/file.ext") == ("", "file", ".ext")
    assert utils.split_path("file") == ("", "file", "")


def test_resolve_fileformat():
    assert utils.resolve_fileformat(".xlsx") == "excel"
    assert utils.resolve_fileformat(".csv") == "csv"
    assert utils.resolve_fileformat(".json") == "json"
    assert utils.resolve_fileformat(".xml") == "xml"
    assert utils.resolve_fileformat(".yaml") == "yaml"
    assert utils.resolve_fileformat(".undefined") is None
    assert utils.resolve_fileformat("") is None
    assert utils.resolve_fileformat(None) is None

    # Test glob patterns
    assert utils.resolve_fileformat("*.csv") == "csv"
    assert utils.resolve_fileformat("./out/*.xlsx") == "excel"


def test_generate_unique_string_id():
    assert len(utils.generate_unique_string_id(10)) == 10

    # Test that IDs are actually unique
    id1 = utils.generate_unique_string_id(16)
    id2 = utils.generate_unique_string_id(16)
    assert id1 != id2
    assert len(id1) == 16
    assert len(id2) == 16


def test_is_scalar_type():
    """Test scalar type checking function."""
    # Test scalar types
    assert utils.is_scalar_type(42) is True
    assert utils.is_scalar_type(3.14) is True
    assert utils.is_scalar_type("hello") is True
    assert utils.is_scalar_type(True) is True

    # Test non-scalar types
    assert utils.is_scalar_type([1, 2, 3]) is False
    assert utils.is_scalar_type({"key": "value"}) is False
    assert utils.is_scalar_type(None) is False
    assert utils.is_scalar_type((1, 2, 3)) is False


def test_scalar_types_constant():
    """Test SCALAR_TYPES constant."""
    # Test that it contains the expected types
    assert int in utils.SCALAR_TYPES
    assert float in utils.SCALAR_TYPES
    assert str in utils.SCALAR_TYPES
    assert bool in utils.SCALAR_TYPES

    # Test it's a tuple
    assert isinstance(utils.SCALAR_TYPES, tuple)

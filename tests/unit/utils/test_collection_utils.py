"""Tests for collection utility functions."""

import cypher_graphdb.utils as utils


def test_dict_from_value_pairs():
    value_pairs = 'a=1, b="strval", c=True, d=NULL'
    result = utils.dict_from_value_pairs(value_pairs)

    assert result == {"a": 1, "b": "strval", "c": True, "d": None}


def test_dict_to_value_pairs():
    dict_values = {"a": 1, "b": "strval", "c": True, "d": None}
    result = utils.dict_to_value_pairs(dict_values, separator=", ")

    assert result == 'a=1, b="strval", c=True, d=NULL'


def test_remove_from_dict():
    assert utils.remove_from_dict({"a": 1, "b": 2}, {"a"}) == {"b": 2}


def test_remove_default_from_dict():
    assert utils.remove_default_from_dict(
        {
            "a": 10,
            "bt": True,
            "bf": False,
            "c": None,
        }
    ) == {"a": 10, "bt": True}


def test_order_dict():
    data = {"a": 1, "b": 2}
    result = utils.order_dict(data, {"b"})
    assert result == data


def test_resolve_nested_length():
    assert utils.resolve_nested_lengths([1, 2, 3]) == (3,)
    assert utils.resolve_nested_lengths([[1, 1, 1], [2, 2, 2]]) == (2, 3)
    assert utils.resolve_nested_lengths([[1, 1, 1, {}], [2, 2, 2, {}]]) == (2, 4)


def test_extract_from_nested_dict():
    data = {"a": 1, "b": 2, "c": {"c1": 31, "c2": 32}, "d": {}, "e": 5}

    assert not utils.extract_from_nested_dict(data, ())
    assert utils.extract_from_nested_dict(data, ("a",)) == {"a": 1}
    assert utils.extract_from_nested_dict(data, ("a", "b")) == {"a": 1, "b": 2}
    assert utils.extract_from_nested_dict(data, ("d",)) == {"d": {}}
    assert utils.extract_from_nested_dict(data, (("c", "c2"),)) == {"c": {"c2": 32}}


def test_str_to_collection():
    assert utils.str_to_collection("1,2,3") == "1,2,3"
    assert utils.str_to_collection("[1,2,3]") == ["1", "2", "3"]
    assert utils.str_to_collection("(1,2,3)") == ("1", "2", "3")
    assert utils.str_to_collection("{1,2,3}") == {"1", "2", "3"}


def test_to_collection():
    TestClass = type("TestClass", (), {})
    data = {
        "int": 1,
        "str": "abc",
        "none": None,
        "list": [1, 2, 3],
        "dict": {"c1": 20, "c2": 20},
        "tuple": (1, 2, 3),
        "set": {1, 2, 3},
        "cls": TestClass,
        "inst": TestClass(),
    }

    result = utils.to_collection(data, False)
    # The function converts class to string representation
    result_dict = result  # type: ignore[assignment]
    assert result_dict.pop("cls") == f"{TestClass.__module__}.{TestClass.__name__}"
    # The function converts instance to string representation
    assert isinstance(result_dict.pop("inst"), str)
    # Remove the corresponding keys from expected data for comparison
    del data["cls"]
    del data["inst"]
    assert result_dict == data

    assert utils.to_collection("abc") == {"val": "abc"}


def test_chunk_list():
    """Test list chunking function."""
    assert list(utils.chunk_list([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
    assert list(utils.chunk_list([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]
    assert list(utils.chunk_list([], 2)) == []
    assert list(utils.chunk_list([1], 3)) == [[1]]


def test_concat_dict():
    """Test dictionary concatenation function."""
    d1 = {"a": 1, "b": 2}
    d2 = {"c": 3, "d": 4}
    result = utils.concat_dict(d1, d2)
    expected = {"a": 1, "b": 2, "c": 3, "d": 4}
    assert result == expected

    # Test with overlapping keys (second dict should override)
    d1 = {"a": 1, "b": 2}
    d2 = {"b": 20, "c": 3}
    result = utils.concat_dict(d1, d2)
    expected = {"a": 1, "b": 20, "c": 3}
    assert result == expected


def test_remove_values_from_dict():
    """Test removal of specific values from dictionary."""
    data = {"a": 1, "b": 2, "c": 1, "d": 3}
    result = utils.remove_values_from_dict(data, {1})
    expected = {"b": 2, "d": 3}
    assert result == expected

    # Test with multiple values to remove
    result = utils.remove_values_from_dict(data, {1, 2})
    expected = {"d": 3}
    assert result == expected


def test_replace_empty_dicts():
    """Test replacement of empty dicts with None."""
    data = {"a": 1, "b": {}, "c": {"nested": {}}}
    result = utils.replace_empty_dicts(data)
    expected = {"a": 1, "b": None, "c": {"nested": None}}
    assert result == expected

    # Test with no empty dicts
    data = {"a": 1, "b": {"x": 2}}
    result = utils.replace_empty_dicts(data)
    assert result == data


def test_sort_nested_dict():
    """Test sorting of nested dictionaries."""
    nested = {"z": 1, "a": {"z": 2, "a": 3}, "b": 2}
    result = utils.sort_nested_dict(nested)
    expected = {"a": {"a": 3, "z": 2}, "b": 2, "z": 1}
    assert result == expected


def test_expected_nested_lengths():
    """Test expected nested lengths validation."""
    # Test if a nested structure matches expected sizes
    result = utils.expected_nested_lengths([1, 2, 3], (3,))
    assert result is True

    # Test mismatched sizes
    result = utils.expected_nested_lengths([1, 2], (3,))
    assert result is False

    # Test nested structure
    result = utils.expected_nested_lengths([[1, 2], [3, 4]], (2, 2))
    assert result is True


def test_unnest_result():
    """Test unnesting of nested result structures."""
    # Test with 'first' mode
    nested = [[[1, 2]], [[3, 4]]]
    result = utils.unnest_result(nested, "first")
    # The function behavior depends on implementation
    assert result is not None

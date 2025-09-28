"""Tests for string utility functions."""

from pydantic import BaseModel

import cypher_graphdb.utils as utils


def test_split_str():
    assert utils.split_str("abc", "|") == ["abc"]
    assert utils.split_str("abc|def", "|") == ["abc", "def"]
    assert utils.split_str("abc||def", "||") == ["abc", "def"]
    assert utils.split_str("'abc|def'", "|") == ["'abc|def'"]
    assert utils.split_str('"abc|def"', "|") == ['"abc|def"']


def test_partition_str():
    assert utils.partition_str("abc", ":") == ("abc", "", "")
    assert utils.partition_str("abc:def", ":") == ("abc", ":", "def")
    assert utils.partition_str("abc:def:ghi", ":") == ("abc", ":", "def:ghi")


def test_split_args_kwargs():
    data = "arg1, arg2, arg3, opt1=4711, opt2='val', opt3=['1',2],opt4={'f1':'a','f2':10}"

    args, kwargs = utils.split_into_args_kwargs(data)
    assert args == (
        "arg1",
        "arg2",
        "arg3",
    )
    assert kwargs == {
        "opt1": 4711,
        "opt2": "val",
        "opt3": ["1", 2],
        "opt4": {"f1": "a", "f2": 10},
    }


def test_args_to_dict():
    assert utils.args_to_dict("arg1") == {"arg1": True}
    assert utils.args_to_dict("arg1,arg2") == {"arg1": True, "arg2": True}
    assert utils.args_to_dict(["arg1", "arg2"]) == {"arg1": True, "arg2": True}
    assert utils.args_to_dict("arg1=10") == {"arg1": 10}
    assert utils.args_to_dict("arg1=10, arg2='abc'") == {"arg1": 10, "arg2": "abc"}
    assert utils.args_to_dict(["arg1=10", "arg2='abc'"]) == {"arg1": 10, "arg2": "abc"}


def test_resolve_properties():
    data = {
        "p1": "None",
        "p2": "Null",
        "p3": "['a',1]",
        "p4": "{ 'f1': 'a', 'f2': 10 }",
    }

    result = utils.resolve_properties(data)
    assert result == {
        "p1": None,
        "p2": None,
        "p3": ["a", 1],
        "p4": {"f1": "a", "f2": 10},
    }


def test_slice_model_properties():
    class TestClass(BaseModel):
        f1: str
        f2: str

    data = {"f1": "a", "f2": "b", "f3": "c", "f4": "d"}
    fields, props = utils.slice_model_properties(TestClass, data)

    assert fields == {"f1": "a", "f2": "b"}
    assert props == {"f3": "c", "f4": "d"}


def test_try_literal_eval():
    assert utils.try_literal_eval("'val'") == ("val", False)
    assert utils.try_literal_eval("4711") == (4711, False)
    assert utils.try_literal_eval("varname") == ("varname", True)


def test_string_match():
    assert utils.StringMatch.is_list("[1,2,3]")
    assert utils.StringMatch.is_list("[]")
    assert utils.StringMatch.is_list("abc") is False
    assert utils.StringMatch.is_dict("{}")
    assert utils.StringMatch.is_dict("{'a':4711}")
    assert utils.StringMatch.is_tuple("(1,2,3)")
    assert utils.StringMatch.is_set("{1,2,3}")


def test_resolve_template():
    """Test the resolve_template function with various inputs."""
    # Basic template with single variable
    template = "Hello {name}!"
    result = utils.resolve_template(template, name="World")
    assert result == "Hello World!"

    # Template with multiple variables
    template = "MATCH ({node}:{label}) RETURN {node}"
    result = utils.resolve_template(template, node="n", label="Person")
    assert result == "MATCH (n:Person) RETURN n"

    # Cypher pattern template (like in backend capabilities)
    template = "labels({node})[0]"
    result = utils.resolve_template(template, node="n")
    assert result == "labels(n)[0]"

    # Template with repeated variables
    template = "{var} + {var} = {result}"
    result = utils.resolve_template(template, var="5", result="10")
    assert result == "5 + 5 = 10"

    # Template with no variables
    template = "MATCH (n) RETURN n"
    result = utils.resolve_template(template)
    assert result == "MATCH (n) RETURN n"

    # Empty template
    result = utils.resolve_template("", anything="value")
    assert result == ""


def test_resolve_template_missing_variable():
    """Test that missing variables raise KeyError."""
    import pytest

    template = "Hello {name}!"
    with pytest.raises(KeyError, match="name"):
        utils.resolve_template(template, other="value")


def test_convert_to_str():
    """Test conversion to string function."""
    assert utils.convert_to_str(42) == "42"
    assert utils.convert_to_str("hello") == '"hello"'  # Strings get quoted
    assert utils.convert_to_str([1, 2, 3]) == "[1,2,3]"
    assert utils.convert_to_str({"a": 1}) == "{a: 1}"


def test_dict_to_non_quoted_json():
    """Test dictionary to non-quoted JSON conversion."""
    d = {"a": 1, "b": "hello"}
    result = utils.dict_to_non_quoted_json(d)
    expected = '{a: 1,b: "hello"}'
    assert result == expected

    # Test with empty dict
    assert utils.dict_to_non_quoted_json({}) == "{}"


def test_resolve_to_type():
    """Test string to type resolution."""
    assert utils.resolve_to_type("42") is int
    assert utils.resolve_to_type("3.14") is float
    assert utils.resolve_to_type("True") is bool
    assert utils.resolve_to_type("[1,2,3]") is list
    assert utils.resolve_to_type("{'a': 1}") is dict

    # Test cases that return None
    assert utils.resolve_to_type("hello world") is type(None)
    assert utils.resolve_to_type("42abc") is type(None)


def test_starts_with():
    """Test starts_with function."""
    assert utils.starts_with("hello world", "hello") == 1
    assert utils.starts_with("hello world", "world") == -1
    # Edge case: empty prefix returns -1
    assert utils.starts_with("hello", "") == -1

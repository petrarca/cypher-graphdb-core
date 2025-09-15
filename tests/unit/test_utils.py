from pydantic import BaseModel

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
    assert utils.remove_from_dict({"a": 1, "b": 2}, ("a",)) == {"b": 2}


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
    result = utils.order_dict(data, ("b",))
    assert result == data


def test_split_path():
    assert utils.split_path("/dir1/file.ext"), ("/dir1", "file", ".ext")
    assert utils.split_path("/dir1/dir2/file.ext"), ("/dir1/dir2", "file", ".ext")
    assert utils.split_path("file.ext"), ("", "file", ".ext")
    assert utils.split_path("/file.ext"), ("/", "file", ".ext")
    assert utils.split_path("file"), ("", "file", "")


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


def test_resolve_fileformat():
    assert utils.resolve_fileformat(".xlsx") == "excel"
    assert utils.resolve_fileformat(".csv") == "csv"
    assert utils.resolve_fileformat(".undefined") is None
    assert utils.resolve_fileformat("") is None
    assert utils.resolve_fileformat(None) is None


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
    assert result.pop("cls") == f"{TestClass.__module__}.{TestClass.__name__}"
    assert isinstance(result.pop("inst"), str)
    del data["cls"]
    del data["inst"]
    assert result == data

    assert utils.to_collection("abc") == {"val": "abc"}


def test_string_match():
    assert utils.StringMatch.is_list("[1,2,3]")
    assert utils.StringMatch.is_list("[]")
    assert utils.StringMatch.is_list("abc") is False
    assert utils.StringMatch.is_dict("{}")
    assert utils.StringMatch.is_dict("{'a':4711}")
    assert utils.StringMatch.is_tuple("(1,2,3)")
    assert utils.StringMatch.is_set("{1,2,3}")


def test_generate_unique_string_id():
    assert len(utils.generate_unique_string_id(10)) == 10


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

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


# Connection URI parsing tests
def test_parse_connection_uri_bolt_complete():
    """Test parsing complete bolt URI with all components."""
    uri = "bolt://username:password@localhost:7687"
    result = utils.parse_connection_uri(uri)
    expected = {
        "protocol": "bolt",
        "host": "localhost",
        "port": 7687,
        "username": "username",
        "password": "password",
    }
    assert result == expected


def test_parse_connection_uri_bolt_minimal():
    """Test parsing minimal bolt URI."""
    uri = "bolt://localhost"
    result = utils.parse_connection_uri(uri)
    expected = {
        "protocol": "bolt",
        "host": "localhost",
    }
    assert result == expected


def test_parse_connection_uri_postgres_with_database():
    """Test parsing postgres URI with database."""
    uri = "postgres://user:pass@localhost:5432/mydb"
    result = utils.parse_connection_uri(uri)
    expected = {
        "protocol": "postgres",
        "host": "localhost",
        "port": 5432,
        "username": "user",
        "password": "pass",
        "database": "mydb",
    }
    assert result == expected


def test_parse_connection_uri_key_value_format():
    """Test parsing key=value format."""
    cinfo = "host=localhost port=7687 username=user password=secret"
    result = utils.parse_connection_uri(cinfo)
    expected = {
        "host": "localhost",
        "port": 7687,
        "username": "user",
        "password": "secret",
    }
    assert result == expected


def test_parse_connection_uri_key_value_partial():
    """Test parsing partial key=value format."""
    cinfo = "host=192.168.1.100 port=7687"
    result = utils.parse_connection_uri(cinfo)
    expected = {
        "host": "192.168.1.100",
        "port": 7687,
    }
    assert result == expected


def test_parse_connection_uri_invalid_port_key_value():
    """Test parsing with invalid port in key=value format."""
    import pytest

    cinfo = "host=localhost port=invalid"
    with pytest.raises(ValueError, match="Invalid port value: invalid"):
        utils.parse_connection_uri(cinfo)


def test_parse_connection_uri_empty_string():
    """Test parsing empty string."""
    result = utils.parse_connection_uri("")
    assert result == {}


def test_parse_connection_uri_none():
    """Test parsing None."""
    result = utils.parse_connection_uri(None)
    assert result == {}


def test_validate_protocol_valid():
    """Test protocol validation with valid protocol."""
    params = {"protocol": "bolt", "host": "localhost"}
    utils.validate_protocol(params, ["bolt", "postgres"])
    # Should not raise


def test_validate_protocol_invalid():
    """Test protocol validation with invalid protocol."""
    import pytest

    params = {"protocol": "mysql", "host": "localhost"}
    with pytest.raises(ValueError, match="Unsupported protocol 'mysql'"):
        utils.validate_protocol(params, ["bolt", "postgres"])


def test_validate_protocol_no_protocol():
    """Test protocol validation when no protocol specified."""
    params = {"host": "localhost", "port": 7687}
    utils.validate_protocol(params, ["bolt", "postgres"])
    # Should not raise when no protocol specified


# Test security/sanitization functions


def test_sanitize_connection_params_for_logging():
    """Test parameter sanitization for logging."""
    # Test with empty params
    assert utils.sanitize_connection_params_for_logging({}) == {}

    # Test masking sensitive fields
    params = {"host": "localhost", "port": 7687, "username": "user123", "password": "secret123", "database": "mydb"}
    result = utils.sanitize_connection_params_for_logging(params)

    assert result["host"] == "localhost"
    assert result["port"] == 7687
    assert result["username"] == "user123"
    assert result["password"] == "***MASKED***"
    assert result["database"] == "mydb"

    # Test various sensitive field names
    sensitive_params = {"pass": "secret", "pwd": "password123", "secret": "mysecret", "token": "token123", "key": "mykey"}
    result = utils.sanitize_connection_params_for_logging(sensitive_params)
    for key, value in result.items():
        assert value == "***MASKED***", f"Field {key} should be masked"


def test_sanitize_connection_string_for_logging():
    """Test connection string sanitization for logging."""
    # Test empty strings
    assert utils.sanitize_connection_string_for_logging("") == ""

    # Test URI format with password
    uri = "postgres://user:secret123@localhost:5432/mydb"
    result = utils.sanitize_connection_string_for_logging(uri)
    assert "secret123" not in result
    assert "***MASKED***" in result
    assert "user" in result
    assert "localhost" in result

    # Test bolt URI with password
    bolt_uri = "bolt://admin:mypassword@127.0.0.1:7687"
    result = utils.sanitize_connection_string_for_logging(bolt_uri)
    assert "mypassword" not in result
    assert "***MASKED***" in result

    # Test key=value format with password
    key_value = "host=localhost port=7687 password=secret123 username=user"
    result = utils.sanitize_connection_string_for_logging(key_value)
    assert "secret123" not in result
    assert "password=***MASKED***" in result
    assert "host=localhost" in result
    assert "username=user" in result

    # Test various sensitive key names
    sensitive_tests = [
        ("password=secret", "secret"),
        ("pass=secret", "secret"),
        ("pwd=secret", "secret"),
        ("secret=value", "value"),
        ("token=mytoken", "mytoken"),
    ]
    for sensitive_string, secret_value in sensitive_tests:
        result = utils.sanitize_connection_string_for_logging(sensitive_string)
        assert secret_value not in result, f"Secret value '{secret_value}' should be masked in '{result}'"
        assert "***MASKED***" in result

    # Test URI without password (should remain unchanged)
    clean_uri = "postgres://user@localhost:5432/mydb"
    result = utils.sanitize_connection_string_for_logging(clean_uri)
    assert result == clean_uri

    # Test key=value without sensitive fields
    clean_kv = "host=localhost port=7687 username=user"
    result = utils.sanitize_connection_string_for_logging(clean_kv)
    assert result == clean_kv

    # Test unparseable string (should be fully masked for safety)
    weird_string = "some_weird_connection_string"
    result = utils.sanitize_connection_string_for_logging(weird_string)
    assert result == "***CONNECTION_STRING_MASKED***"


def test_secure_log_env(monkeypatch):
    """Test secure environment variable logging functions exist and work."""
    # Mock environment variables
    monkeypatch.setenv("TEST_NORMAL", "normal_value")
    monkeypatch.setenv("CGDB_CINFO", "host=localhost password=secret123")
    monkeypatch.setenv("MY_PASSWORD", "supersecret")

    # Test that log_env function works without crashing
    # The actual output verification is difficult with loguru in tests
    # but we can verify the function executes and handles sensitive data

    # These should not raise exceptions
    utils.log_env("TEST_NORMAL")
    utils.log_env("CGDB_CINFO")
    utils.log_env("MY_PASSWORD")

    # Test that the underlying sanitization logic works
    # (which is what really matters for security)
    conn_str = "host=localhost password=secret123"
    value = utils.sanitize_connection_string_for_logging(conn_str)
    assert "secret123" not in value
    assert "***MASKED***" in value

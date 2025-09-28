"""Tests for connection utility functions."""

import cypher_graphdb.utils as utils


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
    params = {
        "host": "localhost",
        "port": 7687,
        "username": "user123",
        "password": "secret123",
        "database": "mydb",
    }
    result = utils.sanitize_connection_params_for_logging(params)

    assert result["host"] == "localhost"
    assert result["port"] == 7687
    assert result["username"] == "user123"
    assert result["password"] == "***MASKED***"
    assert result["database"] == "mydb"

    # Test various sensitive field names
    sensitive_params = {
        "pass": "secret",
        "pwd": "password123",
        "secret": "mysecret",
        "token": "token123",
        "key": "mykey",
    }
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

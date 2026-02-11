"""Tests for settings representation utilities."""

import pytest
from pydantic import BaseModel, Field

from cypher_graphdb.utils.settings_repr import safe_settings_repr, safe_settings_str


class MockSettings(BaseModel):
    """Mock settings class for testing."""

    username: str = Field(default="testuser")
    password: str = Field(default="secret123")
    database: str = Field(default="testdb")
    host: str = Field(default="localhost")
    port: int = Field(default=5432)


class TestSafeSettingsRepr:
    """Test suite for safe_settings_repr function."""

    def test_basic_repr_without_sanitizers(self):
        """Test basic repr functionality without field sanitizers."""
        settings = MockSettings()
        result = safe_settings_repr(settings)

        expected = "MockSettings(username='testuser', password='secret123', database='testdb', host='localhost', port=5432)"
        assert result == expected

    def test_repr_with_single_field_sanitizer(self):
        """Test repr with sanitization of a single sensitive field."""
        settings = MockSettings()
        sanitizers = {"password": lambda _: "***MASKED***"}

        result = safe_settings_repr(settings, field_sanitizers=sanitizers)

        expected = "MockSettings(username='testuser', password='***MASKED***', database='testdb', host='localhost', port=5432)"
        assert result == expected

    def test_repr_with_multiple_field_sanitizers(self):
        """Test repr with sanitization of multiple fields."""
        settings = MockSettings()
        sanitizers = {"password": lambda _: "***MASKED***", "username": lambda _: "***USER***"}

        result = safe_settings_repr(settings, field_sanitizers=sanitizers)

        expected = "MockSettings(username='***USER***', password='***MASKED***', database='testdb', host='localhost', port=5432)"
        assert result == expected

    def test_repr_with_custom_sanitizer_logic(self):
        """Test repr with custom sanitizer logic based on field value."""
        settings = MockSettings(password="topsecret", username="admin")

        def mask_if_contains_secret(value):
            return "***REDACTED***" if "secret" in str(value).lower() else value

        sanitizers = {"password": mask_if_contains_secret}

        result = safe_settings_repr(settings, field_sanitizers=sanitizers)

        expected = "MockSettings(username='admin', password='***REDACTED***', database='testdb', host='localhost', port=5432)"
        assert result == expected

    def test_repr_with_nonexistent_field_sanitizer(self):
        """Test that sanitizers for non-existent fields are ignored."""
        settings = MockSettings()
        sanitizers = {"password": lambda _: "***MASKED***", "nonexistent_field": lambda _: "should_be_ignored"}

        result = safe_settings_repr(settings, field_sanitizers=sanitizers)

        expected = "MockSettings(username='testuser', password='***MASKED***', database='testdb', host='localhost', port=5432)"
        assert result == expected

    def test_repr_with_none_values(self):
        """Test repr handling of None field values."""

        class SettingsWithNone(BaseModel):
            required_field: str = "value"
            optional_field: str | None = None

        settings = SettingsWithNone()
        sanitizers = {"optional_field": lambda v: "***SANITIZED***" if v is not None else v}

        result = safe_settings_repr(settings, field_sanitizers=sanitizers)

        expected = "SettingsWithNone(required_field='value', optional_field=None)"
        assert result == expected

    def test_repr_with_empty_sanitizers(self):
        """Test repr with empty sanitizers dict."""
        settings = MockSettings()

        result = safe_settings_repr(settings, field_sanitizers={})

        expected = "MockSettings(username='testuser', password='secret123', database='testdb', host='localhost', port=5432)"
        assert result == expected


class TestSafeSettingsStr:
    """Test suite for safe_settings_str function."""

    def test_basic_str_without_sanitizers(self):
        """Test basic str functionality without field sanitizers."""
        settings = MockSettings()
        result = safe_settings_str(settings)

        expected = "username='testuser' password='secret123' database='testdb' host='localhost' port=5432"
        assert result == expected

    def test_str_with_single_field_sanitizer(self):
        """Test str with sanitization of a single sensitive field."""
        settings = MockSettings()
        sanitizers = {"password": lambda _: "***MASKED***"}

        result = safe_settings_str(settings, field_sanitizers=sanitizers)

        expected = "username='testuser' password='***MASKED***' database='testdb' host='localhost' port=5432"
        assert result == expected

    def test_str_with_multiple_field_sanitizers(self):
        """Test str with sanitization of multiple fields."""
        settings = MockSettings()
        sanitizers = {"password": lambda _: "***MASKED***", "username": lambda _: "***USER***"}

        result = safe_settings_str(settings, field_sanitizers=sanitizers)

        expected = "username='***USER***' password='***MASKED***' database='testdb' host='localhost' port=5432"
        assert result == expected

    def test_str_with_connection_string_sanitizer(self):
        """Test str with realistic connection string sanitization."""

        class ConnectionSettings(BaseModel):
            dsn: str = "postgres://user:secret@localhost:5432/db"
            timeout: int = 30

        settings = ConnectionSettings()

        def sanitize_dsn(value):
            if "://" in str(value) and "@" in str(value):
                # Mock connection string sanitization
                return str(value).replace("secret", "***MASKED***")
            return value

        sanitizers = {"dsn": sanitize_dsn}

        result = safe_settings_str(settings, field_sanitizers=sanitizers)

        expected = "dsn='postgres://user:***MASKED***@localhost:5432/db' timeout=30"
        assert result == expected

    def test_str_field_order_consistency(self):
        """Test that field order is consistent with model field definition order."""
        settings = MockSettings()
        result = safe_settings_str(settings)

        # Fields should appear in definition order
        assert result.startswith("username=")
        assert "password=" in result
        assert result.endswith("port=5432")

    def test_str_with_numeric_and_boolean_fields(self):
        """Test str representation of various field types."""

        class MixedSettings(BaseModel):
            name: str = "test"
            count: int = 42
            enabled: bool = True
            ratio: float = 3.14

        settings = MixedSettings()
        result = safe_settings_str(settings)

        expected = "name='test' count=42 enabled=True ratio=3.14"
        assert result == expected


class TestIntegrationWithActualSettings:
    """Integration tests using the actual Settings class."""

    def test_integration_with_cypher_settings(self, monkeypatch, tmp_path):
        """Test utilities work with actual CypherGraphDB Settings class."""
        from cypher_graphdb.settings import Settings

        # Change to temp directory to avoid loading .env file from project
        monkeypatch.chdir(tmp_path)

        # Set environment variables for the test (BaseSettings prefers env vars)
        monkeypatch.setenv("CGDB_BACKEND", "memgraph")
        monkeypatch.setenv("CGDB_CINFO", "postgres://user:topsecret@localhost:5432/mydb")
        monkeypatch.setenv("CGDB_GRAPH", "test_graph")

        # Create settings instance - will read from env vars we just set
        settings = Settings()

        # Test with sanitizer that matches Settings class behavior
        sanitizers = {"cinfo": lambda _: "postgres://user:***MASKED***@localhost:5432/mydb"}

        repr_result = safe_settings_repr(settings, field_sanitizers=sanitizers)
        str_result = safe_settings_str(settings, field_sanitizers=sanitizers)

        # Verify sensitive info is masked
        assert "topsecret" not in repr_result
        assert "topsecret" not in str_result
        assert "***MASKED***" in repr_result
        assert "***MASKED***" in str_result

        # Verify other fields are preserved (note: Settings uses defaults)
        assert "backend='memgraph'" in repr_result
        assert "backend='memgraph'" in str_result
        # Note: Settings may not show empty/default graph in repr

    def test_sanitizer_function_receives_actual_value(self):
        """Test that sanitizer functions receive the actual field value."""
        original_cinfo = "postgres://user:secret123@localhost:5432/mydb"

        # Need to use environment variable to set cinfo in Settings
        import os

        os.environ["CGDB_CINFO"] = original_cinfo
        from cypher_graphdb.settings import get_settings

        get_settings.cache_clear()
        settings = get_settings()

        captured_value = None

        def capture_and_sanitize(value):
            nonlocal captured_value
            captured_value = value
            return "***SANITIZED***"

        sanitizers = {"cinfo": capture_and_sanitize}

        safe_settings_repr(settings, field_sanitizers=sanitizers)

        # Verify the sanitizer received the original value
        assert captured_value == original_cinfo

        # Clean up
        if "CGDB_CINFO" in os.environ:
            del os.environ["CGDB_CINFO"]

    def test_error_handling_in_sanitizers(self):
        """Test behavior when sanitizer functions raise exceptions."""
        settings = MockSettings()

        def failing_sanitizer(_):
            raise ValueError("Sanitizer error")

        sanitizers = {"password": failing_sanitizer}

        # Should not crash, should fall back to original value or handle gracefully
        with pytest.raises(ValueError, match="Sanitizer error"):
            safe_settings_repr(settings, field_sanitizers=sanitizers)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_model(self):
        """Test with model that has no fields."""

        class EmptyModel(BaseModel):
            pass

        model = EmptyModel()

        assert safe_settings_repr(model) == "EmptyModel()"
        assert safe_settings_str(model) == ""

    def test_single_field_model(self):
        """Test with model that has only one field."""

        class SingleFieldModel(BaseModel):
            only_field: str = "value"

        model = SingleFieldModel()
        sanitizers = {"only_field": lambda _: "***MASKED***"}

        assert safe_settings_repr(model, field_sanitizers=sanitizers) == "SingleFieldModel(only_field='***MASKED***')"
        assert safe_settings_str(model, field_sanitizers=sanitizers) == "only_field='***MASKED***'"

    def test_field_with_quotes_in_value(self):
        """Test handling of field values that contain quotes."""

        class QuotedModel(BaseModel):
            quoted_field: str = "value with 'single' and \"double\" quotes"

        model = QuotedModel()
        result = safe_settings_str(model)

        # Should properly escape or handle quotes
        assert "quoted_field=" in result
        assert "single" in result
        assert "double" in result

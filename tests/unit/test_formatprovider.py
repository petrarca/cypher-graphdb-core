"""Tests for the data-format registry."""

import pytest

from cypher_graphdb.formatprovider import FormatProvider, FormatSpec, data_format
from cypher_graphdb.formatprovider import format_provider as global_provider


@pytest.fixture
def provider() -> FormatProvider:
    """An isolated provider, so tests never mutate the global registry."""
    return FormatProvider()


class _Importer:
    def __init__(self, db=None):
        self.db = db


class _Exporter:
    def __init__(self, db=None, options=None):
        self.db = db
        self.options = options


# ── Registration ──────────────────────────────────────────────────────────────


def test_register_returns_a_spec(provider):
    spec = provider.register("parquet", extensions=(".parquet",), importer=_Importer)
    assert isinstance(spec, FormatSpec)
    assert spec.name == "parquet"
    assert spec.extensions == (".parquet",)
    assert spec.supports_import()
    assert not spec.supports_export()


def test_names_are_case_insensitive(provider):
    provider.register("Parquet", importer=_Importer)
    assert provider.resolve("PARQUET").name == "parquet"
    assert "parquet" in provider


def test_extensions_are_normalised(provider):
    """A missing dot and upper case are both tolerated."""
    provider.register("parquet", extensions=("PARQUET", ".Snappy"), importer=_Importer)
    assert provider.extensions()[".parquet"] == "parquet"
    assert provider.extensions()[".snappy"] == "parquet"


def test_a_format_must_do_something(provider):
    with pytest.raises(ValueError, match="importer, an exporter, or both"):
        provider.register("useless")


def test_empty_name_is_rejected(provider):
    with pytest.raises(ValueError, match="must not be empty"):
        provider.register("   ", importer=_Importer)


def test_re_registering_replaces_and_drops_stale_extensions(provider):
    """A consumer may override a format; its old extensions must not linger."""
    provider.register("tabular", extensions=(".v1",), importer=_Importer)
    provider.register("tabular", extensions=(".v2",), importer=_Importer)

    assert ".v1" not in provider.extensions()
    assert provider.extensions()[".v2"] == "tabular"


# ── Resolution ────────────────────────────────────────────────────────────────


def test_resolve_unknown_format_lists_what_is_registered(provider):
    provider.register("csv", importer=_Importer)
    with pytest.raises(ValueError, match="registered: csv"):
        provider.resolve("nope")


def test_get_does_not_raise_for_unknown(provider):
    assert provider.get("nope") is None


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("data.parquet", "parquet"),
        ("./out/data.parquet", "parquet"),
        ("*.parquet", "parquet"),
        ("DATA.PARQUET", "parquet"),
        ("notes.txt", None),
        ("noextension", None),
        ("", None),
    ],
)
def test_format_name_for_file(provider, filename, expected):
    provider.register("parquet", extensions=(".parquet",), importer=_Importer)
    assert provider.format_name_for_file(filename) == expected


def test_resolve_for_file_returns_the_spec(provider):
    provider.register("parquet", extensions=(".parquet",), importer=_Importer)
    assert provider.resolve_for_file("x.parquet").name == "parquet"
    assert provider.resolve_for_file("x.txt") is None


# ── The decorator ─────────────────────────────────────────────────────────────


def test_decorator_registers_on_the_given_provider(provider):
    @data_format("parquet", extensions=(".parquet",), extra="parquet", provider=provider)
    class ParquetFormat:
        importer = _Importer
        exporter = _Exporter

    spec = provider.resolve("parquet")
    assert spec.importer is _Importer
    assert spec.exporter is _Exporter
    assert spec.extra == "parquet"
    assert ParquetFormat.importer is _Importer  # decorator returns the class


# ── Collection protocol ───────────────────────────────────────────────────────


def test_collection_protocol(provider):
    provider.register("a", importer=_Importer)
    provider.register("b", exporter=_Exporter)

    assert len(provider) == 2
    assert "a" in provider
    assert "missing" not in provider
    assert 42 not in provider  # non-str must not blow up
    assert sorted(name for name, _ in provider) == ["a", "b"]


# ── The built-in formats, on the global provider ──────────────────────────────


def test_builtin_formats_are_discoverable():
    """Built-ins load lazily by name, without being imported up front."""
    for name in ("csv", "json", "yaml"):
        spec = global_provider.resolve(name)
        assert spec.supports_import()
        assert spec.supports_export()


def test_builtin_extensions_resolve():
    assert global_provider.format_name_for_file("a.csv") == "csv"
    assert global_provider.format_name_for_file("a.json") == "json"
    assert global_provider.format_name_for_file("a.yml") == "yaml"
    assert global_provider.format_name_for_file("a.yaml") == "yaml"


def test_excel_declares_its_extra():
    """Excel is the one built-in behind an optional dependency."""
    spec = global_provider.resolve("excel")
    assert spec.extra == "excel"
    assert global_provider.format_name_for_file("a.xlsx") == "excel"
    assert global_provider.format_name_for_file("a.xls") == "excel"


def test_load_builtins_registers_all_of_them():
    global_provider.load_builtins()
    assert {"csv", "excel", "json", "yaml"} <= set(global_provider.items())


# ── Probing (the non-throwing counterpart to resolve) ─────────────────────────


def test_probe_reports_an_available_format(provider):
    provider.register("parquet", importer=_Importer)
    result = provider.probe("parquet")

    assert result.available
    assert result.registered
    assert result.reason is None
    assert bool(result) is True


def test_probe_reports_an_unknown_format_without_raising(provider):
    provider.register("csv", importer=_Importer)
    result = provider.probe("nope")

    assert not result.available
    assert not result.registered  # unknown name != missing dependency
    assert "Unknown format" in result.reason
    assert bool(result) is False


def test_probe_distinguishes_unknown_from_unavailable():
    """A missing extra means registered-but-unusable, not unknown."""
    excel = global_provider.probe("excel")
    unknown = global_provider.probe("definitely-not-a-format")

    assert excel.registered
    assert not unknown.registered


def test_probe_all_covers_the_builtins():
    results = global_provider.probe_all()
    assert {"csv", "excel", "json", "yaml"} <= set(results)
    assert results["csv"].available
    assert results["json"].available


def test_probe_is_serializable(provider):
    """House standard: these are pydantic models, so they round-trip to JSON."""
    provider.register("parquet", importer=_Importer)
    dumped = provider.probe("parquet").model_dump()

    assert dumped["name"] == "parquet"
    assert dumped["available"] is True
    assert set(dumped) == {"name", "available", "registered", "reason", "extra", "missing_package"}


def test_spec_serializes_classes_by_name(provider):
    provider.register("parquet", extensions=(".parquet",), importer=_Importer, exporter=_Exporter)
    dumped = provider.resolve("parquet").model_dump()

    assert dumped == {
        "name": "parquet",
        "extensions": [".parquet"],
        "importer": "_Importer",
        "exporter": "_Exporter",
        "extra": None,
    }


def test_extension_lookup_never_imports_anything():
    """Naming a file's format must work even when its extra is absent."""
    assert global_provider.format_name_for_file("report.xlsx") == "excel"
    assert global_provider.format_name_for_file("report.xls") == "excel"

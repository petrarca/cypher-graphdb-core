"""Unit tests for composite endpoint-key helpers (cypher_graphdb.bulk_refs)."""

import pytest

from cypher_graphdb.bulk_refs import as_ref_props, ref_key

pytestmark = pytest.mark.unit


class TestAsRefProps:
    """Normalising the ref-prop argument to an ordered tuple of names."""

    def test_single_string(self):
        assert as_ref_props("symbol") == ("symbol",)

    def test_sequence(self):
        assert as_ref_props(["qualified_name", "source_key"]) == ("qualified_name", "source_key")

    def test_tuple_preserved_in_order(self):
        assert as_ref_props(("a", "b", "c")) == ("a", "b", "c")

    def test_empty_sequence_rejected(self):
        with pytest.raises(ValueError, match="at least one property"):
            as_ref_props([])

    def test_non_string_elements_rejected(self):
        with pytest.raises(TypeError, match="property-name strings"):
            as_ref_props(["ok", 123])  # type: ignore[list-item]


class TestRefKey:
    """Turning an edge's src/dst value into a key tuple aligned with the props."""

    def test_scalar_single_prop(self):
        assert ref_key("mth:foo.bar", 1) == ("mth:foo.bar",)

    def test_one_element_sequence_single_prop(self):
        assert ref_key(["mth:foo.bar"], 1) == ("mth:foo.bar",)

    def test_wrong_length_single_prop(self):
        with pytest.raises(ValueError, match="expected 1 match value"):
            ref_key(["a", "b"], 1)

    def test_composite(self):
        assert ref_key(("pkg.foo", "srcA"), 2) == ("pkg.foo", "srcA")

    def test_composite_from_list(self):
        assert ref_key(["pkg.foo", "srcA"], 2) == ("pkg.foo", "srcA")

    def test_composite_scalar_rejected(self):
        with pytest.raises(TypeError, match="needs a sequence"):
            ref_key("pkg.foo", 2)

    def test_composite_wrong_length(self):
        with pytest.raises(ValueError, match="expected 2 match values"):
            ref_key(("pkg.foo",), 2)

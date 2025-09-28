"""Tests for conversion utility functions."""

import numpy as np

import cypher_graphdb.utils as utils


def test_type_to_default_value():
    """Test type to default value conversion."""
    assert utils.type_to_default_value(str) == ""
    assert utils.type_to_default_value(int) == 0
    assert utils.type_to_default_value(float) is None
    assert utils.type_to_default_value(bool) is False
    assert utils.type_to_default_value(list) == []
    assert utils.type_to_default_value(dict) == {}
    assert utils.type_to_default_value(set) == set()
    assert utils.type_to_default_value(tuple) == ()


def test_isnan():
    """Test NaN checking function."""
    # Test with actual NaN
    assert utils.isnan(np.nan)
    assert utils.isnan(float("nan"))

    # Test with regular numbers
    assert not utils.isnan(42.0)
    assert not utils.isnan(42)
    assert not utils.isnan(0.0)

    # Test with non-numeric types
    assert not utils.isnan("not a number")
    assert not utils.isnan(None)
    assert not utils.isnan([])
    assert not utils.isnan({})

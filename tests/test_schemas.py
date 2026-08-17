"""Schema contracts are the single source of truth; their invariants must hold."""

from __future__ import annotations

import pytest

from pipeline.schemas import DTYPE_FAMILY, MACRO_DATA, MARKET_PRICES, get_schema


def test_canonical_schemas_have_unique_columns():
    for schema in (MARKET_PRICES, MACRO_DATA):
        names = [c.name for c in schema.columns]
        assert len(names) == len(set(names)), f"duplicate columns in {schema.name}"


def test_primary_keys_are_declared_columns():
    for schema in (MARKET_PRICES, MACRO_DATA):
        for key in schema.primary_key:
            assert key in schema.column_names


def test_dtype_family_maps_all_declared_dtypes():
    for declared in ("date", "timestamp", "string", "float", "int"):
        assert declared in DTYPE_FAMILY


def test_get_schema_known_and_unknown():
    assert get_schema("market_prices") is MARKET_PRICES
    with pytest.raises(KeyError):
        get_schema("does_not_exist")

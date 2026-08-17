"""Unit normalization.

The single most common way a public financial feed produces a wrong number downstream is
a unit convention nobody wrote down: yields in tenths of a percent, rates as decimals
instead of percent, CPI as an index rather than a change. Conversions therefore live in
one place, are applied exactly once, and the resulting unit is stored next to the value
so that a query can never silently mix conventions.
"""

from __future__ import annotations

import pandas as pd

KNOWN_UNITS = frozenset({"percent", "index", "ratio", "price", "shares"})

# Source-side conventions and the factor that brings them to the canonical unit.
SOURCE_UNIT_TO_CANONICAL: dict[str, tuple[str, float]] = {
    "percent": ("percent", 1.0),
    "tenths_of_percent": ("percent", 0.1),
    "basis_points": ("percent", 0.01),
    "decimal_rate": ("percent", 100.0),
    "index": ("index", 1.0),
    "ratio": ("ratio", 1.0),
}


class UnknownUnitError(ValueError):
    pass


def canonical_unit(source_unit: str) -> tuple[str, float]:
    """Map a source-side unit to (canonical unit, multiplicative factor)."""
    try:
        return SOURCE_UNIT_TO_CANONICAL[source_unit]
    except KeyError as exc:
        raise UnknownUnitError(
            f"unknown source unit {source_unit!r}; declare it in SOURCE_UNIT_TO_CANONICAL"
        ) from exc


def convert(values: pd.Series, source_unit: str, extra_scale: float = 1.0) -> tuple[pd.Series, str]:
    """Convert a value column to its canonical unit.

    ``extra_scale`` covers per-series adjustments that are not a property of the unit
    itself; it is normally 1.0.
    """
    unit, factor = canonical_unit(source_unit)
    return values * factor * extra_scale, unit


def assert_known_unit(unit: str) -> None:
    if unit not in KNOWN_UNITS:
        raise UnknownUnitError(f"{unit!r} is not a declared canonical unit")

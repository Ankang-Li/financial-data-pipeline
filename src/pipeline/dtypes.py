"""Dtype families and coercion helpers.

Financial data arrives as text far more often than people expect ("." for a missing
FRED observation, thousands separators, ISO strings, Excel serials). The pipeline
therefore keeps raw payloads as text and coerces types exactly once, in the
normalization layer, where the intent is explicit and auditable.

Type *checking* is done by family rather than by exact pandas dtype string, because
pandas 2.x and 3.x disagree on default datetime resolution and string storage.
"""

from __future__ import annotations

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)

from pipeline.schemas import DTYPE_FAMILY, DType

# Families that satisfy a declared dtype. Ints are accepted where floats are declared
# (a whole-number volume column is not a schema violation).
_COMPATIBLE: dict[str, tuple[str, ...]] = {
    "datetime": ("datetime",),
    "datetime_tz": ("datetime", "datetime_tz"),
    "string": ("string",),
    "float": ("float", "int"),
    "int": ("int",),
}


def dtype_family(series: pd.Series) -> str:
    """Classify a pandas Series into a coarse family name."""
    if is_datetime64_any_dtype(series):
        return "datetime_tz" if getattr(series.dtype, "tz", None) is not None else "datetime"
    if is_bool_dtype(series):
        return "bool"
    if is_integer_dtype(series):
        return "int"
    if is_float_dtype(series):
        return "float"
    if is_string_dtype(series) or series.dtype == object:
        return "string"
    return str(series.dtype)


def family_matches(series: pd.Series, declared: DType) -> bool:
    """Whether a Series satisfies a declared schema dtype."""
    expected = DTYPE_FAMILY[declared]
    return dtype_family(series) in _COMPATIBLE.get(expected, (expected,))


def coerce_numeric(series: pd.Series, *, missing_tokens: tuple[str, ...] = (".", "", "NA", "-")):
    """Text to float, treating source-specific missing markers as NaN.

    FRED encodes missing observations as ".", AKShare sometimes returns an empty
    string. Silent `errors="coerce"` on the whole column would hide genuine parse
    problems, so tokens we know about are mapped to NaN first and anything else that
    still fails to parse is reported by validation as a missing value.
    """
    if is_float_dtype(series) or is_integer_dtype(series):
        return series.astype("float64")
    cleaned = series.astype("string").str.strip().str.replace(",", "", regex=False)
    cleaned = cleaned.replace(list(missing_tokens), pd.NA)
    return pd.to_numeric(cleaned, errors="coerce").astype("float64")


def coerce_date(series: pd.Series) -> pd.Series:
    """Text/dates to naive calendar dates (no time component, no timezone).

    Market and macro observations are calendar facts, not instants: storing them as
    naive dates avoids the classic bug where a UTC conversion shifts an Asian trading
    day by one. Instants (ingestion timestamps) are stored separately, in UTC.
    """
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    if getattr(parsed.dtype, "tz", None) is not None:
        parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
    return parsed.dt.normalize()


def coerce_string(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()

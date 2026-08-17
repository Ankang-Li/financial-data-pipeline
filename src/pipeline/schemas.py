"""Canonical schemas, raw source schemas and record-level contracts.

Three ideas live here:

1. ``TableSchema`` — the canonical, versioned shape of a research table. It is the
   single source of truth for validation, normalization and the DuckDB DDL, so the
   warehouse cannot silently drift away from the declared contract.
2. ``RawSchema`` — what each source is *expected* to return. Comparing the actual
   payload against this is how source schema drift is detected at ingestion time
   instead of three layers downstream.
3. Pydantic record models — a strict per-row contract applied at the boundary
   between normalization and storage.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

DType = Literal["date", "string", "float", "int", "timestamp"]
ColumnRole = Literal["key", "value", "provenance"]

# Canonical schema versions. Bump these when the meaning or shape of a table
# changes; the version is written into dataset_metadata with every load so old
# extracts stay interpretable.
MARKET_PRICES_SCHEMA_VERSION = "1.0.0"
MACRO_DATA_SCHEMA_VERSION = "1.0.0"

# Declared dtypes are checked by *family* (datetime / string / float / int) rather than by
# exact pandas dtype string: pandas 2.x defaults to datetime64[ns] while pandas 3.x defaults
# to datetime64[us], and both are acceptable here. See validation.schema_checks.
DTYPE_FAMILY: Mapping[DType, str] = {
    "date": "datetime",
    "timestamp": "datetime_tz",
    "string": "string",
    "float": "float",
    "int": "int",
}

DUCKDB_TYPES: Mapping[DType, str] = {
    "date": "DATE",
    "timestamp": "TIMESTAMPTZ",
    "string": "VARCHAR",
    "float": "DOUBLE",
    "int": "BIGINT",
}


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: DType
    role: ColumnRole = "value"
    required: bool = True
    nullable: bool = False
    unit: str | None = None
    description: str = ""


@dataclass(frozen=True)
class TableSchema:
    """Versioned contract for one research table."""

    name: str
    version: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    description: str = ""
    expected_frequency: str | None = None

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate column names in schema {self.name}")
        missing_pk = [c for c in self.primary_key if c not in names]
        if missing_pk:
            raise ValueError(f"primary key columns not declared in {self.name}: {missing_pk}")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if c.required)

    @property
    def non_nullable_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if not c.nullable)

    def columns_by_role(self, role: ColumnRole) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if c.role == role)

    @property
    def record_columns(self) -> tuple[str, ...]:
        """Business columns, i.e. everything except pipeline provenance."""
        return tuple(c.name for c in self.columns if c.role != "provenance")

    def spec(self, name: str) -> ColumnSpec:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{name} is not a column of {self.name}")

    def dtype_map(self) -> dict[str, DType]:
        return {c.name: c.dtype for c in self.columns}


MARKET_PRICES = TableSchema(
    name="market_prices",
    version=MARKET_PRICES_SCHEMA_VERSION,
    description="Daily OHLCV bars for exchange-traded instruments, one row per date/ticker/source.",
    expected_frequency="B",
    primary_key=("date", "ticker", "source"),
    columns=(
        ColumnSpec("date", "date", role="key", description="Trading date in the exchange calendar"),
        ColumnSpec("ticker", "string", role="key", description="Canonical instrument identifier"),
        ColumnSpec("source", "string", role="key", description="Data source identifier"),
        ColumnSpec("open", "float", unit="price", description="Opening price"),
        ColumnSpec("high", "float", unit="price", description="Session high"),
        ColumnSpec("low", "float", unit="price", description="Session low"),
        ColumnSpec("close", "float", unit="price", description="Closing price"),
        ColumnSpec(
            "adj_close",
            "float",
            unit="price",
            nullable=True,
            description="Split/dividend adjusted close, used for return calculations",
        ),
        ColumnSpec("volume", "float", unit="shares", nullable=True, description="Traded volume"),
        ColumnSpec("currency", "string", description="ISO 4217 currency of the quoted prices"),
        ColumnSpec(
            "snapshot_id",
            "string",
            role="provenance",
            description="Raw snapshot this row was derived from",
        ),
        ColumnSpec(
            "run_id", "string", role="provenance", description="Pipeline run that produced the row"
        ),
        ColumnSpec(
            "ingested_at",
            "timestamp",
            role="provenance",
            description="UTC timestamp of the warehouse load",
        ),
    ),
)

MACRO_DATA = TableSchema(
    name="macro_data",
    version=MACRO_DATA_SCHEMA_VERSION,
    description="Long-format macro and rates observations, one row per date/indicator/source.",
    expected_frequency=None,  # per-indicator, see normalization.identifiers
    primary_key=("date", "indicator", "source"),
    columns=(
        ColumnSpec("date", "date", role="key", description="Observation date"),
        ColumnSpec("indicator", "string", role="key", description="Canonical indicator identifier"),
        ColumnSpec("source", "string", role="key", description="Data source identifier"),
        ColumnSpec("value", "float", nullable=True, description="Observed value in `unit`"),
        ColumnSpec(
            "unit",
            "string",
            description="Unit after normalization, e.g. percent, index, ratio",
        ),
        ColumnSpec(
            "frequency", "string", description="Native frequency of the series: D, B, M or Q"
        ),
        ColumnSpec("snapshot_id", "string", role="provenance", description="Source raw snapshot"),
        ColumnSpec("run_id", "string", role="provenance", description="Pipeline run identifier"),
        ColumnSpec(
            "ingested_at", "timestamp", role="provenance", description="UTC load timestamp"
        ),
    ),
)

CANONICAL_SCHEMAS: Mapping[str, TableSchema] = {
    MARKET_PRICES.name: MARKET_PRICES,
    MACRO_DATA.name: MACRO_DATA,
}


def get_schema(name: str) -> TableSchema:
    try:
        return CANONICAL_SCHEMAS[name]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(f"unknown dataset {name!r}; known: {sorted(CANONICAL_SCHEMAS)}") from exc


# --------------------------------------------------------------------------------------
# Raw source schemas: what we expect a source to hand us, used for drift detection.
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RawSchema:
    """Expected shape of a raw payload for one (source, dataset) pair."""

    source: str
    dataset: str
    version: str
    columns: Mapping[str, DType]
    required: tuple[str, ...]

    @property
    def key(self) -> str:
        return f"{self.source}:{self.dataset}"


YAHOO_MARKET_RAW = RawSchema(
    source="yahoo",
    dataset="market_prices",
    version="1.0.0",
    columns={
        "date": "date",
        "ticker": "string",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "adj_close": "float",
        "volume": "float",
    },
    required=("date", "ticker", "open", "high", "low", "close", "volume"),
)

# Yahoo also serves yield indices such as ^TNX. The payload shape is OHLCV, but the
# semantics are a rate quoted in tenths of a percent, so it is ingested as macro data
# and rescaled by the normalizer.
YAHOO_YIELD_RAW = RawSchema(
    source="yahoo",
    dataset="macro_data",
    version="1.0.0",
    columns={
        "date": "date",
        "ticker": "string",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "adj_close": "float",
        "volume": "float",
    },
    required=("date", "ticker", "close"),
)

FRED_MACRO_RAW = RawSchema(
    source="fred",
    dataset="macro_data",
    version="1.0.0",
    columns={"date": "date", "series_id": "string", "value": "string"},
    required=("date", "series_id", "value"),
)

AKSHARE_MACRO_RAW = RawSchema(
    source="akshare",
    dataset="macro_data",
    version="1.0.0",
    # AKShare's bond_zh_us_rate returns Chinese column labels; the adapter keeps them
    # verbatim in the raw layer and only the normalizer renames them.
    columns={
        "日期": "date",
        "中国国债收益率2年": "float",
        "中国国债收益率5年": "float",
        "中国国债收益率10年": "float",
        "中国国债收益率30年": "float",
        "美国国债收益率10年": "float",
    },
    required=("日期", "中国国债收益率10年"),
)

RAW_SCHEMAS: Mapping[str, RawSchema] = {
    schema.key: schema
    for schema in (YAHOO_MARKET_RAW, YAHOO_YIELD_RAW, FRED_MACRO_RAW, AKSHARE_MACRO_RAW)
}


def get_raw_schema(source: str, dataset: str) -> RawSchema | None:
    return RAW_SCHEMAS.get(f"{source}:{dataset}")


# --------------------------------------------------------------------------------------
# Record-level contracts, enforced between normalization and storage.
# --------------------------------------------------------------------------------------


class MarketPriceRecord(BaseModel):
    """Strict contract for a single normalized OHLCV row."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    date: dt.date
    ticker: str = Field(min_length=1, max_length=32)
    source: str = Field(min_length=1, max_length=32)
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    adj_close: float | None = Field(default=None, gt=0)
    volume: float | None = Field(default=None, ge=0)
    currency: str = Field(min_length=3, max_length=3)


class MacroObservationRecord(BaseModel):
    """Strict contract for a single normalized macro observation."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    date: dt.date
    indicator: str = Field(min_length=1, max_length=64)
    source: str = Field(min_length=1, max_length=32)
    value: float | None = None
    unit: str = Field(min_length=1, max_length=32)
    frequency: Literal["D", "B", "W", "M", "Q"]


RECORD_MODELS: Mapping[str, type[BaseModel]] = {
    MARKET_PRICES.name: MarketPriceRecord,
    MACRO_DATA.name: MacroObservationRecord,
}

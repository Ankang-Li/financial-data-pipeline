"""Canonical identifiers.

Two registries, and one rule: nothing enters the research layer under an identifier the
project has not declared. An unmapped symbol is dropped and counted, never silently
passed through with whatever name the source happened to use. That is what makes
``load_dataset(dataset="macro_data", indicators=["US_TREASURY_10Y"])`` mean the same
thing regardless of which source produced the row.
"""

from __future__ import annotations

from dataclasses import dataclass

# --------------------------------------------------------------------------- instruments


@dataclass(frozen=True)
class Instrument:
    ticker: str
    currency: str
    asset_class: str
    description: str


INSTRUMENTS: dict[str, Instrument] = {
    "SPY": Instrument("SPY", "USD", "equity_etf", "SPDR S&P 500 ETF Trust"),
    "TLT": Instrument("TLT", "USD", "bond_etf", "iShares 20+ Year Treasury Bond ETF"),
    "GLD": Instrument("GLD", "USD", "commodity_etf", "SPDR Gold Shares"),
}

# Source symbol -> canonical ticker. Identical today, but the indirection is the point:
# the day a source starts using "SPY.US" only this table changes.
INSTRUMENT_SYMBOL_MAP: dict[tuple[str, str], str] = {
    ("yahoo", "SPY"): "SPY",
    ("yahoo", "TLT"): "TLT",
    ("yahoo", "GLD"): "GLD",
}


def canonical_ticker(source: str, symbol: str) -> str | None:
    return INSTRUMENT_SYMBOL_MAP.get((source, str(symbol).strip()))


def instrument(ticker: str) -> Instrument | None:
    return INSTRUMENTS.get(ticker)


# ---------------------------------------------------------------------------- indicators


@dataclass(frozen=True)
class Indicator:
    name: str
    unit: str
    frequency: str
    description: str


INDICATORS: dict[str, Indicator] = {
    "US_TREASURY_10Y": Indicator(
        "US_TREASURY_10Y", "percent", "B", "US 10-year Treasury constant maturity yield"
    ),
    "US_TREASURY_2Y": Indicator(
        "US_TREASURY_2Y", "percent", "B", "US 2-year Treasury constant maturity yield"
    ),
    "US_CPI_LEVEL": Indicator(
        "US_CPI_LEVEL", "index", "M", "US CPI for all urban consumers, index level"
    ),
    "US_VIX": Indicator("US_VIX", "index", "B", "CBOE volatility index"),
    "CN_TREASURY_2Y": Indicator(
        "CN_TREASURY_2Y", "percent", "B", "China government bond 2-year yield"
    ),
    "CN_TREASURY_5Y": Indicator(
        "CN_TREASURY_5Y", "percent", "B", "China government bond 5-year yield"
    ),
    "CN_TREASURY_10Y": Indicator(
        "CN_TREASURY_10Y", "percent", "B", "China government bond 10-year yield"
    ),
    "CN_TREASURY_30Y": Indicator(
        "CN_TREASURY_30Y", "percent", "B", "China government bond 30-year yield"
    ),
}


@dataclass(frozen=True)
class SeriesMapping:
    """How one source series becomes one canonical indicator.

    ``source_unit`` carries the whole unit conversion (see ``normalization.units``);
    ``scale`` exists only for per-series adjustments that are not a property of the unit,
    and is 1.0 for every series currently mapped. Keeping the two separate prevents the
    classic double-rescaling bug.
    """

    indicator: str
    scale: float = 1.0
    source_unit: str = "percent"


# (source, source symbol or column label) -> mapping
SERIES_MAP: dict[tuple[str, str], SeriesMapping] = {
    # FRED publishes yields in percent and CPI/VIX as index levels.
    ("fred", "DGS10"): SeriesMapping("US_TREASURY_10Y", 1.0, "percent"),
    ("fred", "DGS2"): SeriesMapping("US_TREASURY_2Y", 1.0, "percent"),
    ("fred", "CPIAUCSL"): SeriesMapping("US_CPI_LEVEL", 1.0, "index"),
    ("fred", "VIXCLS"): SeriesMapping("US_VIX", 1.0, "index"),
    # Yahoo's ^TNX is quoted in tenths of a percent: 42.5 means 4.25%. The conversion is
    # expressed as a unit, not as a magic multiplier.
    ("yahoo", "^TNX"): SeriesMapping("US_TREASURY_10Y", 1.0, "tenths_of_percent"),
    # AKShare keeps Chinese column labels; the label *is* the series identifier.
    ("akshare", "中国国债收益率2年"): SeriesMapping("CN_TREASURY_2Y", 1.0, "percent"),
    ("akshare", "中国国债收益率5年"): SeriesMapping("CN_TREASURY_5Y", 1.0, "percent"),
    ("akshare", "中国国债收益率10年"): SeriesMapping("CN_TREASURY_10Y", 1.0, "percent"),
    ("akshare", "中国国债收益率30年"): SeriesMapping("CN_TREASURY_30Y", 1.0, "percent"),
    ("akshare", "美国国债收益率10年"): SeriesMapping("US_TREASURY_10Y", 1.0, "percent"),
}


def series_mapping(source: str, symbol: str) -> SeriesMapping | None:
    return SERIES_MAP.get((source, str(symbol).strip()))


def indicator(name: str) -> Indicator | None:
    return INDICATORS.get(name)


def multi_source_indicators() -> dict[str, list[str]]:
    """Canonical indicators observed by more than one source, i.e. cross-checkable."""
    coverage: dict[str, list[str]] = {}
    for (source, _), mapping in SERIES_MAP.items():
        coverage.setdefault(mapping.indicator, [])
        if source not in coverage[mapping.indicator]:
            coverage[mapping.indicator].append(source)
    return {k: sorted(v) for k, v in coverage.items() if len(v) > 1}

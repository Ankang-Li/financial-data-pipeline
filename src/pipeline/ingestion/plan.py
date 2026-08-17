"""The default ingestion plan.

Kept small on purpose. Three sources are enough to make the interesting problems
appear — heterogeneous column names, different missing-value conventions, different
units, and one economic quantity (the US 10-year Treasury yield) that is available
from all three, which is what makes a genuine cross-source consistency check possible.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

DEFAULT_START = dt.date(2019, 1, 1)


@dataclass(frozen=True)
class IngestionSpec:
    source: str
    dataset: str
    symbols: tuple[str, ...]
    description: str


DEFAULT_PLAN: tuple[IngestionSpec, ...] = (
    IngestionSpec(
        source="yahoo",
        dataset="market_prices",
        symbols=("SPY", "TLT", "GLD"),
        description="US equity, long Treasury and gold ETFs — daily OHLCV",
    ),
    IngestionSpec(
        source="yahoo",
        dataset="macro_data",
        symbols=("^TNX",),
        description="CBOE 10-year Treasury yield index, quoted in tenths of a percent",
    ),
    IngestionSpec(
        source="fred",
        dataset="macro_data",
        symbols=("DGS10", "DGS2", "CPIAUCSL", "VIXCLS"),
        description="US 10y/2y Treasury yields, CPI level and VIX from FRED",
    ),
    IngestionSpec(
        source="akshare",
        dataset="macro_data",
        symbols=("CN2Y", "CN5Y", "CN10Y", "CN30Y", "US10Y"),
        description="China government bond yield curve plus the US 10y as published in China",
    ),
)


def plan_for(sources: tuple[str, ...] | None = None) -> tuple[IngestionSpec, ...]:
    """Filter the default plan by source name."""
    if not sources:
        return DEFAULT_PLAN
    wanted = {s.lower() for s in sources}
    return tuple(spec for spec in DEFAULT_PLAN if spec.source in wanted)

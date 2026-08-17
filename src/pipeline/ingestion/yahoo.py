"""Yahoo Finance adapter (live mode only; requires the optional ``yfinance`` extra).

Two datasets are served from the same client:

* ``market_prices`` — ETF OHLCV bars.
* ``macro_data``    — yield indices such as ``^TNX``, whose payload looks like OHLCV
  but whose ``close`` is a rate in tenths of a percent. The rescaling is the
  normalizer's job, not this adapter's.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from pipeline.ingestion.base import Availability, FetchRequest, FetchResult, SourceAdapter
from pipeline.logging_utils import get_logger

logger = get_logger(__name__)

_COLUMN_MAP = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}
RAW_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]


class _YahooBase(SourceAdapter):
    name = "yahoo"

    def availability(self) -> Availability:
        try:
            import yfinance  # noqa: F401
        except ImportError:
            return Availability(False, "yfinance is not installed (pip install '.[live]')")
        return Availability(True)

    def fetch(self, request: FetchRequest) -> FetchResult:
        import yfinance as yf

        frames: list[pd.DataFrame] = []
        for symbol in request.symbols:
            logger.info("yahoo fetch symbol=%s %s..%s", symbol, request.start, request.end)
            # auto_adjust=False keeps close and adj_close separate: the raw layer should
            # preserve both, so that a later decision to use total-return prices does not
            # require a re-download.
            frame = yf.download(
                symbol,
                start=request.start.isoformat(),
                # Yahoo's end date is exclusive; add a day so the window is inclusive.
                end=(request.end + dt.timedelta(days=1)).isoformat(),
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            if frame is None or frame.empty:
                logger.warning("yahoo returned no rows for %s", symbol)
                continue
            if isinstance(frame.columns, pd.MultiIndex):
                frame.columns = frame.columns.get_level_values(0)
            frame = frame.rename(columns=_COLUMN_MAP).reset_index()
            frame = frame.rename(columns={frame.columns[0]: "date"})
            frame["ticker"] = symbol
            for column in RAW_COLUMNS:
                if column not in frame.columns:
                    frame[column] = pd.NA
            frames.append(frame[RAW_COLUMNS])

        payload = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=RAW_COLUMNS)
        )
        return FetchResult(frame=payload, params=request.as_params())


class YahooMarketPricesAdapter(_YahooBase):
    dataset = "market_prices"
    description = "Daily OHLCV bars for exchange-traded instruments"


class YahooYieldIndexAdapter(_YahooBase):
    dataset = "macro_data"
    description = "Yield indices (e.g. ^TNX) served through the equity endpoint"

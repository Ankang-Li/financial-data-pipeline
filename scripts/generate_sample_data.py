"""Generate deterministic, synthetic raw snapshots committed to ``data/sample/raw``.

These are *not* real market data. They are reproducible stand-ins with the same shapes
and the same kinds of defects a public feed actually ships: a yield quoted in the wrong
unit, a missed observation, a stale run, a gap, an implausible jump. They let the whole
pipeline and the test suite run offline, with no API key and no network, while still
exercising the validation engine on realistic problems.

What is deliberately wrong, and which check catches it:

* AKShare publishes the US 10y in basis points (425 instead of 4.25) while FRED and Yahoo
  use percent. Normalization applies the *declared* unit, so the value lands 100x too
  large, and ``check_cross_source_consistency`` raises ERROR — without ever blocking the
  load of the other, correct series.
* A short stretch of missing FRED VIX observations (``.`` marker) -> ``check_value_missingness`` WARNING.
* A multi-day gap in one ETF and in AKShare -> ``check_calendar_gaps`` WARNING.
* One +50% ETF day -> ``check_extreme_returns`` WARNING.
* A 12-day flat run in a China yield -> ``check_stale_values`` WARNING.

Hard ERROR-class defects (OHLC violation, duplicate timestamps, >5% missing) are covered
by the unit tests against constructed frames, so they do not quarantine the very tables
the research example depends on.

Re-running this script regenerates byte-identical snapshots (fixed seed and fixed
retrieval timestamp), so the committed files are stable and CI is reproducible.
"""

from __future__ import annotations

import datetime as dt
import shutil

import numpy as np
import pandas as pd

from pipeline.config import get_settings
from pipeline.ingestion.plan import DEFAULT_PLAN, DEFAULT_START
from pipeline.logging_utils import configure_logging, get_logger
from pipeline.provenance import RunContext
from pipeline.raw.store import RawStore

logger = get_logger(__name__)

SEED = 42
END = dt.date(2024, 12, 31)
# Fixed retrieval timestamp so the committed snapshot ids and checksums are reproducible.
RETRIEVED_AT = dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)


def _business_days() -> pd.DatetimeIndex:
    return pd.bdate_range(DEFAULT_START, END)


def _rand(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def _random_walk(n: int, level0: float, vol: float, drift: float, rng: np.random.Generator) -> np.ndarray:
    return level0 * np.exp(np.cumsum(rng.normal(0, vol, n)) + np.linspace(0, drift, n))


def _us10y_truth(n: int, rng: np.random.Generator) -> np.ndarray:
    """The 'true' US 10y yield in percent; FRED and Yahoo are derived from it so they
    genuinely agree, leaving AKShare (injected 100x) as the unambiguous outlier."""
    return _random_walk(n, 4.25, 0.006, 0.3, rng)


def _yahoo_market() -> pd.DataFrame:
    rng = _rand(SEED)
    days = _business_days()
    tickers = {"SPY": 250.0, "TLT": 105.0, "GLD": 130.0}
    frames = []
    for ticker, price in tickers.items():
        d = days
        n = len(d)
        level = _random_walk(n, price, 0.012, 0.2, rng)
        close = level
        open_ = np.concatenate([[close[0]], close[:-1]]) * (1 + rng.normal(0, 0.002, n))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.003, n)))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, n)))
        volume = (1e7 * (1 + rng.normal(0, 0.3, n))).round()
        frame = pd.DataFrame(
            {
                "date": [x.date().isoformat() for x in d],
                "ticker": ticker,
                "open": open_.round(3),
                "high": high.round(3),
                "low": low.round(3),
                "close": close.round(3),
                "adj_close": close.round(3),
                "volume": volume.astype("int64"),
            }
        )
        frames.append(frame)

    # --- injected issues (WARNING-class only, so the table still loads) ---
    spy = frames[0]
    idx = len(spy) // 3
    for col in ("open", "high", "low", "close", "adj_close"):
        spy.loc[spy.index[idx], col] = float(spy.loc[spy.index[idx - 1], col]) * 1.5
    gld = frames[2]
    gap_start, gap_end = len(gld) // 2, len(gld) // 2 + 12
    frames[2] = gld.drop(gld.index[gap_start:gap_end]).reset_index(drop=True)

    return pd.concat(frames, ignore_index=True)


def _yahoo_yield(us10y: np.ndarray) -> pd.DataFrame:
    rng = _rand(SEED + 1)
    days = _business_days()
    n = len(days)
    # ^TNX is quoted in tenths of a percent, so a 4.25% yield arrives as 42.5.
    close = (us10y * 10.0 + rng.normal(0, 0.02, n)).round(3)
    open_ = np.concatenate([[close[0]], close[:-1]]) * (1 + rng.normal(0, 0.002, n))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.003, n)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, n)))
    volume = (5e6 * (1 + rng.normal(0, 0.3, n))).round()
    return pd.DataFrame(
        {
            "date": [x.date().isoformat() for x in days],
            "ticker": "^TNX",
            "open": open_.round(3),
            "high": high.round(3),
            "low": low.round(3),
            "close": close.round(3),
            "adj_close": close.round(3),
            "volume": volume.astype("int64"),
        }
    )


def _fred_macro(us10y: np.ndarray) -> pd.DataFrame:
    rng = _rand(SEED + 2)
    days = _business_days()
    n = len(days)

    dgs10 = (us10y + rng.normal(0, 0.02, n)).round(3)  # US 10y in percent
    dgs2 = _random_walk(n, 1.5, 0.008, 0.3, rng).round(3)  # US 2y in percent
    cpi = _random_walk(n, 250.0, 0.001, 0.25, rng).round(3)  # CPI index level
    vix = _random_walk(n, 18.0, 0.03, 0.0, rng).round(2)  # VIX index level

    # A short stretch of missing VIX observations (FRED's "." marker).
    miss_start = len(vix) // 4
    vix[miss_start : miss_start + 8] = np.nan

    rows = []
    for i, day in enumerate(days):
        iso = day.date().isoformat()
        rows.append({"date": iso, "series_id": "DGS10", "value": _text(dgs10[i])})
        rows.append({"date": iso, "series_id": "DGS2", "value": _text(dgs2[i])})
        rows.append({"date": iso, "series_id": "CPIAUCSL", "value": _text(cpi[i])})
        rows.append({"date": iso, "series_id": "VIXCLS", "value": _text(vix[i])})
    return pd.DataFrame(rows, columns=["date", "series_id", "value"])


def _text(value: float) -> str:
    return "." if pd.isna(value) else f"{value:.3f}"


def _akshare_macro(us10y: np.ndarray) -> pd.DataFrame:
    rng = _rand(SEED + 3)
    days = _business_days()
    n = len(days)

    cn2 = _random_walk(n, 2.2, 0.008, 0.2, rng).round(3)
    cn5 = _random_walk(n, 2.4, 0.008, 0.25, rng).round(3)
    cn10 = _random_walk(n, 2.5, 0.008, 0.3, rng).round(3)
    cn30 = _random_walk(n, 3.0, 0.008, 0.35, rng).round(3)
    # US 10y as published in China, in percent -- but injected 100x off (basis points).
    us10 = (us10y * 100.0).round(3)

    # A 12-day flat run in the 30y (frozen feed).
    cn30[100:112] = cn30[100]
    # A multi-day gap (drop a contiguous slice).
    gap = slice(n // 3, n // 3 + 9)

    frame = pd.DataFrame(
        {
            "日期": [d.date().isoformat() for d in days],
            "中国国债收益率2年": cn2.round(3),
            "中国国债收益率5年": cn5.round(3),
            "中国国债收益率10年": cn10.round(3),
            "中国国债收益率30年": cn30.round(3),
            "美国国债收益率10年": us10.round(3),
        }
    )
    return frame.drop(frame.index[gap]).reset_index(drop=True)


def _raw_for(source: str, dataset: str, us10y: np.ndarray) -> pd.DataFrame:
    if source == "yahoo" and dataset == "market_prices":
        return _yahoo_market()
    if source == "yahoo" and dataset == "macro_data":
        return _yahoo_yield(us10y)
    if source == "fred" and dataset == "macro_data":
        return _fred_macro(us10y)
    if source == "akshare" and dataset == "macro_data":
        return _akshare_macro(us10y)
    raise ValueError(f"no sample generator for {source}:{dataset}")


def main() -> None:
    configure_logging()
    settings = get_settings()
    root = settings.paths.sample_raw_dir
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    us10y = _us10y_truth(len(_business_days()), _rand(SEED + 99))

    store = RawStore(root)
    run = RunContext.new(mode="offline", config={"seed": SEED, "end": END.isoformat()})

    for spec in DEFAULT_PLAN:
        frame = _raw_for(spec.source, spec.dataset, us10y)
        notes = {
            "yahoo": "synthetic OHLCV; SPY +50% day, GLD 12bd gap",
            "fred": "synthetic FRED; VIX missing stretch",
            "akshare": "synthetic China yields; US10y injected 100x (bp), 30y stale, gap",
        }.get(spec.source, "")
        snapshot = store.write(
            source=spec.source,
            dataset=spec.dataset,
            frame=frame,
            run=run,
            params={"plan": spec.description, "symbols": list(spec.symbols)},
            synthetic=True,
            notes=notes,
            retrieved_at=RETRIEVED_AT,
        )
        logger.info("wrote sample snapshot %s (%d rows)", snapshot.snapshot_id, snapshot.row_count)

    logger.info("sample raw layer written to %s", root)


if __name__ == "__main__":
    main()

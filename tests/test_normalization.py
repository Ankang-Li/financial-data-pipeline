"""Normalization turns source-shaped payloads into canonical tables, applying symbol
mapping and unit conversion exactly once.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from pipeline.normalization.normalizer import normalize_snapshots
from pipeline.provenance import RunContext
from pipeline.raw.store import RawStore


def _write_raw(tmp_path, source, dataset, frame):
    store = RawStore(tmp_path)
    run = RunContext.new(mode="offline", config={})
    return store.write(
        source=source,
        dataset=dataset,
        frame=frame,
        run=run,
        params={},
        retrieved_at=dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
    )


def test_yahoo_market_prices_normalized(tmp_path):
    frame = pd.DataFrame(
        {
            "date": ["2020-01-02", "2020-01-03"],
            "ticker": ["SPY", "SPY"],
            "open": ["300.1", "301.2"],
            "high": ["302.0", "303.0"],
            "low": ["299.0", "300.0"],
            "close": ["301.0", "302.5"],
            "adj_close": ["301.0", "302.5"],
            "volume": ["1e7", "1.1e7"],
        }
    )
    snap = _write_raw(tmp_path, "yahoo", "market_prices", frame)
    out = normalize_snapshots([snap], run_id="r1")["market_prices"]

    assert list(out["ticker"].unique()) == ["SPY"]
    assert (out["open"] > 0).all()
    assert out["currency"].iloc[0] == "USD"
    assert out["source"].iloc[0] == "yahoo"
    assert "snapshot_id" in out.columns and "ingested_at" in out.columns


def test_yahoo_tnx_rescaled_from_tenths_to_percent(tmp_path):
    # ^TNX quotes 4.25% as 42.5; normalization must divide by 10 via unit, not magic.
    frame = pd.DataFrame(
        {
            "date": ["2020-01-02", "2020-01-03"],
            "ticker": ["^TNX", "^TNX"],
            "open": ["42.4", "42.6"],
            "high": ["42.9", "42.8"],
            "low": ["42.1", "42.2"],
            "close": ["42.5", "42.7"],
            "adj_close": ["42.5", "42.7"],
            "volume": ["5e6", "5e6"],
        }
    )
    snap = _write_raw(tmp_path, "yahoo", "macro_data", frame)
    out = normalize_snapshots([snap], run_id="r1")["macro_data"]

    assert (out["indicator"] == "US_TREASURY_10Y").all()
    assert out["unit"].iloc[0] == "percent"
    assert out["value"].round(3).tolist() == [4.25, 4.27]


def test_unmapped_symbol_is_dropped(tmp_path):
    frame = pd.DataFrame(
        {
            "date": ["2020-01-02"],
            "ticker": ["NOTREAL"],
            "open": ["1"],
            "high": ["1"],
            "low": ["1"],
            "close": ["1"],
            "adj_close": ["1"],
            "volume": ["1"],
        }
    )
    snap = _write_raw(tmp_path, "yahoo", "market_prices", frame)
    out = normalize_snapshots([snap], run_id="r1")["market_prices"]
    assert out.empty

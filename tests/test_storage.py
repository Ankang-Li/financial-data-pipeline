"""DuckDB warehouse: idempotent per-source loads and faithful round-trips."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from pipeline.provenance import RunContext
from pipeline.storage.warehouse import open_warehouse


def _macro_df(run_id: str) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=5, freq="B")
    return pd.DataFrame(
        {
            "date": dates,
            "indicator": "US_TREASURY_10Y",
            "source": "fred",
            "value": [4.2, 4.3, 4.1, 4.4, 4.25],
            "unit": "percent",
            "frequency": "B",
            "snapshot_id": "s1",
            "run_id": run_id,
            "ingested_at": pd.Timestamp.now(dt.timezone.utc),
        }
    )


def test_load_and_query_round_trip(tmp_path):
    wh = open_warehouse(warehouse_path=tmp_path / "research.duckdb")
    run = RunContext.new(mode="offline", config={})
    wh.load_dataset("macro_data", _macro_df(run.run_id), run=run, schema_version="1.0.0")

    back = wh.query("SELECT indicator, source, value FROM macro_data ORDER BY date")
    assert len(back) == 5
    assert back["indicator"].iloc[0] == "US_TREASURY_10Y"
    assert back["source"].iloc[0] == "fred"


def test_load_is_idempotent_per_source(tmp_path):
    wh = open_warehouse(warehouse_path=tmp_path / "research.duckdb")
    run = RunContext.new(mode="offline", config={})
    wh.load_dataset("macro_data", _macro_df(run.run_id), run=run, schema_version="1.0.0")
    wh.load_dataset("macro_data", _macro_df(run.run_id), run=run, schema_version="1.0.0")

    assert wh.row_count("macro_data") == 5


def test_replacing_one_source_keeps_others(tmp_path):
    wh = open_warehouse(warehouse_path=tmp_path / "research.duckdb")
    run = RunContext.new(mode="offline", config={})
    wh.load_dataset("macro_data", _macro_df(run.run_id), run=run, schema_version="1.0.0")

    yahoo = _macro_df(run.run_id)
    yahoo["source"] = "yahoo"
    yahoo["value"] = [4.25, 4.26, 4.24, 4.27, 4.28]
    wh.load_dataset("macro_data", yahoo, run=run, schema_version="1.0.0")

    sources = wh.query("SELECT DISTINCT source FROM macro_data")["source"].tolist()
    assert set(sources) == {"fred", "yahoo"}
    assert wh.row_count("macro_data") == 10


def test_validation_results_persisted(tmp_path):
    wh = open_warehouse(warehouse_path=tmp_path / "research.duckdb")
    from pipeline.validation.base import ValidationReport, ok

    report = ValidationReport(dataset="macro_data", source="fred", stage="normalized", run_id="r1")
    report.add(ok("dtypes", "all good"))
    wh.persist_validation(report)
    rows = wh.query("SELECT * FROM validation_results")
    assert len(rows) == 1
    assert rows["passed"].iloc[0]

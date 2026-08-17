"""End-to-end offline run: the committed synthetic samples flow through the whole
pipeline with no network, producing loadable tables and a recorded validation report.
"""

from __future__ import annotations

from pathlib import Path

from pipeline.run import run_pipeline


def test_offline_pipeline_loads_both_tables(tmp_path: Path):
    result = run_pipeline(
        mode="offline",
        warehouse_path=tmp_path / "research.duckdb",
        quiet=True,
    )
    assert result.loaded_rows.get("macro_data", 0) > 0
    assert result.loaded_rows.get("market_prices", 0) > 0
    # No source should be quarantined: the injected defects are WARNING-class or a
    # post-load cross-source ERROR, neither of which blocks a load.
    assert result.quarantined == []


def test_offline_pipeline_flags_akshare_unit_mismatch(tmp_path: Path):
    result = run_pipeline(
        mode="offline",
        warehouse_path=tmp_path / "research.duckdb",
        quiet=True,
    )
    assert result.cross_source is not None
    errors = [r for r in result.cross_source.errors if r.check == "cross_source_consistency"]
    assert errors, "expected the AKShare 100x unit mismatch to be caught"
    assert any("akshare" in r.message for r in errors)


def test_research_api_reads_loaded_data(tmp_path: Path):
    run_pipeline(mode="offline", warehouse_path=tmp_path / "research.duckdb", quiet=True)
    from pipeline import load_dataset

    us10y = load_dataset("macro_data", indicators=["US_TREASURY_10Y"], warehouse_path=tmp_path / "research.duckdb")
    # FRED and Yahoo normalise to percent (~4-5); AKShare lands ~425 and is still present.
    assert set(us10y["source"].unique()) == {"fred", "yahoo", "akshare"}
    assert us10y["unit"].unique().tolist() == ["percent"]

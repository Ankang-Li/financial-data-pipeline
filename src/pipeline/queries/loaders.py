"""Research-facing query API.

Everything a research notebook or a quant script needs is here. The contract is
intentional: callers name a *dataset* and optionally filter by canonical identifier,
never by source table internals. If the data came from Yahoo, FRED or AKShare is a
filter, not a requirement.

    from pipeline import load_dataset, query

    us10y = load_dataset("macro_data", indicators=["US_TREASURY_10Y"])
    panel = load_dataset("market_prices", tickers=["SPY", "TLT", "GLD"], start="2020-01-01")
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.logging_utils import get_logger
from pipeline.schemas import get_schema
from pipeline.storage.warehouse import open_warehouse

logger = get_logger(__name__)

_SQL_DIR = Path(__file__).resolve().parent / "sql"


def _open(warehouse_path: str | Path | None = None):
    return open_warehouse(warehouse_path=warehouse_path)


def load_dataset(
    dataset: str,
    *,
    indicators: list[str] | None = None,
    tickers: list[str] | None = None,
    sources: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    warehouse_path: str | Path | None = None,
) -> pd.DataFrame:
    """Read a canonical research table, filtered by canonical identifiers and date window.

    ``indicators`` filters ``macro_data``; ``tickers`` filters ``market_prices``. Both are
    optional. Returns the rows in date order.
    """
    get_schema(dataset)  # raises on unknown dataset
    where: list[str] = []
    params: list[object] = []

    entity_filter = indicators if dataset == "macro_data" else tickers
    entity_column = "indicator" if dataset == "macro_data" else "ticker"
    if entity_filter:
        placeholders = ", ".join(["?"] * len(entity_filter))
        where.append(f"{entity_column} IN ({placeholders})")
        params.extend(entity_filter)
    if sources:
        placeholders = ", ".join(["?"] * len(sources))
        where.append(f"source IN ({placeholders})")
        params.extend(sources)
    if start:
        where.append("date >= ?")
        params.append(start)
    if end:
        where.append("date <= ?")
        params.append(end)

    sql = f"SELECT * FROM {dataset}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date, source"
    return _open(warehouse_path).query(sql, params)


def query(sql: str, params: list | dict | None = None, *, warehouse_path: str | Path | None = None) -> pd.DataFrame:
    """Run arbitrary SQL against the warehouse (read-only by convention)."""
    return _open(warehouse_path).query(sql, params)


def named_query(name: str, params: list | None = None, *, warehouse_path: str | Path | None = None) -> pd.DataFrame:
    """Run a parameterized query stored under ``queries/sql/<name>.sql``."""
    path = _SQL_DIR / f"{name}.sql"
    if not path.is_file():
        raise FileNotFoundError(f"no SQL query named {name!r} at {path}")
    sql = path.read_text(encoding="utf-8")
    return _open(warehouse_path).query(sql, params)


def list_datasets(*, warehouse_path: str | Path | None = None) -> list[str]:
    """Datasets that have been loaded at least once."""
    wh = _open(warehouse_path)
    if "dataset_metadata" not in wh.tables():
        return []
    rows = wh.query("SELECT DISTINCT dataset FROM dataset_metadata ORDER BY dataset")
    return rows["dataset"].tolist()


def dataset_metadata(
    name: str | None = None, *, warehouse_path: str | Path | None = None
) -> pd.DataFrame:
    """Load log: when and how each (dataset, source) partition was written."""
    sql = "SELECT * FROM dataset_metadata"
    if name:
        sql += " WHERE dataset = ?"
        return _open(warehouse_path).query(sql, [name])
    return _open(warehouse_path).query(sql + " ORDER BY loaded_at_utc DESC")


def validation_report(
    run_id: str | None = None,
    *,
    dataset: str | None = None,
    source: str | None = None,
    warehouse_path: str | Path | None = None,
) -> pd.DataFrame:
    """All recorded validation checks, optionally filtered and ordered worst-first."""
    where: list[str] = []
    params: list[object] = []
    if run_id:
        where.append("run_id = ?")
        params.append(run_id)
    if dataset:
        where.append("dataset = ?")
        params.append(dataset)
    if source:
        where.append("source = ?")
        params.append(source)
    sql = "SELECT * FROM validation_results"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY run_id, stage, severity DESC, check_name"
    return _open(warehouse_path).query(sql, params)

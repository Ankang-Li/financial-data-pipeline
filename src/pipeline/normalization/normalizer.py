"""Normalization: turn raw source payloads into canonical research tables.

This is the only place where values are retyped, renamed and rescaled. The raw
layer upstream stores bytes exactly as received; the storage layer downstream
trusts whatever lands here. Everything that makes two sources comparable
therefore lives in this module:

* text -> typed (``coerce_date`` / ``coerce_numeric`` from :mod:`pipeline.dtypes`);
* source symbol -> canonical identifier (``INSTRUMENT_SYMBOL_MAP`` /
  ``SERIES_MAP`` in :mod:`pipeline.normalization.identifiers`);
* source unit -> canonical unit, applied exactly once (``convert`` in
  :mod:`pipeline.normalization.units`).

A row whose symbol or series the project has not declared is dropped and counted,
never silently passed through. That is the entire mechanism that lets
``load_dataset("macro_data", indicators=["US_TREASURY_10Y"])`` mean the same thing
no matter which source produced the number.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from pipeline.config import Settings, get_settings
from pipeline.dtypes import coerce_date, coerce_numeric
from pipeline.logging_utils import get_logger
from pipeline.normalization.identifiers import (
    canonical_ticker,
    indicator,
    instrument,
    series_mapping,
)
from pipeline.normalization.units import convert
from pipeline.provenance import utc_now
from pipeline.raw.store import RawStore, Snapshot
from pipeline.schemas import MACRO_DATA, MARKET_PRICES

logger = get_logger(__name__)

_EMPTY_DTYPE = {
    "date": "datetime64[ns]",
    "timestamp": "datetime64[ns]",
    "string": "string",
    "float": "float64",
    "int": "int64",
}


def _empty(schema) -> pd.DataFrame:
    return pd.DataFrame({c.name: pd.Series(dtype=_EMPTY_DTYPE[c.dtype]) for c in schema.columns})


def _utc() -> dt.datetime:
    return utc_now()


# --------------------------------------------------------------------------- market_prices
def _market_from_frame(
    frame: pd.DataFrame, *, source: str, snapshot_id: str, run_id: str, ingested_at: dt.datetime
) -> pd.DataFrame | None:
    if frame.empty:
        return None
    out = frame.copy()
    if "date" not in out.columns or "ticker" not in out.columns:
        logger.warning("market: snapshot %s missing date/ticker columns; skipped", snapshot_id)
        return None

    out["date"] = coerce_date(out["date"])
    out["ticker"] = out["ticker"].map(lambda s: canonical_ticker(source, str(s)))
    dropped = int(out["ticker"].isna().sum())
    if dropped:
        logger.warning("market: dropped %d unmapped ticker rows from %s", dropped, source)
        out = out[out["ticker"].notna()]
    if out.empty:
        return None

    for column in ("open", "high", "low", "close", "adj_close", "volume"):
        out[column] = coerce_numeric(out[column]) if column in out.columns else pd.NA

    out["currency"] = out["ticker"].map(
        lambda t: instrument(t).currency if instrument(t) is not None else None
    )
    out["source"] = source
    out["snapshot_id"] = snapshot_id
    out["run_id"] = run_id
    out["ingested_at"] = ingested_at
    return out[[c.name for c in MARKET_PRICES.columns]]


# -------------------------------------------------------------------------------- macro_data
def _macro_row_block(
    *,
    dates: pd.Series,
    values: pd.Series,
    mapping_indicator: str,
    source: str,
    source_unit: str,
    scale: float,
    snapshot_id: str,
    run_id: str,
    ingested_at: dt.datetime,
) -> pd.DataFrame:
    converted, unit = convert(values, source_unit, scale)
    ind = indicator(mapping_indicator)
    frequency = ind.frequency if ind is not None else "B"
    return pd.DataFrame(
        {
            "date": dates.reset_index(drop=True),
            "indicator": mapping_indicator,
            "source": source,
            "value": converted.reset_index(drop=True),
            "unit": unit,
            "frequency": frequency,
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "ingested_at": ingested_at,
        }
    )


def _macro_from_yahoo(
    frame: pd.DataFrame, *, snapshot_id: str, run_id: str, ingested_at: dt.datetime
) -> pd.DataFrame | None:
    if "close" not in frame.columns or "ticker" not in frame.columns:
        return None
    long = frame[["date", "ticker", "close"]].copy()
    long["date"] = coerce_date(long["date"])
    long = long.dropna(subset=["date"])
    long["_m"] = long["ticker"].apply(lambda t: series_mapping("yahoo", str(t)))
    long = long[long["_m"].notna()]
    if long.empty:
        return None
    blocks = []
    for _, grp in long.groupby("ticker"):
        m = series_mapping("yahoo", str(grp["ticker"].iloc[0]))
        blocks.append(
            _macro_row_block(
                dates=grp["date"],
                values=coerce_numeric(grp["close"]),
                mapping_indicator=m.indicator,
                source="yahoo",
                source_unit=m.source_unit,
                scale=m.scale,
                snapshot_id=snapshot_id,
                run_id=run_id,
                ingested_at=ingested_at,
            )
        )
    return pd.concat(blocks, ignore_index=True)


def _macro_from_fred(
    frame: pd.DataFrame, *, snapshot_id: str, run_id: str, ingested_at: dt.datetime
) -> pd.DataFrame | None:
    if "series_id" not in frame.columns or "value" not in frame.columns:
        return None
    long = frame[["date", "series_id", "value"]].copy()
    long["date"] = coerce_date(long["date"])
    long = long.dropna(subset=["date"])
    long["_m"] = long["series_id"].apply(lambda s: series_mapping("fred", str(s)))
    unmapped = int(long["_m"].isna().sum())
    if unmapped:
        logger.warning("fred: dropped %d rows with undeclared series_id", unmapped)
        long = long[long["_m"].notna()]
    if long.empty:
        return None
    blocks = []
    for _, grp in long.groupby("series_id"):
        m = series_mapping("fred", str(grp["series_id"].iloc[0]))
        blocks.append(
            _macro_row_block(
                dates=grp["date"],
                values=coerce_numeric(grp["value"]),
                mapping_indicator=m.indicator,
                source="fred",
                source_unit=m.source_unit,
                scale=m.scale,
                snapshot_id=snapshot_id,
                run_id=run_id,
                ingested_at=ingested_at,
            )
        )
    return pd.concat(blocks, ignore_index=True)


def _macro_from_akshare(
    frame: pd.DataFrame, *, snapshot_id: str, run_id: str, ingested_at: dt.datetime
) -> pd.DataFrame | None:
    date_col = "日期"
    if date_col not in frame.columns:
        logger.warning("akshare: snapshot %s missing 日期 column; skipped", snapshot_id)
        return None
    long = frame.melt(id_vars=[date_col], var_name="raw_label", value_name="raw_value")
    long["date"] = coerce_date(long[date_col])
    long = long.dropna(subset=["date"])
    long["_m"] = long["raw_label"].apply(lambda lab: series_mapping("akshare", str(lab)))
    unmapped = int(long["_m"].isna().sum())
    if unmapped:
        logger.warning("akshare: dropped %d columns with undeclared label", unmapped)
        long = long[long["_m"].notna()]
    if long.empty:
        return None
    blocks = []
    for _, grp in long.groupby("raw_label"):
        m = series_mapping("akshare", str(grp["raw_label"].iloc[0]))
        blocks.append(
            _macro_row_block(
                dates=grp["date"],
                values=coerce_numeric(grp["raw_value"]),
                mapping_indicator=m.indicator,
                source="akshare",
                source_unit=m.source_unit,
                scale=m.scale,
                snapshot_id=snapshot_id,
                run_id=run_id,
                ingested_at=ingested_at,
            )
        )
    return pd.concat(blocks, ignore_index=True)


def _macro_from_frame(
    frame: pd.DataFrame, *, source: str, snapshot_id: str, run_id: str, ingested_at: dt.datetime
) -> pd.DataFrame | None:
    if source == "yahoo":
        return _macro_from_yahoo(frame, snapshot_id=snapshot_id, run_id=run_id, ingested_at=ingested_at)
    if source == "fred":
        return _macro_from_fred(frame, snapshot_id=snapshot_id, run_id=run_id, ingested_at=ingested_at)
    if source == "akshare":
        return _macro_from_akshare(frame, snapshot_id=snapshot_id, run_id=run_id, ingested_at=ingested_at)
    logger.warning("macro: no normalizer for source %s", source)
    return None


# -------------------------------------------------------------------------------- public API
def normalize_snapshots(
    snapshots: list[Snapshot], run_id: str, *, ingested_at: dt.datetime | None = None
) -> dict[str, pd.DataFrame]:
    """Normalize a set of raw snapshots into canonical tables.

    ``snapshots`` are read back from disk (``Snapshot.load_frame``) so the frames that
    get validated and stored are exactly the bytes that were committed to the raw layer.
    """
    ingested_at = ingested_at or _utc()
    by_dataset: dict[str, list[Snapshot]] = {}
    for snap in snapshots:
        by_dataset.setdefault(snap.dataset, []).append(snap)

    out: dict[str, pd.DataFrame] = {}
    if "market_prices" in by_dataset:
        parts = [
            _market_from_frame(
                s.load_frame(),
                source=s.source,
                snapshot_id=s.snapshot_id,
                run_id=run_id,
                ingested_at=ingested_at,
            )
            for s in by_dataset["market_prices"]
        ]
        parts = [p for p in parts if p is not None]
        out["market_prices"] = pd.concat(parts, ignore_index=True) if parts else _empty(MARKET_PRICES)
    if "macro_data" in by_dataset:
        parts = [
            _macro_from_frame(
                s.load_frame(),
                source=s.source,
                snapshot_id=s.snapshot_id,
                run_id=run_id,
                ingested_at=ingested_at,
            )
            for s in by_dataset["macro_data"]
        ]
        parts = [p for p in parts if p is not None]
        out["macro_data"] = pd.concat(parts, ignore_index=True) if parts else _empty(MACRO_DATA)
    return out


def normalize_latest(
    run_id: str, *, settings: Settings | None = None, ingested_at: dt.datetime | None = None
) -> dict[str, pd.DataFrame]:
    """Normalize the latest committed sample snapshots (offline demo / examples)."""
    settings = settings or get_settings()
    store = RawStore(settings.paths.sample_raw_dir)
    snapshots = store.list_snapshots()
    if not snapshots:
        raise FileNotFoundError(
            f"no raw snapshots under {settings.paths.sample_raw_dir}; "
            "run `python scripts/generate_sample_data.py` first"
        )
    return normalize_snapshots(snapshots, run_id=run_id, ingested_at=ingested_at)

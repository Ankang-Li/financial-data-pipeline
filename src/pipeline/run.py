"""Pipeline orchestration: wire ingestion -> raw -> validation -> normalization -> warehouse.

One entry point, :func:`run_pipeline`, drives the whole flow for either mode:

* ``offline`` — replay the committed synthetic snapshots (no network, no key); this is
  what CI, the demo and the tests use.
* ``live``   — call the real adapters (Yahoo/FRED/AKShare); requires the optional extras
  and, for FRED, ``FDP_FRED_API_KEY``.

The orchestrator is deliberately thin. It does not know what a yield *is*; it knows the
order of operations and the routing rules:

* a raw payload that fails structural validation is logged and skipped;
* a normalized table that raises an ERROR is **quarantined** (written to
  ``data/quarantine`` and never loaded) — WARNINGs load but are recorded next to the data;
* cross-source consistency is checked last, after every usable source is loaded, and is
  reported but does not un-load anything — it is analyst signal, not a gate.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from pipeline.config import Settings, get_settings
from pipeline.ingestion.base import FetchRequest
from pipeline.ingestion.plan import DEFAULT_START, plan_for
from pipeline.ingestion.registry import get_adapter
from pipeline.logging_utils import configure_logging, get_logger
from pipeline.normalization.normalizer import normalize_snapshots
from pipeline.provenance import RunContext
from pipeline.raw.store import RawStore, Snapshot
from pipeline.schemas import MACRO_DATA_SCHEMA_VERSION, MARKET_PRICES_SCHEMA_VERSION
from pipeline.storage.warehouse import Warehouse, open_warehouse
from pipeline.validation.base import ValidationReport
from pipeline.validation.runner import (
    validate_cross_source,
    validate_normalized,
    validate_raw,
)

logger = get_logger(__name__)

_SCHEMA_VERSION = {"market_prices": MARKET_PRICES_SCHEMA_VERSION, "macro_data": MACRO_DATA_SCHEMA_VERSION}


@dataclass
class RunResult:
    run_id: str
    mode: str
    warehouse_path: Path
    loaded_rows: dict[str, int] = field(default_factory=dict)
    quarantined: list[tuple[str, str]] = field(default_factory=list)
    reports: list[ValidationReport] = field(default_factory=list)
    cross_source: ValidationReport | None = None

    @property
    def n_errors(self) -> int:
        return sum(len(r.errors) for r in self.reports)

    @property
    def n_warnings(self) -> int:
        return sum(len(r.warnings) for r in self.reports)

    def summary(self) -> str:
        loaded = ", ".join(f"{k}={v}" for k, v in self.loaded_rows.items()) or "none"
        quarantined = ", ".join(f"{d}/{s}" for d, s in self.quarantined) or "none"
        return (
            f"run {self.run_id} [{self.mode}] loaded=({loaded}) "
            f"quarantined=({quarantined}) errors={self.n_errors} warnings={self.n_warnings}"
        )


def _offline_snapshots(settings: Settings) -> list[Snapshot]:
    store = RawStore(settings.paths.sample_raw_dir)
    return store.list_snapshots()


def run_pipeline(
    *,
    mode: str = "offline",
    sources: tuple[str, ...] | None = None,
    start: dt.date | None = None,
    end: dt.date | None = None,
    settings: Settings | None = None,
    warehouse_path: str | Path | None = None,
    quiet: bool = False,
) -> RunResult:
    """Execute ingestion -> validation -> normalization -> warehouse for the default plan.

    Returns a :class:`RunResult` summarising what loaded, what was quarantined, and every
    validation report produced.
    """
    if not quiet:
        configure_logging()
    settings = settings or get_settings()
    settings.paths.ensure()

    start = start or DEFAULT_START
    end = end or dt.date.today()
    plan = plan_for(sources)

    run = RunContext.new(
        mode=mode,  # type: ignore[arg-type]
        config=settings.fingerprint(),
        plan=[f"{s.source}:{s.dataset}" for s in plan],
        window=f"{start.isoformat()}..{end.isoformat()}",
    )
    wh = open_warehouse(settings, warehouse_path=warehouse_path)

    # 1. Collect raw snapshots (replay offline, fetch+write live).
    raw_snapshots: list[Snapshot] = []
    if mode == "offline":
        raw_snapshots = _offline_snapshots(settings)
    else:
        live_store = RawStore(settings.paths.raw_dir)
        for spec in plan:
            adapter = get_adapter(spec.source, spec.dataset, mode="live")
            availability = adapter.availability()
            if not availability.ok:
                logger.warning("skip %s:%s — %s", spec.source, spec.dataset, availability.reason)
                continue
            request = FetchRequest(dataset=spec.dataset, symbols=spec.symbols, start=start, end=end)
            result = adapter.fetch(request)
            snap = live_store.write(
                source=spec.source,
                dataset=spec.dataset,
                frame=result.frame,
                run=run,
                params=result.params,
                notes=result.notes,
            )
            raw_snapshots.append(snap)

    # 2. Validate raw, normalize, validate normalized, quarantine or collect.
    collected: dict[str, list[pd.DataFrame]] = {}
    reports: list[ValidationReport] = []
    quarantined: list[tuple[str, str]] = []

    for snapshot in raw_snapshots:
        frame = snapshot.load_frame()
        raw_report = validate_raw(
            frame, source=snapshot.source, dataset=snapshot.dataset, run_id=run.run_id
        )
        reports.append(raw_report)
        if raw_report.blocking:
            logger.error("raw validation blocked %s:%s — skipped", snapshot.source, snapshot.dataset)
            continue

        normalized = normalize_snapshots([snapshot], run_id=run.run_id)
        df = normalized.get(snapshot.dataset)
        if df is None or df.empty:
            logger.warning("normalization produced no rows for %s:%s", snapshot.source, snapshot.dataset)
            continue

        norm_report = validate_normalized(
            df, dataset=snapshot.dataset, source=snapshot.source, run_id=run.run_id
        )
        reports.append(norm_report)
        if norm_report.blocking:
            _quarantine(wh, snapshot.dataset, snapshot.source, df, run, norm_report)
            quarantined.append((snapshot.dataset, snapshot.source))
            continue
        collected.setdefault(snapshot.dataset, []).append(df)

    # 3. Load collected (non-quarantined) tables, idempotently per source.
    loaded_rows: dict[str, int] = {}
    cross_source: ValidationReport | None = None
    for dataset, frames in collected.items():
        combined = pd.concat(frames, ignore_index=True)
        n = wh.load_dataset(
            dataset,
            combined,
            run=run,
            schema_version=_SCHEMA_VERSION.get(dataset),
            config_fingerprint=run.config_fingerprint,
        )
        loaded_rows[dataset] = n

        if dataset == "macro_data":
            cross_source = validate_cross_source(combined, run_id=run.run_id)
            reports.append(cross_source)

    # 4. Persist every report for auditability.
    for report in reports:
        wh.persist_validation(report)

    wh.close()
    result = RunResult(
        run_id=run.run_id,
        mode=mode,
        warehouse_path=wh.path,
        loaded_rows=loaded_rows,
        quarantined=quarantined,
        reports=reports,
        cross_source=cross_source,
    )
    logger.info(result.summary())
    return result


def _quarantine(
    wh: Warehouse,
    dataset: str,
    source: str,
    df: pd.DataFrame,
    run: RunContext,
    report: ValidationReport,
) -> None:
    path = wh.path.parent / "quarantine" / f"{dataset}__{source}__{run.run_id}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.error(
        "quarantined %s/%s (%d rows) -> %s | reasons: %s",
        dataset,
        source,
        len(df),
        path,
        "; ".join(f"{i.check}:{i.message}" for i in report.errors),
    )

"""Validation orchestration: which checks run at which stage."""

from __future__ import annotations

import pandas as pd

from pipeline.config import Settings, get_settings
from pipeline.logging_utils import get_logger
from pipeline.schemas import RawSchema, TableSchema, get_raw_schema, get_schema
from pipeline.validation import numeric_checks as numeric
from pipeline.validation import schema_checks as schema_c
from pipeline.validation import timeseries_checks as ts
from pipeline.validation.base import Severity, ValidationReport
from pipeline.validation.cross_source import check_cross_source_consistency

logger = get_logger(__name__)

# The non-key, non-provenance identifier of a series within a table.
ENTITY_COLUMNS = {"market_prices": "ticker", "macro_data": "indicator"}


def entity_column_for(dataset: str) -> str:
    try:
        return ENTITY_COLUMNS[dataset]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(f"no entity column configured for dataset {dataset!r}") from exc


def validate_raw(
    frame: pd.DataFrame,
    *,
    source: str,
    dataset: str,
    run_id: str,
    settings: Settings | None = None,
    raw_schema: RawSchema | None = None,
) -> ValidationReport:
    """Structural checks against the source's expected payload shape."""
    settings = settings or get_settings()
    raw_schema = raw_schema if raw_schema is not None else get_raw_schema(source, dataset)
    report = ValidationReport(
        dataset=dataset, source=source, stage="raw", run_id=run_id, n_rows=len(frame)
    )
    report.add(schema_c.check_payload_not_empty(frame))
    report.add(schema_c.check_duplicate_columns(frame))
    report.add(schema_c.check_raw_schema_drift(frame, raw_schema))
    report.add(schema_c.check_min_rows(frame, settings.min_expected_rows))
    logger.info("validation(raw) %s", report.summary())
    return report


def validate_normalized(
    frame: pd.DataFrame,
    *,
    dataset: str,
    source: str,
    run_id: str,
    settings: Settings | None = None,
    schema: TableSchema | None = None,
) -> ValidationReport:
    """Full contract checks against the canonical schema, plus time-series and
    numerical checks appropriate to the dataset."""
    settings = settings or get_settings()
    schema = schema or get_schema(dataset)
    entity = entity_column_for(dataset)
    report = ValidationReport(
        dataset=dataset, source=source, stage="normalized", run_id=run_id, n_rows=len(frame)
    )

    # --- schema / contract
    report.add(schema_c.check_canonical_columns(frame, schema))
    report.add(schema_c.check_dtypes(frame, schema))
    report.add(schema_c.check_nullability(frame, schema, settings.missing_ratio_error))
    report.add(schema_c.check_primary_key_unique(frame, schema))
    report.add(schema_c.check_record_contract(frame, schema))

    # --- time series
    report.add(ts.check_timezone_naive(frame))
    report.add(ts.check_duplicate_timestamps(frame, entity))
    report.add(ts.check_monotonic_timestamps(frame, entity))
    frequency_column = "frequency" if "frequency" in frame.columns else None
    report.add(ts.check_calendar_gaps(frame, entity, frequency_column=frequency_column))
    report.add(ts.check_frequency_consistency(frame, entity))

    # --- numerical / financial logic
    if dataset == "market_prices":
        report.add(numeric.check_positive_prices(frame))
        report.add(numeric.check_finite_values(frame, numeric.PRICE_COLUMNS + ("volume",)))
        report.add(numeric.check_ohlc_consistency(frame))
        report.add(numeric.check_volume_sanity(frame))
        report.add(numeric.check_extreme_returns(frame, settings.extreme_return_threshold))
    else:
        report.add(numeric.check_finite_values(frame, ("value",)))
        report.add(
            numeric.check_value_missingness(frame, entity, "value", settings.missing_ratio_error)
        )
        report.add(ts.check_stale_values(frame, entity, "value"))

    logger.info("validation(normalized) %s", report.summary())
    if report.blocking:
        for issue in report.errors:
            logger.error("  ERROR %s: %s", issue.check, issue.message)
    for issue in report.warnings:
        logger.warning("  WARNING %s: %s", issue.check, issue.message)
    return report


def validate_cross_source(
    macro: pd.DataFrame,
    *,
    run_id: str,
    settings: Settings | None = None,
) -> ValidationReport:
    """Compare multi-sourced macro series against each other."""
    settings = settings or get_settings()
    report = ValidationReport(
        dataset="macro_data",
        source="__cross_source__",
        stage="cross_source",
        run_id=run_id,
        n_rows=len(macro),
    )
    report.add(check_cross_source_consistency(macro, settings.cross_source_tolerance_bp))
    logger.info("validation(cross_source) %s", report.summary())
    for issue in report.issues:
        level = logger.error if issue.severity is Severity.ERROR else logger.warning
        level("  %s %s: %s", issue.severity.value, issue.check, issue.message)
    return report

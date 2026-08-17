"""Validation layer: schema, time-series, numerical and cross-source checks."""

from pipeline.validation.base import (
    CheckResult,
    Severity,
    ValidationReport,
    fail,
    ok,
)
from pipeline.validation.cross_source import check_cross_source_consistency, coverage_summary
from pipeline.validation.runner import (
    entity_column_for,
    validate_cross_source,
    validate_normalized,
    validate_raw,
)

__all__ = [
    "CheckResult",
    "Severity",
    "ValidationReport",
    "check_cross_source_consistency",
    "coverage_summary",
    "entity_column_for",
    "fail",
    "ok",
    "validate_cross_source",
    "validate_normalized",
    "validate_raw",
]

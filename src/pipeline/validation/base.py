"""Validation primitives: check results, severities and reports.

Design decisions worth stating explicitly, because they are what separates this from
``df.dropna()``:

* **Every check is recorded, not just the failures.** A run that reports "12 checks
  executed, 10 passed, 2 warnings" is auditable; a run that silently drops rows is not.
* **Severity decides routing, not truth.** ``ERROR`` means the dataset is quarantined and
  never reaches the research layer. ``WARNING`` means it loads but the issue is stored
  next to the data. ``INFO`` is context.
* **Issues carry evidence.** Row counts and example offending keys are kept in
  ``details`` so a problem can be reproduced from the report alone.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd

from pipeline.provenance import utc_now


class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

    @property
    def rank(self) -> int:
        return {"INFO": 0, "WARNING": 1, "ERROR": 2}[self.value]


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one validation check against one dataset."""

    check: str
    passed: bool
    message: str
    severity: Severity = Severity.WARNING
    column: str | None = None
    n_offending: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def effective_severity(self) -> Severity:
        """A passing check never carries a severity above INFO."""
        return Severity.INFO if self.passed else self.severity


def ok(check: str, message: str, **details: Any) -> CheckResult:
    return CheckResult(check=check, passed=True, message=message, details=details)


def fail(
    check: str,
    message: str,
    severity: Severity = Severity.WARNING,
    *,
    column: str | None = None,
    n_offending: int = 0,
    **details: Any,
) -> CheckResult:
    return CheckResult(
        check=check,
        passed=False,
        message=message,
        severity=severity,
        column=column,
        n_offending=n_offending,
        details=details,
    )


@dataclass
class ValidationReport:
    """All check results for one (dataset, source, stage) triple."""

    dataset: str
    source: str
    stage: str  # "raw" | "normalized" | "cross_source"
    run_id: str
    n_rows: int = 0
    created_at: dt.datetime = field(default_factory=utc_now)
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult | list[CheckResult] | None) -> None:
        if result is None:
            return
        if isinstance(result, list):
            self.results.extend(result)
        else:
            self.results.append(result)

    @property
    def issues(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.issues if r.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.issues if r.severity is Severity.WARNING]

    @property
    def blocking(self) -> bool:
        """True when the dataset must be quarantined instead of loaded."""
        return bool(self.errors)

    @property
    def max_severity(self) -> Severity:
        if not self.issues:
            return Severity.INFO
        return max((r.severity for r in self.issues), key=lambda s: s.rank)

    def to_frame(self) -> pd.DataFrame:
        """One row per check, ready for the ``validation_results`` table."""
        rows = [
            {
                "run_id": self.run_id,
                "dataset": self.dataset,
                "source": self.source,
                "stage": self.stage,
                "check_name": result.check,
                "column_name": result.column,
                "passed": result.passed,
                "severity": result.effective_severity.value,
                "n_offending": int(result.n_offending),
                "message": result.message,
                "details_json": json.dumps(result.details, default=str, ensure_ascii=False),
                "created_at": self.created_at,
            }
            for result in self.results
        ]
        return pd.DataFrame(rows)

    def summary(self) -> str:
        return (
            f"{self.dataset}/{self.source} [{self.stage}] rows={self.n_rows} "
            f"checks={len(self.results)} errors={len(self.errors)} warnings={len(self.warnings)}"
        )

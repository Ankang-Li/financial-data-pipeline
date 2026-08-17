"""Time-series checks.

What "validation" means for a financial time series is mostly about the index, not the
values: a series can have perfectly plausible numbers and still be unusable because a
day is duplicated, a week is missing, the frequency silently changed, or timestamps
arrived with a timezone that shifts an Asian trading day into the previous session.

No exchange-holiday calendar is used. Introducing one would add a dependency and a
maintenance burden for little gain here, so calendar gaps are reported with their length
and the interpretation is left to the analyst: a one-day gap is almost always a holiday,
a five-day gap almost never is.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from pipeline.validation.base import CheckResult, Severity, fail, ok

# A gap longer than this many business days is unlikely to be an exchange holiday.
SUSPICIOUS_GAP_BDAYS = 5
# Fraction of missing business days above which the series looks incomplete.
MISSING_BDAY_RATIO_WARNING = 0.05
# Identical consecutive observations beyond this length suggest a stale feed.
STALE_RUN_LENGTH = 10


def check_timezone_naive(frame: pd.DataFrame, date_column: str = "date") -> CheckResult:
    if date_column not in frame.columns:
        return fail(
            "timestamp_timezone",
            f"{date_column} column is absent",
            Severity.ERROR,
            column=date_column,
        )
    tz = getattr(frame[date_column].dtype, "tz", None)
    if tz is not None:
        return fail(
            "timestamp_timezone",
            f"{date_column} is timezone-aware ({tz}); calendar dates must be naive so that "
            "a trading day is never shifted by a UTC conversion",
            Severity.ERROR,
            column=date_column,
            timezone=str(tz),
        )
    return ok("timestamp_timezone", f"{date_column} is timezone-naive")


def check_duplicate_timestamps(
    frame: pd.DataFrame, entity_column: str, date_column: str = "date"
) -> CheckResult:
    keys = [entity_column, date_column]
    if any(k not in frame.columns for k in keys):
        return fail("duplicate_timestamps", "key columns absent", Severity.ERROR)
    duplicated = frame.duplicated(subset=keys, keep=False)
    n = int(duplicated.sum())
    if n:
        examples = (
            frame.loc[duplicated, keys]
            .drop_duplicates()
            .head(5)
            .astype(str)
            .to_dict(orient="records")
        )
        return fail(
            "duplicate_timestamps",
            f"{n} rows share an ({entity_column}, {date_column}) pair",
            Severity.ERROR,
            n_offending=n,
            examples=examples,
        )
    return ok("duplicate_timestamps", f"no duplicated ({entity_column}, {date_column}) pairs")


def check_monotonic_timestamps(
    frame: pd.DataFrame, entity_column: str, date_column: str = "date"
) -> CheckResult:
    """Whether each series arrives in chronological order.

    Run before any sorting: out-of-order delivery is a real source behaviour (paged APIs,
    concatenated backfills) and worth knowing about, even though storage sorts anyway.
    """
    offenders: list[str] = []
    for entity, group in frame.groupby(entity_column, sort=True):
        if not group[date_column].is_monotonic_increasing:
            offenders.append(str(entity))
    if offenders:
        return fail(
            "monotonic_timestamps",
            f"{len(offenders)} series are not delivered in chronological order",
            Severity.WARNING,
            n_offending=len(offenders),
            entities=offenders[:10],
        )
    return ok("monotonic_timestamps", "all series are chronologically ordered")


def _expected_step_months(frequency: str) -> int | None:
    return {"M": 1, "Q": 3}.get(frequency)


def check_calendar_gaps(
    frame: pd.DataFrame,
    entity_column: str,
    date_column: str = "date",
    frequency_column: str | None = None,
    default_frequency: str = "B",
) -> list[CheckResult]:
    """Report missing observations per series, frequency-aware."""
    results: list[CheckResult] = []
    gap_report: dict[str, dict[str, object]] = {}
    worst_gap = 0
    worst_ratio = 0.0

    for entity, group in frame.groupby(entity_column, sort=True):
        dates = pd.to_datetime(group[date_column]).dropna().sort_values().unique()
        if len(dates) < 3:
            continue
        frequency = default_frequency
        if frequency_column and frequency_column in group.columns:
            values = group[frequency_column].dropna()
            if not values.empty:
                frequency = str(values.iloc[0])

        step_months = _expected_step_months(frequency)
        if step_months is not None:
            periods = pd.PeriodIndex(pd.to_datetime(dates), freq="M")
            steps = np.diff(periods.astype("int64"))
            gaps = steps[steps > step_months]
            missing = int((gaps - step_months).sum())
            max_gap = int(gaps.max() - step_months) if gaps.size else 0
            expected_points = int((periods[-1].ordinal - periods[0].ordinal) / step_months) + 1
            unit = "months"
        else:
            as_days = pd.to_datetime(dates).to_numpy(dtype="datetime64[D]")
            steps = np.busday_count(as_days[:-1], as_days[1:])
            gaps = steps[steps > 1]
            missing = int((gaps - 1).sum())
            max_gap = int(gaps.max() - 1) if gaps.size else 0
            expected_points = int(np.busday_count(as_days[0], as_days[-1])) + 1
            unit = "business days"

        ratio = missing / max(expected_points, 1)
        if missing:
            gap_report[str(entity)] = {
                "missing": missing,
                "max_gap": max_gap,
                "unit": unit,
                "missing_ratio": round(ratio, 4),
                "n_observations": int(len(dates)),
            }
            worst_gap = max(worst_gap, max_gap)
            worst_ratio = max(worst_ratio, ratio)

    if not gap_report:
        results.append(ok("calendar_gaps", "no missing observations detected"))
        return results

    suspicious = worst_gap > SUSPICIOUS_GAP_BDAYS or worst_ratio > MISSING_BDAY_RATIO_WARNING
    severity = Severity.WARNING if suspicious else Severity.INFO
    message = (
        f"{len(gap_report)} series have missing observations "
        f"(worst gap {worst_gap}, worst missing share {worst_ratio:.2%})"
    )
    if suspicious:
        results.append(
            fail(
                "calendar_gaps",
                message,
                severity,
                n_offending=len(gap_report),
                per_series=gap_report,
                suspicious_gap_threshold=SUSPICIOUS_GAP_BDAYS,
            )
        )
    else:
        results.append(
            ok(
                "calendar_gaps",
                message + " — consistent with exchange holidays",
                per_series=gap_report,
            )
        )
    return results


def check_frequency_consistency(
    frame: pd.DataFrame,
    entity_column: str,
    date_column: str = "date",
    frequency_column: str | None = None,
) -> CheckResult:
    """Detect a series whose spacing changed, e.g. daily history turning weekly."""
    offenders: dict[str, dict[str, object]] = {}
    for entity, group in frame.groupby(entity_column, sort=True):
        dates = pd.to_datetime(group[date_column]).dropna().sort_values().unique()
        if len(dates) < 10:
            continue
        steps = pd.Series(np.diff(pd.to_datetime(dates).to_numpy(dtype="datetime64[D]")).astype(int))
        modal_step = int(steps.mode().iloc[0])
        # Weekend rollovers make a modal step of 1 appear alongside legitimate steps of 3.
        tolerated = {modal_step, modal_step + 1, modal_step + 2, modal_step + 3}
        irregular = steps[~steps.isin(tolerated)]
        ratio = len(irregular) / len(steps)
        if ratio > 0.05:
            offenders[str(entity)] = {
                "modal_step_days": modal_step,
                "irregular_share": round(ratio, 4),
                "example_steps": sorted(irregular.unique().tolist())[:5],
            }
    if offenders:
        return fail(
            "frequency_consistency",
            f"{len(offenders)} series show inconsistent observation spacing",
            Severity.WARNING,
            n_offending=len(offenders),
            per_series=offenders,
        )
    return ok("frequency_consistency", "observation spacing is consistent")


def check_stale_values(
    frame: pd.DataFrame,
    entity_column: str,
    value_column: str,
    date_column: str = "date",
    max_run_length: int = STALE_RUN_LENGTH,
) -> CheckResult:
    """Flag long runs of identical values, the classic signature of a frozen feed."""
    offenders: dict[str, dict[str, object]] = {}
    for entity, group in frame.groupby(entity_column, sort=True):
        series = group.sort_values(date_column)[value_column].dropna()
        if len(series) < max_run_length + 1:
            continue
        changed = series.ne(series.shift())
        run_ids = changed.cumsum()
        run_lengths = run_ids.value_counts()
        longest = int(run_lengths.max())
        if longest > max_run_length:
            longest_run_id = run_lengths.idxmax()
            value = series.loc[run_ids == longest_run_id].iloc[0]
            offenders[str(entity)] = {"run_length": longest, "value": float(value)}
    if offenders:
        return fail(
            "stale_values",
            f"{len(offenders)} series contain an unchanged run longer than {max_run_length} "
            "observations",
            Severity.WARNING,
            column=value_column,
            n_offending=len(offenders),
            per_series=offenders,
        )
    return ok("stale_values", f"no unchanged run longer than {max_run_length} observations")

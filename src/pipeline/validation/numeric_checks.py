"""Numerical and financial-logic checks.

These are the checks that encode domain knowledge rather than generic data hygiene: a
high below the close is not a statistical outlier, it is an impossible bar. A negative
volume is not a small error, it is a parsing failure. A 40% single-day move in a broad
ETF is possible but rare enough that it should be looked at before it reaches a model.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from pipeline.validation.base import CheckResult, Severity, fail, ok

PRICE_COLUMNS = ("open", "high", "low", "close", "adj_close")
ZERO_VOLUME_WARNING_RATIO = 0.10


def check_positive_prices(frame: pd.DataFrame) -> list[CheckResult]:
    results: list[CheckResult] = []
    offenders: dict[str, int] = {}
    for column in PRICE_COLUMNS:
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        n_bad = int((values <= 0).sum())
        if n_bad:
            offenders[column] = n_bad
    if offenders:
        results.append(
            fail(
                "positive_prices",
                f"non-positive prices found: {offenders}",
                Severity.ERROR,
                n_offending=sum(offenders.values()),
                per_column=offenders,
            )
        )
    else:
        results.append(ok("positive_prices", "all prices are strictly positive"))
    return results


def check_finite_values(frame: pd.DataFrame, columns: tuple[str, ...]) -> CheckResult:
    offenders: dict[str, int] = {}
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        n_bad = int(np.isinf(values.to_numpy(dtype="float64", na_value=np.nan)).sum())
        if n_bad:
            offenders[column] = n_bad
    if offenders:
        return fail(
            "finite_values",
            f"infinite values found: {offenders}",
            Severity.ERROR,
            n_offending=sum(offenders.values()),
            per_column=offenders,
        )
    return ok("finite_values", "no infinite values")


def check_ohlc_consistency(frame: pd.DataFrame) -> CheckResult:
    """high >= max(open, close, low) and low <= min(open, close)."""
    needed = ("open", "high", "low", "close")
    if any(c not in frame.columns for c in needed):
        return ok("ohlc_consistency", "not an OHLC dataset; check skipped")

    numeric = frame.loc[:, list(needed)].apply(pd.to_numeric, errors="coerce")
    high_violation = numeric["high"] < numeric[["open", "close", "low"]].max(axis=1)
    low_violation = numeric["low"] > numeric[["open", "close"]].min(axis=1)
    violation = high_violation | low_violation
    n = int(violation.sum())
    if n:
        key_columns = [c for c in ("date", "ticker") if c in frame.columns]
        examples = (
            pd.concat([frame.loc[violation, key_columns], numeric.loc[violation]], axis=1)
            .head(5)
            .astype(str)
            .to_dict(orient="records")
        )
        return fail(
            "ohlc_consistency",
            f"{n} bars violate OHLC ordering (high below open/close/low, or low above "
            "open/close)",
            Severity.ERROR,
            n_offending=n,
            n_high_violations=int(high_violation.sum()),
            n_low_violations=int(low_violation.sum()),
            examples=examples,
        )
    return ok("ohlc_consistency", f"all {len(frame)} bars satisfy OHLC ordering")


def check_volume_sanity(frame: pd.DataFrame) -> list[CheckResult]:
    if "volume" not in frame.columns:
        return [ok("volume_sanity", "no volume column; check skipped")]

    values = pd.to_numeric(frame["volume"], errors="coerce")
    results: list[CheckResult] = []

    n_negative = int((values < 0).sum())
    if n_negative:
        results.append(
            fail(
                "volume_non_negative",
                f"{n_negative} rows report negative volume",
                Severity.ERROR,
                column="volume",
                n_offending=n_negative,
            )
        )
    else:
        results.append(ok("volume_non_negative", "no negative volume"))

    observed = values.notna().sum()
    n_zero = int((values == 0).sum())
    ratio = n_zero / max(int(observed), 1)
    if ratio > ZERO_VOLUME_WARNING_RATIO:
        results.append(
            fail(
                "volume_zero_share",
                f"{ratio:.2%} of observations report zero volume, which usually means "
                "a non-trading day was returned as a trading day",
                Severity.WARNING,
                column="volume",
                n_offending=n_zero,
                zero_share=round(ratio, 4),
            )
        )
    else:
        results.append(ok("volume_zero_share", f"zero-volume share {ratio:.2%} is plausible"))
    return results


def check_extreme_returns(
    frame: pd.DataFrame,
    threshold: float,
    entity_column: str = "ticker",
    price_column: str | None = None,
    date_column: str = "date",
) -> CheckResult:
    """Flag implausibly large single-day moves, the usual signature of a bad split
    adjustment or a mis-scaled price."""
    price = price_column or ("adj_close" if "adj_close" in frame.columns else "close")
    if price not in frame.columns or entity_column not in frame.columns:
        return ok("extreme_returns", "no price column; check skipped")

    ordered = frame.sort_values([entity_column, date_column])
    values = pd.to_numeric(ordered[price], errors="coerce")
    previous = values.groupby(ordered[entity_column]).shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        log_return = np.log(values / previous)
    extreme = log_return.abs() > threshold
    n = int(extreme.fillna(False).sum())
    if n:
        examples = (
            pd.DataFrame(
                {
                    entity_column: ordered.loc[extreme, entity_column],
                    date_column: ordered.loc[extreme, date_column].astype(str),
                    "log_return": log_return[extreme].round(4),
                }
            )
            .head(5)
            .to_dict(orient="records")
        )
        return fail(
            "extreme_returns",
            f"{n} daily moves exceed |log return| > {threshold}",
            Severity.WARNING,
            column=price,
            n_offending=n,
            threshold=threshold,
            examples=examples,
        )
    return ok("extreme_returns", f"no daily move exceeds |log return| > {threshold}")


def check_value_missingness(
    frame: pd.DataFrame,
    entity_column: str,
    value_column: str = "value",
    error_ratio: float = 0.05,
) -> CheckResult:
    """Per-series missingness, evaluated relative to the series' own length.

    A macro series where 30% of observations failed to parse is broken even though the
    dataset as a whole may look fine, so the ratio is computed per indicator.
    """
    if value_column not in frame.columns or entity_column not in frame.columns:
        return ok("value_missingness", "columns absent; check skipped")

    per_series: dict[str, dict[str, object]] = {}
    worst = 0.0
    for entity, group in frame.groupby(entity_column, sort=True):
        n_missing = int(group[value_column].isna().sum())
        if not n_missing:
            continue
        ratio = n_missing / max(len(group), 1)
        per_series[str(entity)] = {
            "missing": n_missing,
            "n_observations": int(len(group)),
            "missing_ratio": round(ratio, 4),
        }
        worst = max(worst, ratio)

    if not per_series:
        return ok("value_missingness", "no missing values")
    severity = Severity.ERROR if worst > error_ratio else Severity.WARNING
    return fail(
        "value_missingness",
        f"{len(per_series)} series contain missing values (worst share {worst:.2%})",
        severity,
        column=value_column,
        n_offending=sum(int(v["missing"]) for v in per_series.values()),
        per_series=per_series,
        error_ratio=error_ratio,
    )

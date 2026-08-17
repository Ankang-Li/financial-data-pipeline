"""Cross-source consistency.

The premise of this project in one check: a successful HTTP 200 is not evidence that a
number is right. The only cheap way to catch a systematic error in a public feed is to
obtain the same economic quantity from an independent source and compare.

The US 10-year Treasury yield is available three times in this pipeline:

* Yahoo ``^TNX`` — quoted in tenths of a percent, so 4.25% arrives as 42.5
* FRED ``DGS10`` — quoted in percent
* AKShare ``bond_zh_us_rate`` — the US 10y as republished in China, in percent

If the unit rescaling in normalization were ever removed or applied twice, the median
difference between sources would jump by an order of magnitude and this check fails with
``ERROR`` rather than quietly feeding a model a yield ten times too large.
"""

from __future__ import annotations

from itertools import combinations

import numpy as np
import pandas as pd

from pipeline.validation.base import CheckResult, Severity, fail, ok

# Below this many shared observations a comparison is not informative.
MIN_OVERLAP = 20
# Share of days allowed to breach the tolerance before the pair is flagged.
MAX_BREACH_SHARE = 0.10
# A median gap this many times the tolerance almost certainly means a unit mismatch.
UNIT_MISMATCH_FACTOR = 20.0


def _to_basis_points(values: pd.Series, unit: str) -> pd.Series:
    """Express a difference in basis points when the unit allows it."""
    return values * 100.0 if unit == "percent" else values


def check_cross_source_consistency(
    macro: pd.DataFrame,
    tolerance_bp: float,
    *,
    date_column: str = "date",
    entity_column: str = "indicator",
    value_column: str = "value",
    source_column: str = "source",
    unit_column: str = "unit",
) -> list[CheckResult]:
    """Compare every indicator that is observed by more than one source."""
    required = {date_column, entity_column, value_column, source_column}
    if not required.issubset(macro.columns):
        return [fail("cross_source_inputs", "macro frame lacks the required columns", Severity.ERROR)]

    results: list[CheckResult] = []
    compared = 0

    for indicator, group in macro.groupby(entity_column, sort=True):
        sources = sorted(group[source_column].dropna().unique().tolist())
        if len(sources) < 2:
            continue

        units = sorted(group[unit_column].dropna().unique().tolist()) if unit_column in group else []
        if len(units) > 1:
            results.append(
                fail(
                    "cross_source_units",
                    f"{indicator} is reported in inconsistent units across sources: {units}",
                    Severity.ERROR,
                    column=str(indicator),
                    n_offending=len(units),
                    units=units,
                    sources=sources,
                )
            )
            continue
        unit = units[0] if units else "unknown"

        wide = group.pivot_table(
            index=date_column, columns=source_column, values=value_column, aggfunc="last"
        ).sort_index()

        for left, right in combinations(sources, 2):
            if left not in wide.columns or right not in wide.columns:
                continue
            pair = wide.loc[:, [left, right]].dropna()
            compared += 1
            if len(pair) < MIN_OVERLAP:
                results.append(
                    ok(
                        "cross_source_consistency",
                        f"{indicator}: {left} vs {right} share only {len(pair)} observations, "
                        "too few to compare",
                        indicator=str(indicator),
                        pair=[left, right],
                        overlap=int(len(pair)),
                    )
                )
                continue

            difference = _to_basis_points(pair[left] - pair[right], unit).abs()
            median_abs = float(difference.median())
            p95_abs = float(difference.quantile(0.95))
            max_abs = float(difference.max())
            breach_share = float((difference > tolerance_bp).mean())
            correlation = float(pair[left].corr(pair[right])) if len(pair) > 2 else np.nan

            details = {
                "indicator": str(indicator),
                "pair": [left, right],
                "unit": unit,
                "overlap": int(len(pair)),
                "median_abs_diff_bp": round(median_abs, 4),
                "p95_abs_diff_bp": round(p95_abs, 4),
                "max_abs_diff_bp": round(max_abs, 4),
                "breach_share": round(breach_share, 4),
                "correlation": None if np.isnan(correlation) else round(correlation, 6),
                "tolerance_bp": tolerance_bp,
            }

            if median_abs > tolerance_bp * UNIT_MISMATCH_FACTOR:
                results.append(
                    fail(
                        "cross_source_consistency",
                        f"{indicator}: {left} vs {right} differ by a median of {median_abs:.1f}bp, "
                        "which is too large to be a data revision — check unit normalization",
                        Severity.ERROR,
                        column=str(indicator),
                        n_offending=int((difference > tolerance_bp).sum()),
                        **details,
                    )
                )
            elif median_abs > tolerance_bp or breach_share > MAX_BREACH_SHARE:
                results.append(
                    fail(
                        "cross_source_consistency",
                        f"{indicator}: {left} vs {right} disagree on "
                        f"{breach_share:.1%} of {len(pair)} shared days "
                        f"(median {median_abs:.2f}bp, tolerance {tolerance_bp:.1f}bp)",
                        Severity.WARNING,
                        column=str(indicator),
                        n_offending=int((difference > tolerance_bp).sum()),
                        **details,
                    )
                )
            else:
                results.append(
                    ok(
                        "cross_source_consistency",
                        f"{indicator}: {left} vs {right} agree within {tolerance_bp:.1f}bp on "
                        f"{len(pair)} shared days (median {median_abs:.2f}bp)",
                        **details,
                    )
                )

    if not compared:
        results.append(
            ok(
                "cross_source_consistency",
                "no indicator is currently covered by two or more sources",
            )
        )
    return results


def coverage_summary(macro: pd.DataFrame) -> pd.DataFrame:
    """Per indicator and source: observation count and date range.

    Used by the CLI and the docs to show which series are multi-sourced.
    """
    grouped = macro.groupby(["indicator", "source"], sort=True)
    summary = grouped.agg(
        n_observations=("value", "size"),
        n_missing=("value", lambda s: int(s.isna().sum())),
        first_date=("date", "min"),
        last_date=("date", "max"),
    ).reset_index()
    return summary

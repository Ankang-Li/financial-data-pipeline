"""Validation must catch real defects. These tests feed constructed frames to the
individual checks so the routing rules are unambiguous: which defect raises ERROR (and
would quarantine a dataset) versus WARNING (which loads but is recorded).
"""

from __future__ import annotations

import pandas as pd

from pipeline.validation import numeric_checks as numeric
from pipeline.validation import timeseries_checks as ts
from pipeline.validation.base import Severity
from pipeline.validation.cross_source import check_cross_source_consistency
from pipeline.validation.numeric_checks import check_value_missingness


def _ohlc(high_below_low: bool = False) -> pd.DataFrame:
    rows = [
        {"date": "2020-01-02", "ticker": "SPY", "open": 100, "high": 102, "low": 99, "close": 101},
        {"date": "2020-01-03", "ticker": "SPY", "open": 101, "high": 103, "low": 100, "close": 102},
    ]
    if high_below_low:
        rows[1]["high"] = 98  # high below low -> impossible bar
    return pd.DataFrame(rows)


def test_ohlc_violation_is_error():
    report = numeric.check_ohlc_consistency(_ohlc(high_below_low=True))
    assert not report.passed
    assert report.severity is Severity.ERROR


def test_ohlc_clean_passes():
    report = numeric.check_ohlc_consistency(_ohlc())
    assert report.passed


def test_positive_prices_flags_non_positive():
    frame = _ohlc()
    frame.loc[0, "close"] = 0.0
    report = numeric.check_positive_prices(frame)
    assert any(not r.passed and r.severity is Severity.ERROR for r in report)


def test_duplicate_timestamps_is_error():
    frame = _ohlc()
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    report = ts.check_duplicate_timestamps(frame, "ticker")
    assert not report.passed and report.severity is Severity.ERROR


def test_value_missingness_error_above_threshold():
    frame = pd.DataFrame(
        {
            "indicator": ["X"] * 100,
            "value": [1.0] * 90 + [float("nan")] * 10,
        }
    )
    report = check_value_missingness(frame, "indicator", "value", error_ratio=0.05)
    assert not report.passed and report.severity is Severity.ERROR


def test_value_missingness_warning_below_threshold():
    frame = pd.DataFrame(
        {
            "indicator": ["X"] * 100,
            "value": [1.0] * 99 + [float("nan")],
        }
    )
    report = check_value_missingness(frame, "indicator", "value", error_ratio=0.05)
    assert not report.passed and report.severity is Severity.WARNING


def _cross_frame(scale: float = 1.0, unit: str = "percent") -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=40, freq="B").normalize()
    return pd.DataFrame(
        {
            "date": list(dates) * 2,
            "indicator": ["US_TREASURY_10Y"] * 80,
            "source": ["fred"] * 40 + ["akshare"] * 40,
            "value": [4.25] * 40 + [4.25 * scale] * 40,
            "unit": [unit] * 40 + [unit] * 40,
        }
    )


def test_cross_source_catches_unit_mismatch_by_magnitude():
    # AKShare 100x too large but same declared unit -> huge median diff -> ERROR.
    report = check_cross_source_consistency(_cross_frame(scale=100.0), tolerance_bp=5.0)
    errors = [r for r in report if not r.passed and r.severity is Severity.ERROR]
    assert errors, "expected an ERROR for a 100x value mismatch"


def test_cross_source_flags_inconsistent_unit_strings():
    # Same numbers, but the two sources declare different units -> cross_source_units ERROR.
    frame = _cross_frame(scale=1.0, unit="percent")
    frame.loc[frame["source"] == "akshare", "unit"] = "tenths_of_percent"
    report = check_cross_source_consistency(frame, tolerance_bp=5.0)
    assert any(not r.passed and r.check == "cross_source_units" for r in report)


def test_cross_source_ok_when_sources_agree():
    report = check_cross_source_consistency(_cross_frame(scale=1.0), tolerance_bp=5.0)
    assert all(r.passed for r in report if r.check == "cross_source_consistency")

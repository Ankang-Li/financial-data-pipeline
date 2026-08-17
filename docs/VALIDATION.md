# Validation catalog

Validation is the core feature of the pipeline. Every check is recorded (never just used to
drop rows), and each carries a severity that decides routing:

- `ERROR` → the dataset is **quarantined** and never loaded.
- `WARNING` → the dataset loads, but the issue is stored in `validation_results`.
- `INFO` → context only.

Checks run at three stages: `raw` (structural, against the registered `RawSchema`),
`normalized` (full canonical contract + time-series + financial logic), and `cross_source`
(consistency between independent sources of the same economic quantity).

## Raw stage

| Check | Severity if failed | Catches |
|-------|--------------------|---------|
| `payload_not_empty` | ERROR | source returned zero rows |
| `duplicate_columns` | ERROR | duplicated column labels |
| `raw_required_columns` | ERROR | source dropped a required column (schema drift) |
| `raw_optional_columns` | WARNING | optional column absent |
| `raw_unexpected_columns` | WARNING | provider added a column without notice |
| `min_rows` | WARNING | fewer rows than `min_expected_rows` |

## Normalized stage — schema / contract

| Check | Severity if failed |
|-------|--------------------|
| `canonical_columns` | ERROR (missing required) / WARNING (extra) |
| `dtypes` | ERROR (dtype family mismatch) |
| `nullability` | ERROR if missing ratio > `missing_ratio_error`, else WARNING |
| `primary_key_unique` | ERROR |
| `record_contract` | ERROR (Pydantic sample of 500 rows) |

## Normalized stage — time series

| Check | Severity if failed |
|-------|--------------------|
| `timestamp_timezone` | ERROR (tz-aware dates shift trading days) |
| `duplicate_timestamps` | ERROR |
| `monotonic_timestamps` | WARNING (out-of-order delivery) |
| `calendar_gaps` | WARNING if gap > 5 business days or missing share > 5% |
| `frequency_consistency` | WARNING (spacing changed) |
| `stale_values` | WARNING (run > 10 identical observations) |

## Normalized stage — financial logic

| Check | Severity if failed |
|-------|--------------------|
| `positive_prices` | ERROR (open/high/low/close ≤ 0) |
| `finite_values` | ERROR (infinite) |
| `ohlc_consistency` | ERROR (high below open/close/low, or low above) |
| `volume_non_negative` | ERROR |
| `volume_zero_share` | WARNING if > 10% zero |
| `extreme_returns` | WARNING if single-day `|log return|` > `extreme_return_threshold` (0.25) |
| `value_missingness` | ERROR if per-series missing > 5%, else WARNING |

## Cross-source consistency

`check_cross_source_consistency` compares every indicator observed by two or more sources
(here, the US 10y Treasury yield from Yahoo `^TNX`, FRED `DGS10` and AKShare). For each
source pair it reports the median and 95th-percentile absolute difference in basis points.

- If the median difference exceeds `tolerance_bp` (default 5bp) by a factor of 20×, it is
  treated as a **unit mismatch** (`ERROR`) — the classic "yield quoted in tenths of a
  percent / basis points" bug. This is what the committed sample triggers via AKShare.
- If units differ in name across sources (`cross_source_units`), that is an immediate `ERROR`.
- Smaller disagreements are `WARNING`.

Thresholds (`FDP_CROSS_SOURCE_TOLERANCE_BP`, `FDP_MISSING_RATIO_ERROR`,
`FDP_EXTREME_RETURN_THRESHOLD`, `FDP_MIN_EXPECTED_ROWS`) are environment-tunable and their
values are stored in the run's config fingerprint for reproducibility.

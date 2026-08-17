# Architecture

This document expands the architecture section of the README with the concrete module map
and the data contracts.

## Module map

```
src/pipeline/
├── config.py              # FDP_* env config, filesystem layout (Paths), Settings.fingerprint
├── schemas.py             # canonical TableSchema + version, RawSchema (drift), Pydantic records
├── dtypes.py              # dtype families + coercion (coerce_date / coerce_numeric / coerce_string)
├── provenance.py          # RunContext, checksums, UTC timestamps
├── raw/store.py           # RawStore (append-only write) + Snapshot (manifest, verify)
├── ingestion/
│   ├── base.py            # SourceAdapter ABC, FetchRequest/Result
│   ├── registry.py        # (source, dataset, mode) -> adapter
│   ├── yahoo.py           # market_prices + ^TNX yield index (live)
│   ├── fred.py            # macro series (live, needs key)
│   ├── akshare_cn.py      # China yield curve (live)
│   └── offline.py         # replay committed samples
├── normalization/
│   ├── identifiers.py     # INSTRUMENTS / INDICATORS / SERIES_MAP / INSTRUMENT_SYMBOL_MAP
│   ├── units.py           # SOURCE_UNIT_TO_CANONICAL, convert()
│   └── normalizer.py      # raw snapshots -> canonical frames
├── validation/
│   ├── base.py            # Severity, CheckResult, ValidationReport
│   ├── schema_checks.py   # raw drift + canonical contract
│   ├── timeseries_checks.py
│   ├── numeric_checks.py
│   ├── cross_source.py    # the multi-source consistency check
│   └── runner.py          # which checks run at which stage
├── storage/warehouse.py   # DuckDB: schema, idempotent load, metadata, validation log
├── queries/
│   ├── loaders.py         # load_dataset / query / named_query / list_datasets / report
│   └── sql/               # price_panel.sql, us10y_cross_source.sql, validation_summary.sql
├── run.py                 # orchestration: wire every stage + routing rules
└── cli.py                 # `fdp` command
```

## Data contracts

### Raw layer (immutable)

```
data/raw/<source>/<dataset>/retrieved_date=YYYY-MM-DD/<snapshot_id>.csv
                                                       <snapshot_id>.manifest.json
```

The manifest records `retrieved_at`, `row_count`, `column_dtypes`, a SHA-256 `checksum`,
fetch `params`, `run_id`, and `synthetic`. `Snapshot.verify()` re-checks the checksum.

### Canonical tables

- **`market_prices`** — daily OHLCV, one row per `(date, ticker, source)`.
  Primary key `(date, ticker, source)`.
- **`macro_data`** — long-format observations, one row per `(date, indicator, source)`.
  Primary key `(date, indicator, source)`. The `unit` column records the normalized unit
  (percent / index / ratio) so queries can never mix conventions.

### Warehouse side tables

- **`dataset_metadata`** — per `(dataset, source, run)` load log: row count, date range,
  schema version, config fingerprint.
- **`validation_results`** — one row per check per run (dataset, source, stage, severity,
  offending count, message, details JSON).

## Routing rules (run.py)

1. Raw payload fails structural validation → skipped (logged).
2. Normalized table raises `ERROR` → **quarantined** to `data/quarantine`, not loaded.
3. `WARNING`s load but are recorded.
4. Cross-source consistency is checked **after** all usable sources are loaded; it is
   reported (and may be `ERROR`) but never un-loads data — it is analyst signal.

## Why offline-first

`OfflineReplayAdapter` serves committed synthetic snapshots, so CI, the demo and the tests
exercise the *exact same code paths* as live ingestion. The only difference is the adapter.
This keeps the repository runnable with no network, no API key and no rate limits, and makes
the test suite hermetic.

"""DuckDB research warehouse.

A single file, ``data/warehouse/research.duckdb``, holds three kinds of tables:

* the two canonical research tables (``market_prices``, ``macro_data``) — one row per
  observation, source-tagged so a query can pin or ignore a provenance;
* ``dataset_metadata`` — when and how each (dataset, source) partition was loaded, so a
  number is always reproducible from a run id;
* ``validation_results`` — every check executed, passed or failed, stored next to the
  data it judged.

Writes are *idempotent per source*: re-running the pipeline for Yahoo replaces the
Yahoo partition rather than appending duplicates. Within a partition, primary keys are
de-duplicated before insert so a noisy source cannot break the load.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from pipeline.config import Settings, get_settings
from pipeline.logging_utils import get_logger
from pipeline.provenance import RunContext
from pipeline.schemas import CANONICAL_SCHEMAS, DUCKDB_TYPES, TableSchema, get_schema
from pipeline.validation.base import ValidationReport

logger = get_logger(__name__)

_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS dataset_metadata (
  dataset       VARCHAR NOT NULL,
  source        VARCHAR NOT NULL,
  schema_version VARCHAR NOT NULL,
  run_id        VARCHAR NOT NULL,
  loaded_at_utc TIMESTAMP NOT NULL,
  row_count     BIGINT NOT NULL,
  date_min      DATE,
  date_max      DATE,
  config_fingerprint VARCHAR
);
"""

_VALIDATION_DDL = """
CREATE TABLE IF NOT EXISTS validation_results (
  run_id      VARCHAR NOT NULL,
  dataset     VARCHAR NOT NULL,
  source      VARCHAR NOT NULL,
  stage       VARCHAR NOT NULL,
  check_name  VARCHAR NOT NULL,
  column_name VARCHAR,
  passed      BOOLEAN NOT NULL,
  severity    VARCHAR NOT NULL,
  n_offending BIGINT,
  message     VARCHAR,
  details_json VARCHAR,
  created_at  TIMESTAMP NOT NULL
);
"""


def _table_ddl(schema: TableSchema) -> str:
    lines = []
    for column in schema.columns:
        null = "" if column.nullable else " NOT NULL"
        lines.append(f"  {column.name} {DUCKDB_TYPES[column.dtype]}{null}")
    pk = ", ".join(schema.primary_key)
    cols = ",\n".join(lines)
    return (
        f"CREATE TABLE IF NOT EXISTS {schema.name} (\n"
        f"{cols},\n  PRIMARY KEY ({pk})\n);"
    )


class Warehouse:
    """Thin, dependency-light wrapper around a DuckDB file."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(self.path))

    # ------------------------------------------------------------------- schema management
    def ensure_schema(self) -> None:
        for schema in CANONICAL_SCHEMAS.values():
            self._con.execute(_table_ddl(schema))
        self._con.execute(_METADATA_DDL)
        self._con.execute(_VALIDATION_DDL)
        logger.info("warehouse schema ensured at %s", self.path)

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> Warehouse:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # --------------------------------------------------------------------------- loading
    def load_dataset(
        self,
        dataset: str,
        frame: pd.DataFrame,
        *,
        run: RunContext,
        schema_version: str | None = None,
        config_fingerprint: str | None = None,
    ) -> int:
        """Idempotently replace the data for every source present in ``frame``.

        Returns the number of rows written.
        """
        schema = get_schema(dataset)
        if frame.empty:
            logger.info("warehouse load skipped: %s is empty", dataset)
            return 0

        sources = [str(s) for s in frame["source"].dropna().unique()]
        staged = self._to_staged(frame, schema)
        staged = staged.drop_duplicates(subset=list(schema.primary_key))

        placeholders = ", ".join(["?"] * len(sources))
        self._con.execute(
            f"DELETE FROM {dataset} WHERE source IN ({placeholders})", sources
        )
        self._con.register("_staged", staged)
        self._con.execute(f"INSERT INTO {dataset} SELECT * FROM _staged")
        self._con.unregister("_staged")

        self._persist_metadata(
            dataset=dataset,
            sources=sources,
            frame=staged,
            run=run,
            schema_version=schema_version or schema.version,
            config_fingerprint=config_fingerprint or run.config_fingerprint,
        )
        logger.info("warehouse loaded %s rows=%d sources=%s", dataset, len(staged), sources)
        return int(len(staged))

    @staticmethod
    def _to_staged(frame: pd.DataFrame, schema: TableSchema) -> pd.DataFrame:
        """Cast to the exact column order/type the table expects."""
        staged = frame[[c.name for c in schema.columns]].copy()
        if "date" in staged.columns:
            # pandas keeps dates as datetimes; DuckDB DATE wants real dates.
            staged["date"] = staged["date"].dt.date
        return staged

    def _persist_metadata(
        self,
        *,
        dataset: str,
        sources: list[str],
        frame: pd.DataFrame,
        run: RunContext,
        schema_version: str,
        config_fingerprint: str,
    ) -> None:
        loaded_at = run.started_at
        date_min = frame["date"].min()
        date_max = frame["date"].max()
        rows = [
            {
                "dataset": dataset,
                "source": src,
                "schema_version": schema_version,
                "run_id": run.run_id,
                "loaded_at_utc": loaded_at,
                "row_count": int((frame["source"] == src).sum()),
                "date_min": date_min,
                "date_max": date_max,
                "config_fingerprint": config_fingerprint,
            }
            for src in sources
        ]
        self._con.register("_meta", pd.DataFrame(rows))
        self._con.execute("INSERT INTO dataset_metadata SELECT * FROM _meta")
        self._con.unregister("_meta")

    def persist_validation(self, report: ValidationReport) -> None:
        frame = report.to_frame()
        if frame.empty:
            return
        self._con.register("_val", frame)
        self._con.execute("INSERT INTO validation_results SELECT * FROM _val")
        self._con.unregister("_val")

    # ---------------------------------------------------------------------------- reading
    def query(self, sql: str, params: list | dict | None = None) -> pd.DataFrame:
        return self._con.execute(sql, params).df()

    def row_count(self, dataset: str) -> int:
        return int(self._con.execute(f"SELECT COUNT(*) FROM {dataset}").fetchone()[0])

    def tables(self) -> list[str]:
        return [r[0] for r in self._con.execute("SHOW TABLES").fetchall()]


def open_warehouse(
    settings: Settings | None = None, *, warehouse_path: str | Path | None = None
) -> Warehouse:
    settings = settings or get_settings()
    path = Path(warehouse_path) if warehouse_path else settings.paths.warehouse_path
    wh = Warehouse(path)
    wh.ensure_schema()
    return wh

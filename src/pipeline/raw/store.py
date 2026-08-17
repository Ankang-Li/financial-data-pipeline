"""The raw layer.

Rules of this layer, and the reason the pipeline has one at all:

* **Append-only.** A snapshot file is never rewritten. Re-running ingestion on the
  same day creates a new snapshot next to the old one.
* **Text-faithful.** Payloads are stored as the source produced them: original column
  names (including Chinese labels from AKShare), original missing markers (FRED's
  "."), no type inference. Everything downstream can therefore be recomputed from the
  raw layer alone.
* **Self-describing.** Each payload has a manifest recording who fetched it, when, with
  which parameters, how many rows, the observed dtypes and a SHA-256 checksum.

Layout::

    data/raw/<source>/<dataset>/retrieved_date=YYYY-MM-DD/<snapshot_id>.csv
                                                          <snapshot_id>.manifest.json
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.dtypes import dtype_family
from pipeline.logging_utils import get_logger
from pipeline.provenance import RunContext, file_checksum, stable_hash, utc_now

logger = get_logger(__name__)

MANIFEST_SUFFIX = ".manifest.json"


@dataclass(frozen=True)
class Snapshot:
    """One immutable raw payload plus its manifest."""

    snapshot_id: str
    source: str
    dataset: str
    payload_path: Path
    manifest_path: Path
    retrieved_at: dt.datetime
    row_count: int
    columns: tuple[str, ...]
    column_dtypes: dict[str, str]
    checksum_sha256: str
    params: dict[str, Any] = field(default_factory=dict)
    run_id: str | None = None
    pipeline_version: str | None = None
    mode: str | None = None
    synthetic: bool = False
    replayed_from: str | None = None
    notes: str = ""

    @classmethod
    def from_manifest(cls, manifest_path: Path) -> Snapshot:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        directory = manifest_path.parent
        return cls(
            snapshot_id=payload["snapshot_id"],
            source=payload["source"],
            dataset=payload["dataset"],
            payload_path=directory / payload["payload_file"],
            manifest_path=manifest_path,
            retrieved_at=dt.datetime.fromisoformat(payload["retrieved_at"]),
            row_count=int(payload["row_count"]),
            columns=tuple(payload["columns"]),
            column_dtypes=dict(payload.get("column_dtypes", {})),
            checksum_sha256=payload["checksum_sha256"],
            params=dict(payload.get("params", {})),
            run_id=payload.get("run_id"),
            pipeline_version=payload.get("pipeline_version"),
            mode=payload.get("mode"),
            synthetic=bool(payload.get("synthetic", False)),
            replayed_from=payload.get("replayed_from"),
            notes=payload.get("notes", ""),
        )

    def to_manifest_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "dataset": self.dataset,
            "payload_file": self.payload_path.name,
            "retrieved_at": self.retrieved_at.isoformat(),
            "row_count": self.row_count,
            "columns": list(self.columns),
            "column_dtypes": self.column_dtypes,
            "checksum_sha256": self.checksum_sha256,
            "params": self.params,
            "run_id": self.run_id,
            "pipeline_version": self.pipeline_version,
            "mode": self.mode,
            "synthetic": self.synthetic,
            "replayed_from": self.replayed_from,
            "notes": self.notes,
        }

    def load_frame(self) -> pd.DataFrame:
        """Read the payload back as text.

        Everything is read as ``string`` on purpose: the raw layer must not depend on
        pandas' type inference, otherwise a column silently changes type when the
        source starts padding numbers or emits an empty file.
        """
        frame = pd.read_csv(self.payload_path, dtype="string", keep_default_na=False)
        return frame

    def verify(self) -> bool:
        """Re-check the payload checksum, i.e. prove the snapshot was not edited."""
        return file_checksum(self.payload_path) == self.checksum_sha256


class RawStore:
    """Reader/writer for the raw layer."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    # ---------------------------------------------------------------- writing

    def write(
        self,
        *,
        source: str,
        dataset: str,
        frame: pd.DataFrame,
        run: RunContext,
        params: dict[str, Any] | None = None,
        synthetic: bool = False,
        replayed_from: str | None = None,
        notes: str = "",
        retrieved_at: dt.datetime | None = None,
    ) -> Snapshot:
        """Persist a payload and its manifest; returns the resulting snapshot."""
        params = dict(params or {})
        stamp = retrieved_at or utc_now()
        snapshot_id = "-".join(
            [
                source,
                dataset,
                f"{stamp:%Y%m%dT%H%M%SZ}",
                stable_hash({"params": params, "rows": int(len(frame)), "run": run.run_id})[:8],
            ]
        )
        directory = self.root / source / dataset / f"retrieved_date={stamp:%Y-%m-%d}"
        directory.mkdir(parents=True, exist_ok=True)
        payload_path = directory / f"{snapshot_id}.csv"
        if payload_path.exists():  # pragma: no cover - snapshot ids embed a timestamp
            raise FileExistsError(f"refusing to overwrite raw snapshot {payload_path}")

        frame.to_csv(payload_path, index=False)
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            source=source,
            dataset=dataset,
            payload_path=payload_path,
            manifest_path=directory / f"{snapshot_id}{MANIFEST_SUFFIX}",
            retrieved_at=stamp,
            row_count=int(len(frame)),
            columns=tuple(str(c) for c in frame.columns),
            column_dtypes={str(c): dtype_family(frame[c]) for c in frame.columns},
            checksum_sha256=file_checksum(payload_path),
            params=params,
            run_id=run.run_id,
            pipeline_version=run.pipeline_version,
            mode=run.mode,
            synthetic=synthetic,
            replayed_from=replayed_from,
            notes=notes,
        )
        snapshot.manifest_path.write_text(
            json.dumps(snapshot.to_manifest_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        logger.info(
            "raw snapshot written source=%s dataset=%s rows=%d id=%s",
            source,
            dataset,
            snapshot.row_count,
            snapshot_id,
        )
        return snapshot

    # ---------------------------------------------------------------- reading

    def list_snapshots(self, source: str | None = None, dataset: str | None = None) -> list[Snapshot]:
        pattern = f"{source or '*'}/{dataset or '*'}/retrieved_date=*/*{MANIFEST_SUFFIX}"
        manifests = sorted(self.root.glob(pattern))
        return [Snapshot.from_manifest(path) for path in manifests]

    def latest(self, source: str, dataset: str) -> Snapshot | None:
        snapshots = self.list_snapshots(source, dataset)
        if not snapshots:
            return None
        return max(snapshots, key=lambda s: (s.retrieved_at, s.snapshot_id))

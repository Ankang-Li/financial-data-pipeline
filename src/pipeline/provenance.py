"""Run identity and provenance primitives.

Reproducibility in research data work is mostly bookkeeping: knowing which code,
which configuration and which source snapshot produced a given number. Everything
in the pipeline is tagged with a ``RunContext``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from pipeline import __version__

Mode = Literal["offline", "live"]


def utc_now() -> dt.datetime:
    """Timezone-aware current UTC time (never naive, never local)."""
    return dt.datetime.now(dt.timezone.utc)


def stable_hash(payload: Any) -> str:
    """Deterministic short hash of any JSON-serializable payload."""
    blob = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def file_checksum(path: Any) -> str:
    """SHA-256 of a file, used to prove a raw snapshot has not been altered."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class RunContext:
    """Identity of a single pipeline execution."""

    run_id: str
    started_at: dt.datetime
    pipeline_version: str
    mode: Mode
    config_fingerprint: str
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def new(cls, mode: Mode, config: dict[str, Any] | None = None, **params: Any) -> RunContext:
        started = utc_now()
        return cls(
            run_id=f"{started:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}",
            started_at=started,
            pipeline_version=__version__,
            mode=mode,
            config_fingerprint=stable_hash(config or {}),
            params=dict(params),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "pipeline_version": self.pipeline_version,
            "mode": self.mode,
            "config_fingerprint": self.config_fingerprint,
            "params": self.params,
        }

"""Source adapter interface.

Every source implements the same three-method contract, which is what allows the
rest of the pipeline — and the research code above it — to stay source-agnostic:

* ``availability()`` — can this adapter run here and now (client installed, key present)?
* ``fetch(request)`` — return a payload plus the exact parameters used to obtain it.
* ``raw_schema`` — the shape the payload is expected to have, for drift detection.

Adapters deliberately do **not** clean, rename or retype anything. Their only job is
to obtain bytes and describe them honestly.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from pipeline.schemas import RawSchema, get_raw_schema


@dataclass(frozen=True)
class FetchRequest:
    """What to fetch: symbols/series identifiers plus an inclusive date window."""

    dataset: str
    symbols: tuple[str, ...]
    start: dt.date
    end: dt.date
    extra: dict[str, Any] = field(default_factory=dict)

    def as_params(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "symbols": list(self.symbols),
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            **self.extra,
        }


@dataclass
class FetchResult:
    """A raw payload and everything needed to reproduce or audit it."""

    frame: pd.DataFrame
    params: dict[str, Any]
    synthetic: bool = False
    replayed_from: str | None = None
    notes: str = ""


@dataclass(frozen=True)
class Availability:
    ok: bool
    reason: str = ""


class SourceAdapter(ABC):
    """Base class for all ingestion adapters."""

    name: str = ""
    dataset: str = ""
    description: str = ""
    requires_api_key: bool = False

    @property
    def key(self) -> str:
        return f"{self.name}:{self.dataset}"

    @property
    def raw_schema(self) -> RawSchema | None:
        return get_raw_schema(self.name, self.dataset)

    def availability(self) -> Availability:
        """Whether this adapter can run in the current environment."""
        return Availability(True)

    @abstractmethod
    def fetch(self, request: FetchRequest) -> FetchResult:
        """Obtain the payload. Must not mutate values or rename columns."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<{type(self).__name__} {self.key}>"

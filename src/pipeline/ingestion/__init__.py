"""Ingestion layer: one uniform interface per source, no cleaning, no renaming."""

from pipeline.ingestion.base import (
    Availability,
    FetchRequest,
    FetchResult,
    SourceAdapter,
)
from pipeline.ingestion.plan import DEFAULT_PLAN, IngestionSpec, plan_for
from pipeline.ingestion.registry import LIVE_ADAPTERS, get_adapter, list_adapters

__all__ = [
    "Availability",
    "DEFAULT_PLAN",
    "FetchRequest",
    "FetchResult",
    "IngestionSpec",
    "LIVE_ADAPTERS",
    "SourceAdapter",
    "get_adapter",
    "list_adapters",
    "plan_for",
]

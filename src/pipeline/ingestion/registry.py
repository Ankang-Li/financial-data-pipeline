"""Adapter registry: resolves (source, dataset, mode) to a concrete adapter."""

from __future__ import annotations

from pipeline.config import get_settings
from pipeline.ingestion.akshare_cn import AkshareMacroAdapter
from pipeline.ingestion.base import SourceAdapter
from pipeline.ingestion.fred import FredMacroAdapter
from pipeline.ingestion.offline import OfflineReplayAdapter
from pipeline.ingestion.yahoo import YahooMarketPricesAdapter, YahooYieldIndexAdapter

LIVE_ADAPTERS: dict[tuple[str, str], type[SourceAdapter]] = {
    ("yahoo", "market_prices"): YahooMarketPricesAdapter,
    ("yahoo", "macro_data"): YahooYieldIndexAdapter,
    ("fred", "macro_data"): FredMacroAdapter,
    ("akshare", "macro_data"): AkshareMacroAdapter,
}


def get_adapter(source: str, dataset: str, mode: str = "offline") -> SourceAdapter:
    """Return the adapter for a (source, dataset) pair in the requested mode."""
    if mode == "offline":
        return OfflineReplayAdapter(source, dataset, get_settings().paths.sample_raw_dir)
    if mode != "live":
        raise ValueError(f"unknown ingestion mode {mode!r}; expected 'offline' or 'live'")
    try:
        adapter_cls = LIVE_ADAPTERS[(source, dataset)]
    except KeyError as exc:
        known = ", ".join(f"{s}:{d}" for s, d in sorted(LIVE_ADAPTERS))
        raise KeyError(f"no live adapter for {source}:{dataset}; known adapters: {known}") from exc
    return adapter_cls()


def list_adapters() -> list[dict[str, object]]:
    """Describe every live adapter and whether it can run in this environment."""
    rows: list[dict[str, object]] = []
    for (source, dataset), adapter_cls in sorted(LIVE_ADAPTERS.items()):
        adapter = adapter_cls()
        availability = adapter.availability()
        rows.append(
            {
                "source": source,
                "dataset": dataset,
                "description": adapter.description,
                "requires_api_key": adapter.requires_api_key,
                "available": availability.ok,
                "reason": availability.reason,
            }
        )
    return rows

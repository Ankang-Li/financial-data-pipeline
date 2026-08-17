"""Research-oriented financial data infrastructure.

The public surface is deliberately small. Downstream research code should not
need to know whether a series came from Yahoo Finance, FRED or AKShare:

    from pipeline import load_dataset, query, run_pipeline

Attributes are resolved lazily so that importing the package stays cheap and
free of circular imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "load_dataset",
    "query",
    "run_pipeline",
    "list_datasets",
    "dataset_metadata",
    "validation_report",
]

_LAZY: dict[str, tuple[str, str]] = {
    "load_dataset": ("pipeline.queries.loaders", "load_dataset"),
    "query": ("pipeline.queries.loaders", "query"),
    "list_datasets": ("pipeline.queries.loaders", "list_datasets"),
    "dataset_metadata": ("pipeline.queries.loaders", "dataset_metadata"),
    "validation_report": ("pipeline.queries.loaders", "validation_report"),
    "run_pipeline": ("pipeline.run", "run_pipeline"),
}


def __getattr__(name: str) -> Any:
    target = _LAZY.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target
    from importlib import import_module

    return getattr(import_module(module_name), attr)


def __dir__() -> list[str]:
    return sorted(__all__)


if TYPE_CHECKING:  # pragma: no cover - import-time convenience for type checkers
    from pipeline.queries.loaders import (
        dataset_metadata,
        list_datasets,
        load_dataset,
        query,
        validation_report,
    )
    from pipeline.run import run_pipeline

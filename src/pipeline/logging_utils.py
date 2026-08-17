"""Logging setup shared by the CLI, the orchestrator and the examples."""

from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False
_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-28s | %(message)s"


def configure_logging(level: str | int | None = None) -> None:
    """Configure root logging once. Level can be overridden with ``FDP_LOG_LEVEL``."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    resolved = level or os.environ.get("FDP_LOG_LEVEL", "INFO")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(resolved)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)

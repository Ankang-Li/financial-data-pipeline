"""FRED adapter (live mode only; needs a free API key in ``FDP_FRED_API_KEY``).

Called over plain HTTP rather than through a client library: the endpoint is one URL,
and depending on ``fredapi`` would add a dependency whose failure modes we would then
have to document anyway.

Note the raw payload keeps FRED's "." missing marker verbatim. Turning it into NaN is a
normalization decision, and doing it here would destroy the evidence that the source
reported a gap rather than a zero.
"""

from __future__ import annotations

import time
from typing import Any

import pandas as pd
import requests

from pipeline.config import get_settings
from pipeline.ingestion.base import Availability, FetchRequest, FetchResult, SourceAdapter
from pipeline.logging_utils import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
RAW_COLUMNS = ["date", "series_id", "value"]


class FredMacroAdapter(SourceAdapter):
    name = "fred"
    dataset = "macro_data"
    description = "Federal Reserve Economic Data series observations"
    requires_api_key = True

    def availability(self) -> Availability:
        if get_settings().fred_api_key is None:
            return Availability(False, "FDP_FRED_API_KEY is not set")
        return Availability(True)

    def fetch(self, request: FetchRequest) -> FetchResult:
        settings = get_settings()
        api_key = settings.fred_api_key
        if api_key is None:
            raise RuntimeError("FDP_FRED_API_KEY is required for live FRED ingestion")

        rows: list[dict[str, Any]] = []
        for series_id in request.symbols:
            observations = self._get_series(
                series_id=series_id,
                api_key=api_key,
                start=request.start.isoformat(),
                end=request.end.isoformat(),
                timeout=settings.http_timeout,
                retries=settings.max_retries,
            )
            logger.info("fred fetch series=%s observations=%d", series_id, len(observations))
            for observation in observations:
                rows.append(
                    {
                        "date": observation.get("date"),
                        "series_id": series_id,
                        # Kept as text, including "." for missing.
                        "value": observation.get("value"),
                    }
                )

        payload = pd.DataFrame(rows, columns=RAW_COLUMNS)
        params = request.as_params()
        params["endpoint"] = BASE_URL
        return FetchResult(frame=payload, params=params)

    @staticmethod
    def _get_series(
        *,
        series_id: str,
        api_key: str,
        start: str,
        end: str,
        timeout: float,
        retries: int,
    ) -> list[dict[str, Any]]:
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": start,
            "observation_end": end,
        }
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(BASE_URL, params=params, timeout=timeout)
                response.raise_for_status()
                return list(response.json().get("observations", []))
            except Exception as exc:  # noqa: BLE001 - retried and re-raised below
                last_error = exc
                backoff = min(2 ** (attempt - 1), 8)
                logger.warning(
                    "fred request failed series=%s attempt=%d/%d: %s (retrying in %ss)",
                    series_id,
                    attempt,
                    retries,
                    exc,
                    backoff,
                )
                time.sleep(backoff)
        raise RuntimeError(f"FRED request failed for {series_id}") from last_error

"""AKShare adapter for Chinese macro / rates data (live mode only, optional extra).

Scope is deliberately limited to macro and yield-curve series, so this project does not
duplicate the market-data path already covered by Yahoo.

Two properties of this source are worth keeping in mind, and are the reason the
validation layer exists:

* Column labels are Chinese and can be renamed upstream without notice — exactly the
  schema-drift case the pipeline is built to detect.
* ``bond_zh_us_rate`` publishes the Chinese *and* the US 10-year yield in the same
  table, which gives the pipeline a third, independent observation of the US 10y.
"""

from __future__ import annotations

import pandas as pd

from pipeline.ingestion.base import Availability, FetchRequest, FetchResult, SourceAdapter
from pipeline.logging_utils import get_logger

logger = get_logger(__name__)

# Canonical identifier -> column label as published by AKShare.
SERIES_COLUMNS: dict[str, str] = {
    "CN2Y": "中国国债收益率2年",
    "CN5Y": "中国国债收益率5年",
    "CN10Y": "中国国债收益率10年",
    "CN30Y": "中国国债收益率30年",
    "US10Y": "美国国债收益率10年",
}
DATE_COLUMN = "日期"


class AkshareMacroAdapter(SourceAdapter):
    name = "akshare"
    dataset = "macro_data"
    description = "China government bond yield curve (China Bond / AKShare)"

    def availability(self) -> Availability:
        try:
            import akshare  # noqa: F401
        except ImportError:
            return Availability(False, "akshare is not installed (pip install '.[live]')")
        return Availability(True)

    def fetch(self, request: FetchRequest) -> FetchResult:
        import akshare as ak

        logger.info("akshare fetch bond_zh_us_rate from %s", request.start)
        frame = ak.bond_zh_us_rate(start_date=request.start.strftime("%Y%m%d"))
        if frame is None or frame.empty:
            return FetchResult(
                frame=pd.DataFrame(columns=[DATE_COLUMN, *SERIES_COLUMNS.values()]),
                params=request.as_params(),
                notes="empty payload",
            )

        # Keep the published column labels; only restrict to the requested series so the
        # snapshot stays small. Unknown identifiers are ignored rather than fabricated.
        wanted = [SERIES_COLUMNS[s] for s in request.symbols if s in SERIES_COLUMNS]
        keep = [DATE_COLUMN, *[c for c in wanted if c in frame.columns]]
        payload = frame.loc[:, keep].copy()

        dates = pd.to_datetime(payload[DATE_COLUMN], errors="coerce")
        mask = (dates >= pd.Timestamp(request.start)) & (dates <= pd.Timestamp(request.end))
        payload = payload.loc[mask].reset_index(drop=True)

        params = request.as_params()
        params["akshare_function"] = "bond_zh_us_rate"
        return FetchResult(frame=payload, params=params)

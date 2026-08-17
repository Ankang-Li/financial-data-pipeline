"""Offline replay adapter.

Replays a committed sample snapshot as if it had just been fetched. This is what makes
the repository runnable with no network access, no API key and no rate limits: the
demo, the tests and CI all exercise the *same* code path as live ingestion, only the
adapter differs.

The replayed payload keeps its ``synthetic`` flag and records which snapshot it came
from, so a research dataset built offline can never be mistaken for real market data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.ingestion.base import Availability, FetchRequest, FetchResult, SourceAdapter
from pipeline.logging_utils import get_logger
from pipeline.raw.store import RawStore

logger = get_logger(__name__)

# Wide-format sources need column-level filtering instead of row-level filtering.
_WIDE_COLUMN_MAP: dict[str, dict[str, str]] = {
    "akshare": {
        "CN2Y": "中国国债收益率2年",
        "CN5Y": "中国国债收益率5年",
        "CN10Y": "中国国债收益率10年",
        "CN30Y": "中国国债收益率30年",
        "US10Y": "美国国债收益率10年",
    }
}
_SYMBOL_COLUMNS = ("ticker", "series_id")


class OfflineReplayAdapter(SourceAdapter):
    """Serves a payload from ``data/sample/raw`` instead of calling a remote API."""

    def __init__(self, source: str, dataset: str, sample_root: Path) -> None:
        self.name = source
        self.dataset = dataset
        self.description = f"offline replay of committed {source} sample snapshots"
        self._store = RawStore(sample_root)

    def availability(self) -> Availability:
        snapshot = self._store.latest(self.name, self.dataset)
        if snapshot is None:
            return Availability(
                False,
                f"no sample snapshot for {self.key} under {self._store.root}; "
                "run scripts/generate_sample_data.py",
            )
        return Availability(True)

    def fetch(self, request: FetchRequest) -> FetchResult:
        snapshot = self._store.latest(self.name, self.dataset)
        if snapshot is None:
            raise FileNotFoundError(f"no sample snapshot available for {self.key}")
        if not snapshot.verify():
            raise ValueError(
                f"sample snapshot {snapshot.snapshot_id} failed its checksum; the file was edited"
            )

        frame = snapshot.load_frame()
        frame = self._filter_symbols(frame, request)
        logger.info(
            "offline replay %s rows=%d from snapshot=%s",
            self.key,
            len(frame),
            snapshot.snapshot_id,
        )
        params = request.as_params()
        params["replayed_from"] = snapshot.snapshot_id
        params["sample_root"] = str(self._store.root)
        return FetchResult(
            frame=frame,
            params=params,
            synthetic=snapshot.synthetic,
            replayed_from=snapshot.snapshot_id,
            notes=snapshot.notes,
        )

    def _filter_symbols(self, frame: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        if not request.symbols:
            return frame
        wide_map = _WIDE_COLUMN_MAP.get(self.name)
        if wide_map:
            wanted = [wide_map[s] for s in request.symbols if s in wide_map]
            keep = [c for c in frame.columns if c not in wide_map.values() or c in wanted]
            return frame.loc[:, keep]
        for column in _SYMBOL_COLUMNS:
            if column in frame.columns:
                return frame.loc[frame[column].isin(request.symbols)].reset_index(drop=True)
        return frame

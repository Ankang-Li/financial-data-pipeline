"""Configuration and filesystem layout.

Every tunable is read from the environment (prefix ``FDP_``) with a sane default,
so a fresh clone runs the offline demo without any configuration at all.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

ENV_PREFIX = "FDP_"


def _repo_root() -> Path:
    """Locate the repository root by walking up to the directory holding pyproject.toml.

    Falls back to the current working directory when the package is installed
    outside a source checkout (e.g. from a wheel).
    """
    here = Path(__file__).resolve()
    for candidate in (here, *here.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return Path.cwd()


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(ENV_PREFIX + name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    return int(_env_float(name, float(default)))


@dataclass(frozen=True)
class Paths:
    """Filesystem layout of the four data layers plus generated artifacts."""

    repo_root: Path
    data_dir: Path

    @property
    def raw_dir(self) -> Path:
        """Immutable, as-returned source snapshots."""
        return self.data_dir / "raw"

    @property
    def normalized_dir(self) -> Path:
        """Validated and normalized parquet files (the pre-warehouse handoff)."""
        return self.data_dir / "normalized"

    @property
    def quarantine_dir(self) -> Path:
        """Datasets rejected by validation; kept for inspection, never loaded."""
        return self.data_dir / "quarantine"

    @property
    def warehouse_dir(self) -> Path:
        return self.data_dir / "warehouse"

    @property
    def warehouse_path(self) -> Path:
        """Single-file DuckDB research database."""
        return self.warehouse_dir / "research.duckdb"

    @property
    def sample_raw_dir(self) -> Path:
        """Committed synthetic snapshots used by the offline mode and the tests."""
        override = _env("SAMPLE_RAW_DIR")
        if override:
            return Path(override).expanduser().resolve()
        return self.repo_root / "data" / "sample" / "raw"

    @property
    def artifacts_dir(self) -> Path:
        """Charts and markdown tables produced by examples/."""
        return self.repo_root / "artifacts"

    def ensure(self) -> None:
        for path in (
            self.raw_dir,
            self.normalized_dir,
            self.quarantine_dir,
            self.warehouse_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Settings:
    """Runtime settings; the fingerprint of these values is stored with every run."""

    paths: Paths
    fred_api_key: str | None
    http_timeout: float
    max_retries: int
    # Validation thresholds
    cross_source_tolerance_bp: float
    missing_ratio_error: float
    extreme_return_threshold: float
    min_expected_rows: int

    def fingerprint(self) -> dict[str, object]:
        """Validation-relevant configuration, recorded for reproducibility.

        The FRED key is represented as a boolean only; secrets never reach the
        warehouse or the logs.
        """
        return {
            "cross_source_tolerance_bp": self.cross_source_tolerance_bp,
            "missing_ratio_error": self.missing_ratio_error,
            "extreme_return_threshold": self.extreme_return_threshold,
            "min_expected_rows": self.min_expected_rows,
            "fred_api_key_present": self.fred_api_key is not None,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings. Call ``get_settings.cache_clear()`` after changing env vars."""
    root = _repo_root()
    load_dotenv(root / ".env", override=False)

    data_dir_raw = _env("DATA_DIR")
    data_dir = Path(data_dir_raw).expanduser().resolve() if data_dir_raw else root / "data"

    return Settings(
        paths=Paths(repo_root=root, data_dir=data_dir),
        fred_api_key=_env("FRED_API_KEY"),
        http_timeout=_env_float("HTTP_TIMEOUT", 30.0),
        max_retries=_env_int("MAX_RETRIES", 3),
        cross_source_tolerance_bp=_env_float("CROSS_SOURCE_TOLERANCE_BP", 5.0),
        missing_ratio_error=_env_float("MISSING_RATIO_ERROR", 0.05),
        extreme_return_threshold=_env_float("EXTREME_RETURN_THRESHOLD", 0.25),
        min_expected_rows=_env_int("MIN_EXPECTED_ROWS", 20),
    )

"""A real research example built on the warehouse.

This script is the proof that the pipeline produces something a quant or an ML engineer
can actually use: it reconstructs the US Treasury yield curve from three public sources,
demonstrates the cross-source consistency check on live (synthetic) numbers, and builds a
return panel from the price table — all through the public query API, with no knowledge of
where the data came from.

Run after ``python scripts/generate_sample_data.py`` (already committed):

    PYTHONPATH=src python examples/research_example.py

Charts land in ``artifacts/``.
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")  # headless / CI friendly
import matplotlib.pyplot as plt  # noqa: E402

from pipeline import load_dataset, run_pipeline, validation_report  # noqa: E402
from pipeline.config import get_settings  # noqa: E402
from pipeline.logging_utils import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


def _us10y_panel() -> None:
    frame = load_dataset("macro_data", indicators=["US_TREASURY_10Y"])
    if frame.empty:
        logger.warning("US_TREASURY_10Y has no rows; skipping chart")
        return
    pivot = frame.pivot_table(index="date", columns="source", values="value", aggfunc="last")
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for source in pivot.columns:
        ax.plot(pivot.index, pivot[source], label=source, linewidth=1.2)
    ax.set_title("US 10-year Treasury yield by source (normalized to percent)")
    ax.set_ylabel("yield (%)")
    ax.legend()
    ax.grid(alpha=0.3)
    out = get_settings().paths.artifacts_dir / "us10y_by_source.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120)
    plt.close(fig)
    logger.info("wrote %s", out)


def _price_returns() -> None:
    frame = load_dataset("market_prices", tickers=["SPY", "TLT", "GLD"])
    if frame.empty:
        logger.warning("market_prices is empty; skipping chart")
        return
    pivot = frame.pivot_table(index="date", columns="ticker", values="close", aggfunc="last")
    returns = pivot.pct_change().dropna()
    cumulative = (1 + returns).cumprod() - 1

    fig, ax = plt.subplots(figsize=(9, 4.5))
    for ticker in cumulative.columns:
        ax.plot(cumulative.index, cumulative[ticker], label=ticker, linewidth=1.2)
    ax.set_title("Cumulative returns from the normalized price table")
    ax.set_ylabel("cumulative return")
    ax.legend()
    ax.grid(alpha=0.3)
    out = get_settings().paths.artifacts_dir / "price_returns.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=120)
    plt.close(fig)
    logger.info("wrote %s", out)


def main() -> None:
    configure_logging()
    # Build (or refresh) the warehouse from the committed synthetic samples.
    run = run_pipeline(mode="offline", quiet=True)
    print("\n" + run.summary())

    _us10y_panel()
    _price_returns()

    # Surface the validation findings for this run next to the data they judged.
    report = validation_report(run_id=run.run_id)
    if not report.empty:
        flagged = report[~report["passed"]]
        print(f"\nValidation findings: {len(flagged)} non-passing checks")
        print(flagged[["dataset", "source", "stage", "check_name", "severity", "message"]]
              .head(20)
              .to_string(index=False))


if __name__ == "__main__":
    main()

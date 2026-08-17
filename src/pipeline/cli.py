"""Command-line interface: ``fdp``.

Thin wrapper over the public API so the pipeline can be driven from a shell:

    fdp run                       # build the warehouse from committed samples (offline)
    fdp run --mode live           # re-ingest from the real sources (needs extras + keys)
    fdp sources                   # show which adapters can run in this environment
    fdp load macro_data --indicators US_TREASURY_10Y
    fdp query "SELECT * FROM price_panel"   # named query
    fdp report                    # validation outcome of the most recent run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline import __version__
from pipeline.config import get_settings
from pipeline.ingestion.registry import list_adapters
from pipeline.logging_utils import configure_logging
from pipeline.queries.loaders import (
    dataset_metadata,
    list_datasets,
    load_dataset,
    named_query,
    query,
    validation_report,
)
from pipeline.run import run_pipeline


def _cmd_run(args: argparse.Namespace) -> int:
    configure_logging()
    result = run_pipeline(
        mode=args.mode,
        sources=tuple(args.sources) if args.sources else None,
        start=_parse_date(args.start),
        end=_parse_date(args.end),
    )
    print("\n" + result.summary())
    if result.quarantined:
        print("Quarantined (not loaded):")
        for dataset, source in result.quarantined:
            print(f"  - {dataset}/{source}")
    return 0


def _cmd_sources(_args: argparse.Namespace) -> int:
    rows = list_adapters()
    print(f"{'source':10} {'dataset':14} {'available':9} description")
    for row in rows:
        print(
            f"{row['source']:10} {row['dataset']:14} "
            f"{'yes' if row['available'] else 'no':9} {row['description']}"
        )
        if not row["available"]:
            print(f"           -> {row['reason']}")
    return 0


def _cmd_load(args: argparse.Namespace) -> int:
    frame = load_dataset(
        args.dataset,
        indicators=args.indicators,
        tickers=args.tickers,
        sources=args.sources,
        start=args.start,
        end=args.end,
    )
    print(frame.to_string(index=False) if not frame.empty else f"{args.dataset} is empty")
    return 0


def _cmd_query(args: argparse.Namespace) -> int:
    text = args.sql
    frame = named_query(text) if _is_named(text) else query(text)
    print(frame.to_string(index=False) if not frame.empty else "no rows")
    return 0


def _cmd_report(_args: argparse.Namespace) -> int:
    settings = get_settings()
    wh_path = settings.paths.warehouse_path
    if not Path(wh_path).exists():
        print("no warehouse yet — run `fdp run` first")
        return 1
    frame = validation_report(warehouse_path=wh_path)
    if frame.empty:
        print("no validation results recorded")
        return 0
    wanted = ("run_id", "dataset", "source", "stage", "check_name", "passed", "severity", "message")
    cols = [c for c in wanted if c in frame]
    print(frame[cols].to_string(index=False))
    return 0


def _cmd_datasets(_args: argparse.Namespace) -> int:
    names = list_datasets()
    print("\n".join(names) if names else "(none loaded)")
    return 0


def _cmd_metadata(_args: argparse.Namespace) -> int:
    print(dataset_metadata().to_string(index=False))
    return 0


def _is_named(text: str) -> bool:
    return " " not in text and text.lower() in {"price_panel", "us10y_cross_source", "validation_summary"}


def _parse_date(value: str | None):
    if not value:
        return None
    import datetime as dt

    return dt.date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fdp", description="financial-data-pipeline CLI")
    parser.add_argument("--version", action="version", version=f"fdp {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run ingestion -> validation -> warehouse")
    p_run.add_argument("--mode", choices=["offline", "live"], default="offline")
    p_run.add_argument("--sources", nargs="+", choices=["yahoo", "fred", "akshare"])
    p_run.add_argument("--start", help="YYYY-MM-DD")
    p_run.add_argument("--end", help="YYYY-MM-DD")
    p_run.set_defaults(func=_cmd_run)

    p_sources = sub.add_parser("sources", help="list ingestion adapters and availability")
    p_sources.set_defaults(func=_cmd_sources)

    p_load = sub.add_parser("load", help="read a canonical table, filtered")
    p_load.add_argument("dataset", choices=["market_prices", "macro_data"])
    p_load.add_argument("--indicators", nargs="+")
    p_load.add_argument("--tickers", nargs="+")
    p_load.add_argument("--sources", nargs="+")
    p_load.add_argument("--start")
    p_load.add_argument("--end")
    p_load.set_defaults(func=_cmd_load)

    p_query = sub.add_parser("query", help="run a SQL or named query")
    p_query.add_argument("sql", help="SQL text or a named query (price_panel, us10y_cross_source)")
    p_query.set_defaults(func=_cmd_query)

    p_report = sub.add_parser("report", help="show validation results of the last run")
    p_report.set_defaults(func=_cmd_report)

    p_ds = sub.add_parser("datasets", help="list loaded datasets")
    p_ds.set_defaults(func=_cmd_datasets)

    p_md = sub.add_parser("metadata", help="show dataset load log")
    p_md.set_defaults(func=_cmd_metadata)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

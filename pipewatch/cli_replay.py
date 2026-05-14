"""CLI subcommand for replaying historical metrics."""
from __future__ import annotations

import argparse
import sys
from typing import Optional

from pipewatch.config import load_config
from pipewatch.history import MetricHistory
from pipewatch.replay import replay_history


def build_replay_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "replay",
        help="Replay historical metric data through alert thresholds",
    )
    p.add_argument("source", help="Source name to replay")
    p.add_argument("metric", help="Metric name to replay")
    p.add_argument("--warn", type=float, default=None, metavar="THRESHOLD",
                   help="Warning threshold")
    p.add_argument("--crit", type=float, default=None, metavar="THRESHOLD",
                   help="Critical threshold")
    p.add_argument("--limit", type=int, default=None,
                   help="Maximum number of most-recent entries to replay")
    p.add_argument("--history-file", default="pipewatch_history.json",
                   metavar="FILE", help="Path to history file")
    p.set_defaults(func=_run_replay)


def _run_replay(args: argparse.Namespace) -> int:
    history = MetricHistory(path=args.history_file)
    report = replay_history(
        source_name=args.source,
        history=history,
        metric_name=args.metric,
        warn_threshold=args.warn,
        crit_threshold=args.crit,
        limit=args.limit,
    )

    if report.total == 0:
        print(f"No history found for source='{args.source}' metric='{args.metric}'")
        return 0

    for event in report.events:
        ts = event.metric.timestamp.isoformat() if event.metric.timestamp else "?"
        print(f"{ts}  {event}")

    print()
    print(str(report))
    return 1 if report.alert_count > 0 else 0


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(prog="pipewatch-replay")
    subparsers = parser.add_subparsers(dest="command")
    build_replay_parser(subparsers)
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    sys.exit(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    main()

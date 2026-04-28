"""CLI entry point for pipewatch using argparse."""

import argparse
import sys
import logging
from pathlib import Path

from pipewatch.config import load_config
from pipewatch.runner import run_once, run_loop

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and alert on data pipeline health metrics.",
    )
    parser.add_argument(
        "-c", "--config",
        metavar="FILE",
        type=Path,
        default=None,
        help="Path to pipewatch YAML config file (default: auto-detect).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single collection cycle and exit.",
    )
    parser.add_argument(
        "--interval",
        metavar="SECONDS",
        type=int,
        default=None,
        help="Override the poll interval from config (seconds).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )
    return parser


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=level,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    try:
        config = load_config(args.config)
    except FileNotFoundError as exc:
        logger.error("Config file not found: %s", exc)
        return 1
    except ValueError as exc:
        logger.error("Invalid config: %s", exc)
        return 1

    if args.interval is not None:
        config = config.model_copy(update={"poll_interval": args.interval})

    if args.once:
        run_once(config)
        return 0

    try:
        run_loop(config)
    except KeyboardInterrupt:
        logger.info("Interrupted — shutting down.")

    return 0


if __name__ == "__main__":
    sys.exit(main())

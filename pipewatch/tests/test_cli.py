"""Tests for the pipewatch CLI module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli import build_parser, main


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

def test_parser_defaults():
    parser = build_parser()
    args = parser.parse_args([])
    assert args.config is None
    assert args.once is False
    assert args.interval is None
    assert args.verbose is False


def test_parser_once_flag():
    parser = build_parser()
    args = parser.parse_args(["--once"])
    assert args.once is True


def test_parser_config_short():
    parser = build_parser()
    args = parser.parse_args(["-c", "my.yml"])
    assert args.config == Path("my.yml")


def test_parser_interval():
    parser = build_parser()
    args = parser.parse_args(["--interval", "30"])
    assert args.interval == 30


def test_parser_verbose():
    parser = build_parser()
    args = parser.parse_args(["-v"])
    assert args.verbose is True


# ---------------------------------------------------------------------------
# main() integration tests (mocked)
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_config():
    cfg = MagicMock()
    cfg.poll_interval = 60
    cfg.model_copy.return_value = cfg
    return cfg


def test_main_once_returns_zero(fake_config):
    with patch("pipewatch.cli.load_config", return_value=fake_config), \
         patch("pipewatch.cli.run_once") as mock_run_once:
        result = main(["--once"])
    assert result == 0
    mock_run_once.assert_called_once_with(fake_config)


def test_main_config_not_found_returns_one():
    with patch("pipewatch.cli.load_config", side_effect=FileNotFoundError("missing")):
        result = main(["--once"])
    assert result == 1


def test_main_invalid_config_returns_one():
    with patch("pipewatch.cli.load_config", side_effect=ValueError("bad yaml")):
        result = main(["--once"])
    assert result == 1


def test_main_interval_override_applied(fake_config):
    with patch("pipewatch.cli.load_config", return_value=fake_config), \
         patch("pipewatch.cli.run_once"):
        main(["--once", "--interval", "15"])
    fake_config.model_copy.assert_called_once_with(update={"poll_interval": 15})


def test_main_keyboard_interrupt_returns_zero(fake_config):
    with patch("pipewatch.cli.load_config", return_value=fake_config), \
         patch("pipewatch.cli.run_loop", side_effect=KeyboardInterrupt):
        result = main([])
    assert result == 0

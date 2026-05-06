"""Tests for pipewatch.tagger."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.tagger import TagIndex, build_tag_index, filter_sources_by_tag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(name: str, tags=None):
    src = MagicMock()
    src.name = name
    src.tags = tags
    return src


# ---------------------------------------------------------------------------
# TagIndex unit tests
# ---------------------------------------------------------------------------

def test_sources_for_tag_unknown_returns_empty():
    idx = TagIndex()
    assert idx.sources_for_tag("missing") == set()


def test_add_and_retrieve_single_tag():
    idx = TagIndex()
    idx.add("src_a", ["prod"])
    assert idx.sources_for_tag("prod") == {"src_a"}


def test_add_multiple_sources_same_tag():
    idx = TagIndex()
    idx.add("src_a", ["prod"])
    idx.add("src_b", ["prod"])
    assert idx.sources_for_tag("prod") == {"src_a", "src_b"}


def test_sources_for_tags_any_match():
    idx = TagIndex()
    idx.add("src_a", ["prod"])
    idx.add("src_b", ["staging"])
    result = idx.sources_for_tags(["prod", "staging"], match_all=False)
    assert result == {"src_a", "src_b"}


def test_sources_for_tags_all_match():
    idx = TagIndex()
    idx.add("src_a", ["prod", "critical"])
    idx.add("src_b", ["prod"])
    result = idx.sources_for_tags(["prod", "critical"], match_all=True)
    assert result == {"src_a"}


def test_sources_for_tags_empty_input_returns_empty():
    idx = TagIndex()
    idx.add("src_a", ["prod"])
    assert idx.sources_for_tags([]) == set()


def test_all_tags_sorted():
    idx = TagIndex()
    idx.add("src_a", ["zebra", "alpha"])
    assert idx.all_tags() == ["alpha", "zebra"]


# ---------------------------------------------------------------------------
# build_tag_index
# ---------------------------------------------------------------------------

def test_build_tag_index_skips_sources_without_tags():
    sources = [_make_source("no_tags", tags=None), _make_source("with_tags", tags=["env:prod"])]
    index = build_tag_index(sources)
    assert "no_tags" not in index.sources_for_tag("env:prod")
    assert "with_tags" in index.sources_for_tag("env:prod")


def test_build_tag_index_empty_sources():
    index = build_tag_index([])
    assert index.all_tags() == []


# ---------------------------------------------------------------------------
# filter_sources_by_tag
# ---------------------------------------------------------------------------

def test_filter_sources_by_tag_returns_matching():
    sources = [
        _make_source("a", tags=["prod"]),
        _make_source("b", tags=["staging"]),
        _make_source("c", tags=["prod", "critical"]),
    ]
    result = filter_sources_by_tag(sources, ["prod"])
    names = [s.name for s in result]
    assert "a" in names
    assert "c" in names
    assert "b" not in names


def test_filter_sources_by_tag_no_match_returns_empty():
    sources = [_make_source("a", tags=["prod"])]
    result = filter_sources_by_tag(sources, ["nonexistent"])
    assert result == []

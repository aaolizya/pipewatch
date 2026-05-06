"""Tests for pipewatch.labeler."""
import pytest

from pipewatch.labeler import LabelSet, MetricLabeler


# ---------------------------------------------------------------------------
# LabelSet
# ---------------------------------------------------------------------------

def test_labelset_set_and_get():
    ls = LabelSet()
    ls.set("env", "prod")
    assert ls.get("env") == "prod"


def test_labelset_get_missing_returns_none():
    ls = LabelSet()
    assert ls.get("missing") is None


def test_labelset_empty_key_raises():
    ls = LabelSet()
    with pytest.raises(ValueError, match="empty"):
        ls.set("", "value")


def test_labelset_matches_true():
    ls = LabelSet()
    ls.set("tier", "gold")
    assert ls.matches("tier", "gold") is True


def test_labelset_matches_false_wrong_value():
    ls = LabelSet()
    ls.set("tier", "gold")
    assert ls.matches("tier", "silver") is False


def test_labelset_matches_false_missing_key():
    ls = LabelSet()
    assert ls.matches("tier", "gold") is False


def test_labelset_as_dict_copy():
    ls = LabelSet()
    ls.set("a", "1")
    d = ls.as_dict()
    d["b"] = "2"
    assert ls.get("b") is None  # original unaffected


def test_labelset_len():
    ls = LabelSet()
    assert len(ls) == 0
    ls.set("x", "y")
    assert len(ls) == 1


# ---------------------------------------------------------------------------
# MetricLabeler
# ---------------------------------------------------------------------------

@pytest.fixture()
def labeler() -> MetricLabeler:
    return MetricLabeler()


def test_label_and_retrieve(labeler):
    labeler.label("src1", "latency", "env", "prod")
    ls = labeler.labels_for("src1", "latency")
    assert ls.get("env") == "prod"


def test_labels_for_unknown_returns_empty_labelset(labeler):
    ls = labeler.labels_for("ghost", "metric")
    assert len(ls) == 0


def test_label_overwrite(labeler):
    labeler.label("src", "m", "env", "staging")
    labeler.label("src", "m", "env", "prod")
    assert labeler.labels_for("src", "m").get("env") == "prod"


def test_multiple_labels_same_pair(labeler):
    labeler.label("src", "m", "env", "prod")
    labeler.label("src", "m", "team", "infra")
    ls = labeler.labels_for("src", "m")
    assert ls.get("team") == "infra"


def test_find_by_label_returns_matching_pairs(labeler):
    labeler.label("src1", "cpu", "env", "prod")
    labeler.label("src2", "mem", "env", "prod")
    labeler.label("src3", "disk", "env", "staging")
    results = list(labeler.find_by_label("env", "prod"))
    assert ("src1", "cpu") in results
    assert ("src2", "mem") in results
    assert ("src3", "disk") not in results


def test_find_by_label_no_match_returns_empty(labeler):
    labeler.label("src", "m", "env", "prod")
    assert list(labeler.find_by_label("env", "dev")) == []


def test_remove_clears_labels(labeler):
    labeler.label("src", "m", "env", "prod")
    labeler.remove("src", "m")
    assert len(labeler.labels_for("src", "m")) == 0


def test_remove_noop_for_unknown(labeler):
    labeler.remove("ghost", "metric")  # should not raise


def test_all_pairs_returns_registered(labeler):
    labeler.label("s1", "a", "k", "v")
    labeler.label("s2", "b", "k", "v")
    pairs = list(labeler.all_pairs())
    assert ("s1", "a") in pairs
    assert ("s2", "b") in pairs

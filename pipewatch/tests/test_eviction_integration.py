"""Integration tests: LRUCache used as a metric-value cache."""
from datetime import datetime, timezone

from pipewatch.eviction import EvictionPolicy, LRUCache
from pipewatch.metrics import Metric


def _metric(name: str, value: float) -> Metric:
    return Metric(
        source="src",
        name=name,
        value=value,
        unit="count",
        timestamp=datetime.now(timezone.utc),
    )


def test_cache_stores_and_retrieves_metrics():
    cache: LRUCache[str, Metric] = LRUCache()
    m = _metric("queue_depth", 42.0)
    cache.put(m.name, m)
    result = cache.get(m.name)
    assert result is not None
    assert result.value == 42.0


def test_cache_evicts_stale_metrics_on_overflow():
    policy = EvictionPolicy(max_size=4, evict_ratio=0.5)
    cache: LRUCache[str, Metric] = LRUCache(policy)
    names = ["a", "b", "c", "d"]
    for n in names:
        cache.put(n, _metric(n, 1.0))
    # overflow: 2 oldest should be evicted
    cache.put("e", _metric("e", 5.0))
    assert len(cache) <= policy.max_size
    assert "a" not in cache
    assert "b" not in cache


def test_recently_accessed_metric_survives_eviction():
    policy = EvictionPolicy(max_size=3, evict_ratio=0.34)
    cache: LRUCache[str, Metric] = LRUCache(policy)
    cache.put("old", _metric("old", 0.0))
    cache.put("mid", _metric("mid", 1.0))
    cache.put("new", _metric("new", 2.0))
    # access "old" to promote it
    cache.get("old")
    cache.put("extra", _metric("extra", 3.0))
    assert "old" in cache
    assert "mid" not in cache


def test_cache_updates_metric_value_in_place():
    cache: LRUCache[str, Metric] = LRUCache()
    cache.put("lag", _metric("lag", 10.0))
    cache.put("lag", _metric("lag", 99.0))
    result = cache.get("lag")
    assert result is not None
    assert result.value == 99.0
    assert len(cache) == 1

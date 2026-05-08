"""Tests for pipewatch.eviction (EvictionPolicy + LRUCache)."""
import pytest

from pipewatch.eviction import EvictionPolicy, LRUCache


# ---------------------------------------------------------------------------
# EvictionPolicy
# ---------------------------------------------------------------------------

def test_policy_defaults():
    p = EvictionPolicy()
    assert p.max_size == 256
    assert p.evict_ratio == 0.25


def test_policy_invalid_max_size_raises():
    with pytest.raises(ValueError, match="max_size"):
        EvictionPolicy(max_size=0)


def test_policy_invalid_evict_ratio_zero_raises():
    with pytest.raises(ValueError, match="evict_ratio"):
        EvictionPolicy(evict_ratio=0.0)


def test_policy_invalid_evict_ratio_above_one_raises():
    with pytest.raises(ValueError, match="evict_ratio"):
        EvictionPolicy(evict_ratio=1.1)


def test_policy_evict_count_at_least_one():
    p = EvictionPolicy(max_size=1, evict_ratio=0.01)
    assert p.evict_count == 1


def test_policy_evict_count_fraction():
    p = EvictionPolicy(max_size=100, evict_ratio=0.25)
    assert p.evict_count == 25


# ---------------------------------------------------------------------------
# LRUCache
# ---------------------------------------------------------------------------

def test_cache_empty_get_returns_none():
    cache: LRUCache[str, int] = LRUCache()
    assert cache.get("missing") is None


def test_cache_put_and_get():
    cache: LRUCache[str, int] = LRUCache()
    cache.put("a", 1)
    assert cache.get("a") == 1


def test_cache_len():
    cache: LRUCache[str, int] = LRUCache()
    cache.put("x", 10)
    cache.put("y", 20)
    assert len(cache) == 2


def test_cache_contains():
    cache: LRUCache[str, int] = LRUCache()
    cache.put("k", 99)
    assert "k" in cache
    assert "z" not in cache


def test_cache_evicts_oldest_on_overflow():
    policy = EvictionPolicy(max_size=3, evict_ratio=0.34)
    cache: LRUCache[str, int] = LRUCache(policy)
    for i, key in enumerate(["a", "b", "c"]):
        cache.put(key, i)
    # inserting a 4th entry should evict the oldest ("a")
    cache.put("d", 3)
    assert "a" not in cache
    assert len(cache) <= policy.max_size


def test_cache_get_promotes_to_mru():
    policy = EvictionPolicy(max_size=3, evict_ratio=0.34)
    cache: LRUCache[str, int] = LRUCache(policy)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)
    # promote "a" so it is most-recently used
    cache.get("a")
    cache.put("d", 4)  # overflow – "b" should be evicted, not "a"
    assert "a" in cache
    assert "b" not in cache


def test_cache_update_existing_key():
    cache: LRUCache[str, int] = LRUCache()
    cache.put("k", 1)
    cache.put("k", 2)
    assert cache.get("k") == 2
    assert len(cache) == 1


def test_cache_clear():
    cache: LRUCache[str, int] = LRUCache()
    cache.put("a", 1)
    cache.clear()
    assert len(cache) == 0


def test_cache_items_returns_all():
    cache: LRUCache[str, int] = LRUCache()
    cache.put("a", 1)
    cache.put("b", 2)
    assert set(cache.items()) == {("a", 1), ("b", 2)}

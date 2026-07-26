"""Tests for the analysis cache: TTL expiry + LRU eviction (audit fix B8)."""

import time

from pspcz_analyzer.services.analysis_cache import AnalysisCache


class TestTTL:
    def test_hit_within_ttl(self, monkeypatch):
        """A fresh entry is served without recomputation."""
        now = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: now[0])
        cache = AnalysisCache(ttl=60, max_entries=10)
        calls = 0

        def _compute():
            nonlocal calls
            calls += 1
            return "v"

        assert cache.get_or_compute("k", _compute) == "v"
        now[0] += 30
        assert cache.get_or_compute("k", _compute) == "v"
        assert calls == 1

    def test_miss_after_ttl(self, monkeypatch):
        """An expired entry is recomputed."""
        now = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: now[0])
        cache = AnalysisCache(ttl=60, max_entries=10)
        calls = 0

        def _compute():
            nonlocal calls
            calls += 1
            return calls

        assert cache.get_or_compute("k", _compute) == 1
        now[0] += 61
        assert cache.get_or_compute("k", _compute) == 2


class TestLRU:
    def test_evicts_oldest_beyond_max(self):
        """Inserting past max_entries drops the least recently used entry."""
        cache = AnalysisCache(ttl=3600, max_entries=3)
        for i in range(4):
            cache.get_or_compute(f"k{i}", lambda i=i: i)

        assert cache.invalidate("k0") == 0  # evicted
        assert cache.invalidate("k3") == 1  # newest survives

    def test_read_refreshes_recency(self):
        """A cache hit marks the entry as recently used, protecting it from eviction."""
        cache = AnalysisCache(ttl=3600, max_entries=3)
        for i in range(3):
            cache.get_or_compute(f"k{i}", lambda i=i: i)

        cache.get_or_compute("k0", lambda: "unused")  # touch k0
        cache.get_or_compute("k3", lambda: 3)  # must evict k1, not k0

        assert cache.invalidate("k1") == 0
        assert cache.invalidate("k0") == 1

    def test_unbounded_keys_are_capped(self):
        """User-controlled keys (search strings) cannot grow the cache past the cap."""
        cache = AnalysisCache(ttl=3600, max_entries=5)
        for i in range(50):
            cache.get_or_compute(f"votes:search-{i}", lambda i=i: i)

        assert cache.invalidate() == 5


class TestInvalidate:
    def test_prefix_invalidation(self):
        """Prefix invalidation removes only matching keys."""
        cache = AnalysisCache(ttl=3600, max_entries=10)
        cache.get_or_compute("votes:1:a", lambda: 1)
        cache.get_or_compute("votes:1:b", lambda: 2)
        cache.get_or_compute("loyalty:1", lambda: 3)

        assert cache.invalidate("votes:") == 2
        assert cache.invalidate() == 1

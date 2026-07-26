"""In-memory TTL + LRU cache for expensive analysis computations."""

import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from loguru import logger

from pspcz_analyzer.config import ANALYSIS_CACHE_MAX_ENTRIES


class AnalysisCache:
    """Thread-safe dict cache with TTL expiry and LRU eviction.

    Entries expire ``ttl`` seconds after insertion. Independently, once the
    cache holds more than ``max_entries`` items the least recently used one
    is evicted — keys include user-controlled strings (free-text search on
    the votes endpoint), so without a cap the cache could grow without limit.
    """

    def __init__(self, ttl: int = 3600, max_entries: int = ANALYSIS_CACHE_MAX_ENTRIES) -> None:
        self._ttl = ttl
        self._max_entries = max(1, max_entries)
        self._store: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._lock = threading.Lock()

    def get_or_compute(self, key: str, compute_fn: Callable[[], Any]) -> Any:
        """Return the cached value for ``key``, computing and storing it on a miss.

        ``compute_fn`` runs outside the lock, so concurrent misses may compute
        the same value in parallel — last write wins, which is fine for pure
        analyses.
        """
        now = time.monotonic()
        with self._lock:
            if key in self._store:
                ts, value = self._store[key]
                if now - ts < self._ttl:
                    self._store.move_to_end(key)
                    logger.debug("Cache HIT: {}", key)
                    return value
                del self._store[key]

        logger.debug("Cache MISS: {}", key)
        value = compute_fn()

        with self._lock:
            self._store[key] = (time.monotonic(), value)
            self._store.move_to_end(key)
            self._evict_lru()
        return value

    def _evict_lru(self) -> None:
        """Drop least-recently-used entries beyond max_entries (lock held)."""
        while len(self._store) > self._max_entries:
            evicted, _ = self._store.popitem(last=False)
            logger.debug("Cache EVICT (LRU): {}", evicted)

    def invalidate(self, prefix: str = "") -> int:
        """Remove cached entries; all of them, or those starting with ``prefix``.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            if not prefix:
                n = len(self._store)
                self._store.clear()
                return n
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]
            return len(keys)


analysis_cache = AnalysisCache()

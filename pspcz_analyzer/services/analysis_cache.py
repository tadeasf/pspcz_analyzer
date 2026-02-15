"""In-memory TTL cache for expensive analysis computations."""

import threading
import time
from typing import Any, Callable

from loguru import logger


class AnalysisCache:
    """Thread-safe dict cache with TTL expiry."""

    def __init__(self, ttl: int = 3600):
        self._ttl = ttl
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get_or_compute(self, key: str, compute_fn: Callable[[], Any]) -> Any:
        now = time.monotonic()
        with self._lock:
            if key in self._store:
                ts, value = self._store[key]
                if now - ts < self._ttl:
                    logger.debug("Cache HIT: {}", key)
                    return value
                del self._store[key]

        logger.debug("Cache MISS: {}", key)
        value = compute_fn()

        with self._lock:
            self._store[key] = (time.monotonic(), value)
        return value

    def invalidate(self, prefix: str = "") -> int:
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

"""Tests for run_with_timeout thread-pool behavior (audit fix B10)."""

import threading

import pytest
from fastapi import HTTPException

from pspcz_analyzer.middleware import run_with_timeout


class TestRunWithTimeout:
    @pytest.mark.asyncio
    async def test_returns_result(self):
        """Fast computations return their value."""
        result = await run_with_timeout(lambda: 42, timeout=5.0, label="test")
        assert result == 42

    @pytest.mark.asyncio
    async def test_raises_503_on_timeout(self):
        """A computation exceeding the timeout surfaces as HTTP 503."""
        barrier = threading.Event()
        try:
            with pytest.raises(HTTPException) as exc_info:
                await run_with_timeout(barrier.wait, timeout=0.05, label="slow")
            assert exc_info.value.status_code == 503
        finally:
            barrier.set()

    @pytest.mark.asyncio
    async def test_timed_out_work_does_not_starve_pool(self):
        """Two stuck computations must not block subsequent requests.

        Regression: with the old hard-coded max_workers=2, two timed-out
        (but still running) workers occupied the entire pool and every later
        computation queued behind them. The pool is now sized with headroom,
        so the next request gets a worker immediately.
        """
        barrier = threading.Event()

        def _block():
            barrier.wait(timeout=10)

        try:
            for _ in range(2):
                with pytest.raises(HTTPException):
                    await run_with_timeout(_block, timeout=0.05, label="zombie")

            result = await run_with_timeout(lambda: "alive", timeout=5.0, label="next")
            assert result == "alive"
        finally:
            barrier.set()

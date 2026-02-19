"""Daily data refresh scheduler.

Pauses the tisk AI pipeline, re-downloads fresh data from psp.cz,
reloads all in-memory state, then restarts the pipeline.
Uses pure asyncio — no external scheduler dependencies.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from loguru import logger

from pspcz_analyzer.config import DAILY_REFRESH_ENABLED, DAILY_REFRESH_HOUR

if TYPE_CHECKING:
    from pspcz_analyzer.services.data_service import DataService

# CET is UTC+1 (fixed offset). DST shifts refresh by 1 hour,
# which is acceptable for a background maintenance task at 3 AM.
_CET_OFFSET_HOURS = 1


def _seconds_until_next_run(target_hour: int) -> float:
    """Calculate seconds until the next occurrence of *target_hour* in CET."""
    now_utc = datetime.now(tz=UTC)
    now_cet_hour = (now_utc.hour + _CET_OFFSET_HOURS) % 24
    now_cet_seconds_into_day = now_cet_hour * 3600 + now_utc.minute * 60 + now_utc.second
    target_seconds_into_day = target_hour * 3600
    diff = target_seconds_into_day - now_cet_seconds_into_day
    if diff <= 0:
        diff += 86400  # next day
    return float(diff)


class DailyRefreshService:
    """Asyncio-based daily scheduler that refreshes all psp.cz data."""

    def __init__(self, data_service: DataService, hour: int = DAILY_REFRESH_HOUR) -> None:
        self._data_service = data_service
        self._hour = hour
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        """Start the scheduler loop. Idempotent — does nothing if already running."""
        if not DAILY_REFRESH_ENABLED:
            logger.info("[daily-refresh] Scheduler disabled via DAILY_REFRESH_ENABLED=0")
            return

        if self._task is not None and not self._task.done():
            return

        self._task = asyncio.create_task(
            self._scheduler_loop(),
            name="daily-refresh-scheduler",
        )
        logger.info(
            "[daily-refresh] Scheduler started (refresh at {:02d}:00 CET daily)",
            self._hour,
        )

    async def stop(self) -> None:
        """Cancel the scheduler gracefully."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            logger.info("[daily-refresh] Scheduler stopped")
        self._task = None

    async def trigger_now(self) -> None:
        """Manually trigger an immediate refresh (for admin/debug use)."""
        logger.info("[daily-refresh] Manual refresh triggered")
        await self._data_service.refresh_all_data()

    async def _scheduler_loop(self) -> None:
        """Sleep until target hour, refresh, repeat."""
        try:
            while True:
                delay = _seconds_until_next_run(self._hour)
                logger.debug(
                    "[daily-refresh] Next refresh in {:.0f}s (~{:.1f}h)",
                    delay,
                    delay / 3600,
                )
                await asyncio.sleep(delay)
                try:
                    await self._data_service.refresh_all_data()
                except Exception:
                    logger.opt(exception=True).error(
                        "[daily-refresh] Refresh failed, retrying in 1 hour"
                    )
                    await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.debug("[daily-refresh] Scheduler loop cancelled")
            raise

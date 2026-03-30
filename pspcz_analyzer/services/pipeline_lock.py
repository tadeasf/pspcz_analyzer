"""Single-pipeline-at-a-time lock for protecting local LLM resources."""

import asyncio
from dataclasses import dataclass

from loguru import logger


@dataclass
class PipelineInfo:
    """Metadata about the currently running pipeline."""

    pipeline_id: str
    pipeline_type: str
    period: int


class PipelineLock:
    """Ensures only one pipeline runs at a time across the backend.

    Usage::

        lock = PipelineLock()
        if not await lock.acquire("tisk", 10):
            raise RuntimeError("Another pipeline is running")
        try:
            ...  # run pipeline
        finally:
            lock.release()
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._current: PipelineInfo | None = None

    @property
    def is_locked(self) -> bool:
        """Whether a pipeline is currently running."""
        return self._lock.locked()

    @property
    def current(self) -> PipelineInfo | None:
        """Info about the currently running pipeline, or None."""
        return self._current if self._lock.locked() else None

    async def acquire(self, pipeline_type: str, period: int) -> bool:
        """Try to acquire the lock for a pipeline.

        Args:
            pipeline_type: Type of pipeline (e.g. "tisk", "amendment").
            period: Electoral period number.

        Returns:
            True if lock acquired, False if another pipeline is running.
        """
        if self._lock.locked():
            logger.warning(
                "[pipeline-lock] Rejected {}/{}: already running {}",
                pipeline_type,
                period,
                self._current,
            )
            return False

        await self._lock.acquire()
        pipeline_id = f"{pipeline_type}:{period}"
        self._current = PipelineInfo(
            pipeline_id=pipeline_id,
            pipeline_type=pipeline_type,
            period=period,
        )
        logger.info("[pipeline-lock] Acquired: {}", pipeline_id)
        return True

    def release(self) -> None:
        """Release the pipeline lock."""
        if not self._lock.locked():
            return
        pipeline_id = self._current.pipeline_id if self._current else "unknown"
        self._current = None
        self._lock.release()
        logger.info("[pipeline-lock] Released: {}", pipeline_id)


# Module-level singleton
pipeline_lock = PipelineLock()

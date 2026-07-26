"""Cooperative cancellation primitives for background pipelines.

Pipeline bodies run in worker threads via ``asyncio.to_thread``, where
``asyncio.Task.cancel()`` cannot interrupt them — the awaiting coroutine
is released, but the thread itself keeps running to completion. Instead,
each running period owns a :class:`CancellationFlag`: HTTP handlers flip
it instantly, and pipeline code calls :meth:`CancellationFlag.check` at
every stage boundary and loop top, exiting promptly with
:class:`PipelineCancelled`.
"""

import threading


class PipelineCancelled(Exception):
    """Raised inside a pipeline when its period has been cancelled."""

    def __init__(self, period: int) -> None:
        """Store the cancelled period on the exception.

        Args:
            period: Electoral period whose pipeline was cancelled.
        """
        self.period = period
        super().__init__(f"Pipeline for period {period} cancelled")


class CancellationFlag:
    """Thread-safe cooperative cancellation flag for one pipeline period.

    ``cancel()`` may be called from any thread (typically an HTTP
    handler on the event loop); ``is_cancelled()`` / ``check()`` are
    read from worker threads at stage boundaries. The flag is one-shot
    and single-period: pipeline services create a fresh flag per run.
    """

    def __init__(self, period: int) -> None:
        """Create an uncancelled flag for a period.

        Args:
            period: Electoral period this flag governs.
        """
        self.period = period
        self._cancelled = False
        self._lock = threading.Lock()

    def cancel(self) -> None:
        """Request cancellation. Idempotent and thread-safe."""
        with self._lock:
            self._cancelled = True

    def is_cancelled(self) -> bool:
        """Whether cancellation has been requested."""
        return self._cancelled

    def check(self) -> None:
        """Raise PipelineCancelled if cancellation was requested.

        Called at stage boundaries and loop tops inside worker threads.
        Reads without the lock — bool reads are atomic under the GIL
        and this sits on the hot path of long-running loops.
        """
        if self._cancelled:
            raise PipelineCancelled(self.period)

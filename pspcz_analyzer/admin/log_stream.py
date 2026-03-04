"""SSE log broadcasting for real-time pipeline log streaming."""

import asyncio
import html
from collections import deque
from collections.abc import AsyncGenerator

from loguru import logger

_MAX_BUFFER = 500


class LogBroadcaster:
    """Captures loguru messages and streams them to SSE subscribers."""

    def __init__(self) -> None:
        self._buffer: deque[str] = deque(maxlen=_MAX_BUFFER)
        self._subscribers: list[asyncio.Queue[str]] = []
        self._sink_id: int | None = None

    def start(self) -> None:
        """Install loguru sink to capture log lines."""
        if self._sink_id is not None:
            return
        self._sink_id = logger.add(
            self._handle_log,
            format="{time:HH:mm:ss} | {level: <8} | {message}",
            level="INFO",
            filter=lambda record: any(
                tag in record["message"]
                for tag in (
                    "[tisk pipeline]",
                    "[amendment pipeline]",
                    "[daily-refresh]",
                    "[pipeline-lock]",
                    "[runtime-config]",
                    "[file-watcher]",
                )
            ),
        )
        logger.info("[log-stream] Broadcaster started")

    def stop(self) -> None:
        """Remove loguru sink."""
        if self._sink_id is not None:
            logger.remove(self._sink_id)
            self._sink_id = None

    def _handle_log(self, message: str) -> None:
        """Loguru sink callback — buffer + broadcast to subscribers."""
        line = message.strip()
        if not line:
            return
        self._buffer.append(line)
        dead: list[asyncio.Queue[str]] = []
        for q in self._subscribers:
            try:
                q.put_nowait(line)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self._subscribers.remove(q)

    async def subscribe(self) -> AsyncGenerator[str, None]:
        """Yield log lines as SSE events wrapped in divs. Sends buffered history first."""
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=200)
        self._subscribers.append(q)
        try:
            # Send buffered history
            for line in self._buffer:
                escaped = html.escape(line)
                yield f'data: <div class="log-line">{escaped}</div>\n\n'
            # Stream new lines
            while True:
                line = await q.get()
                escaped = html.escape(line)
                yield f'data: <div class="log-line">{escaped}</div>\n\n'
        finally:
            if q in self._subscribers:
                self._subscribers.remove(q)


# Module-level singleton
log_broadcaster = LogBroadcaster()

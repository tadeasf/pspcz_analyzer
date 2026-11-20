"""Is the LLM backend answering? Failure counting and a real test completion.

On 20 Nov 2026 the hlidacstatu proxy kept answering ``/models`` in 0.1 s while every chat
completion timed out; the pipelines went on and stored empty results. Now:

- ``is_available()`` sends a tiny completion (``probe_completion``) instead of listing models;
- each failed request (timeout, connection error, HTTP 5xx or 429) is logged at WARNING and
  counted across threads and clients; a successful reply resets the count;
- after ``LLM_MAX_CONSECUTIVE_FAILURES`` in a row the request raises ``LLMUnavailable``,
  which the pipelines re-raise so a run stops instead of continuing without the model.
"""

import threading
import time

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    LLM_ENABLE_THINKING,
    LLM_MAX_CONSECUTIVE_FAILURES,
    LLM_PROBE_TIMEOUT,
)


class LLMUnavailable(Exception):
    """The LLM backend failed LLM_MAX_CONSECUTIVE_FAILURES requests in a row."""


class FailureCounter:
    """Consecutive failed LLM requests, shared by all threads of the process."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._count = 0
        self._lock = threading.Lock()

    def ok(self) -> None:
        """A request got an HTTP answer: the backend is up."""
        with self._lock:
            self._count = 0

    def failed(self, prefix: str, reason: str) -> None:
        """Log a failed request and stop the run when too many failed in a row.

        Args:
            prefix: Log prefix of the client (provider).
            reason: Short description (exception type and message, or HTTP status).

        Raises:
            LLMUnavailable: This was the ``limit``-th failure in a row.
        """
        with self._lock:
            self._count += 1
            count = self._count
        logger.warning(
            "{} LLM request failed ({}/{} in a row): {}", prefix, count, self.limit, reason
        )
        if count >= self.limit:
            raise LLMUnavailable(f"{count} LLM requests in a row failed, last: {reason}")

    def reset(self) -> None:
        """Forget earlier failures (tests, a new run)."""
        self.ok()


failures = FailureCounter(LLM_MAX_CONSECUTIVE_FAILURES)


def is_outage(exc: Exception) -> bool:
    """Whether a request error means the backend is down (not a rejected prompt).

    Args:
        exc: The error of an ``httpx`` request.

    Returns:
        True for timeouts, connection errors, HTTP 5xx and 429; False for other 4xx.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code >= 500 or code == 429
    return isinstance(exc, httpx.TransportError)


def describe(exc: Exception) -> str:
    """Short reason for logs: exception type, HTTP status or message."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    return f"{type(exc).__name__}: {exc}"[:200]


def probe_completion(
    base_url: str, headers: dict[str, str], model: str, openai_compat: bool
) -> bool:
    """Ask the model for a one-word reply; any generated answer within the timeout is up.

    Args:
        base_url: API base URL (without trailing slash).
        headers: Request headers (auth).
        model: Model name.
        openai_compat: ``/chat/completions`` when True, native Ollama ``/api/generate`` else.

    Returns:
        True when the backend generated a reply within ``LLM_PROBE_TIMEOUT`` seconds.
    """
    if openai_compat:
        url = f"{base_url}/chat/completions"
        payload: dict = {
            "model": model,
            "messages": [{"role": "user", "content": "Odpověz jedním slovem: ahoj"}],
            "max_tokens": 8,
            "stream": False,
        }
        if LLM_ENABLE_THINKING:
            payload["chat_template_kwargs"] = {"enable_thinking": LLM_ENABLE_THINKING == "1"}
        key = "choices"
    else:
        url = f"{base_url}/api/generate"
        payload = {"model": model, "prompt": "ahoj", "stream": False, "options": {"num_predict": 8}}
        key = "response"
    start = time.monotonic()
    try:
        resp = httpx.post(url, headers=headers, json=payload, timeout=LLM_PROBE_TIMEOUT)
        resp.raise_for_status()
        answered = key in resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "[llm] test completion failed after {:.1f} s: {}",
            time.monotonic() - start,
            describe(exc),
        )
        return False
    logger.info("[llm] test completion answered in {:.1f} s", time.monotonic() - start)
    return answered

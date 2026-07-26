"""Security headers middleware and computation timeout helper."""

import asyncio
import contextvars
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any
from urllib.parse import urlparse

from fastapi import HTTPException
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from pspcz_analyzer.config import COMPUTE_POOL_WORKERS

# max_workers=None (COMPUTE_POOL_WORKERS=0) lets Python size the pool as
# min(32, cpu_count + 4). The previous hard-coded 2 made two slow/timed-out
# computations starve every other analysis request.
_compute_pool = ThreadPoolExecutor(max_workers=COMPUTE_POOL_WORKERS or None)


def is_same_origin(request: Request) -> bool:
    """Check that the request's Origin or Referer matches its own host.

    Used as CSRF protection for state-changing endpoints: browsers send
    Origin (or Referer) on cross-origin form posts, and a mismatch means
    the request was triggered by a foreign site.

    Args:
        request: Incoming request.

    Returns:
        True if Origin or Referer parses to the same hostname as the
        request URL. False when both headers are missing or unparseable
        (fail closed).
    """
    expected = request.url.hostname
    for header in ("origin", "referer"):
        value = request.headers.get(header)
        if value:
            try:
                return urlparse(value).hostname == expected
            except ValueError:
                return False
    return False


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        # Bill PDFs are embedded in our own law-detail page via a same-origin
        # iframe, so they must allow same-origin framing — the default DENY /
        # frame-ancestors 'none' would block our own embed.
        if request.url.path.startswith("/api/tisk-pdf/"):
            response.headers["X-Frame-Options"] = "SAMEORIGIN"
            frame_ancestors = "frame-ancestors 'self'"
        else:
            response.headers["X-Frame-Options"] = "DENY"
            frame_ancestors = "frame-ancestors 'none'"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' https://unpkg.com 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "connect-src 'self'; "
            f"{frame_ancestors}"
        )
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=(), payment=()"
        )
        return response


async def run_with_timeout(
    fn: Callable[..., Any],
    *args: Any,
    timeout: float = 15.0,
    label: str = "computation",
) -> Any:
    """Run a sync function in a bounded thread pool with timeout.

    Propagates ContextVars (incl. locale) into the worker thread.
    Returns the result or raises HTTP 503 on timeout.

    Note: a timed-out computation keeps running in its worker thread and its
    result is discarded — futures cannot be cancelled mid-flight. The pool is
    sized with headroom (COMPUTE_POOL_WORKERS) so such zombies don't starve
    subsequent requests.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_compute_pool, partial(ctx.run, fn, *args)),
            timeout=timeout,
        )
    except TimeoutError as err:
        logger.warning("Timeout after {}s for {}", timeout, label)
        raise HTTPException(503, detail=f"{label} timed out after {timeout}s") from err

"""Security headers middleware and computation timeout helper."""

import asyncio
import contextvars
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any

from fastapi import HTTPException
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_compute_pool = ThreadPoolExecutor(max_workers=2)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' https://unpkg.com 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "connect-src 'self'; "
            "frame-ancestors 'none'"
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

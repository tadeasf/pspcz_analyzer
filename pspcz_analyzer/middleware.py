"""Security headers middleware and computation timeout helper."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable

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
        return response


async def run_with_timeout(
    fn: Callable[..., Any],
    *args: Any,
    timeout: float = 15.0,
    label: str = "computation",
) -> Any:
    """Run a sync function in a bounded thread pool with timeout.

    Returns the result or raises HTTP 503 on timeout.
    """
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_compute_pool, partial(fn, *args)),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.warning("Timeout after {}s for {}", timeout, label)
        raise HTTPException(503, detail=f"{label} timed out after {timeout}s")

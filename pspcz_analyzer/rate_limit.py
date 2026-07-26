"""Rate limiting configuration via slowapi."""

from fastapi import Request
from fastapi.responses import HTMLResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded

from pspcz_analyzer.config import RATE_LIMIT_TRUSTED_PROXIES
from pspcz_analyzer.proxy import extract_client_ip, parse_ip_networks

_trusted_proxies = parse_ip_networks(RATE_LIMIT_TRUSTED_PROXIES)


def get_rate_limit_key(request: Request) -> str:
    """Bucket key: the TCP peer, or the XFF client behind a trusted proxy.

    With ``RATE_LIMIT_TRUSTED_PROXIES`` empty (the default) X-Forwarded-For
    is never honored, so limit keys cannot be spoofed. Behind a reverse
    proxy, list the proxy's address to bucket by the real client instead
    of sharing one bucket across every visitor.
    """
    if not _trusted_proxies:
        client = request.client
        return client.host if client else "0.0.0.0"
    return extract_client_ip(request, _trusted_proxies)


limiter = Limiter(
    key_func=get_rate_limit_key,
    default_limits=["60/minute"],
    storage_uri="memory://",
)


async def html_rate_limit_exceeded_handler(
    _request: Request, exc: RateLimitExceeded
) -> HTMLResponse:
    """Render a plain HTML 429 for browser-facing apps (the admin dashboard)."""
    return HTMLResponse(
        content=(
            "<h1>429 &mdash; Too Many Requests</h1>"
            f"<p>{exc.detail}</p>"
            "<p>Slow down and try again in a minute.</p>"
        ),
        status_code=429,
    )

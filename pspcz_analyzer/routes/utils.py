"""Shared route utilities."""

from urllib.parse import urlencode, urlparse

from fastapi import HTTPException, Request, Response

from pspcz_analyzer.config import PERIOD_YEARS


def validate_period(period: int) -> int:
    if period not in PERIOD_YEARS:
        raise HTTPException(404, detail=f"Unknown period {period}")
    return period


def _safe_url(url: str) -> str:
    """Return url only if scheme is http/https, else empty string."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        if parsed.scheme in ("http", "https"):
            return url
    except ValueError:
        pass
    return ""


def with_push_url(response: Response, request: Request, page_path: str) -> Response:
    """Mirror an HTMX list request into the browser URL (#96).

    Sets HX-Push-Url to the page with the same non-empty query parameters, so
    a search can be shared and Back restores it. Skipped for history-restore
    requests and when the URL would not change (the initial load).

    Args:
        response: The partial's response.
        request: The HTMX request.
        page_path: Full-page path the partial belongs to (e.g. "/votes").

    Returns:
        The same response.
    """
    if not request.headers.get("hx-request") or request.headers.get("hx-history-restore-request"):
        return response
    query = urlencode([(k, v) for k, v in request.query_params.multi_items() if v])
    url = f"{page_path}?{query}" if query else page_path
    current = urlparse(request.headers.get("hx-current-url", ""))
    current_url = f"{current.path}?{current.query}" if current.query else current.path
    if url != current_url:
        response.headers["HX-Push-Url"] = url
    return response

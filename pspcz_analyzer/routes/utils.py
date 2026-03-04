"""Shared route utilities."""

from urllib.parse import urlparse

from fastapi import HTTPException

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

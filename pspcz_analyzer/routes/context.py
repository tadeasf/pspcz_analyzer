"""Jinja2 context processors shared by frontend templates."""

from datetime import date, datetime
from typing import Any

import polars as pl
from fastapi import Request
from loguru import logger

from pspcz_analyzer.config import DEFAULT_PERIOD, TISKY_HISTORIE_DIR, TISKY_META_DIR
from pspcz_analyzer.i18n import get_locale


def format_date(d: date) -> str:
    """Format a date the way the site writes dates ("24. 10. 2026" / "Oct 24, 2026").

    Args:
        d: Date (or datetime) to format.

    Returns:
        Locale-specific date string.
    """
    return d.strftime("%-d. %-m. %Y" if get_locale() == "cs" else "%b %-d, %Y")


def _last_vote_date(svc: Any, period: int) -> str | None:
    """Newest vote date of an already-loaded period, formatted."""
    if period not in svc.loaded_periods:
        return None
    newest = (
        svc.get_period(period)
        .votes.select(pl.col("datum").str.to_date("%d.%m.%Y", strict=False).max())
        .item()
    )
    return format_date(newest) if newest else None


def _histories_loaded(svc: Any, period: int) -> str | None:
    """When the legislative histories of a period were last scraped, formatted."""
    hist_dir = svc.cache_dir / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
    mtimes = [p.stat().st_mtime for p in hist_dir.glob("*.json")] if hist_dir.is_dir() else []
    return format_date(datetime.fromtimestamp(max(mtimes))) if mtimes else None


def data_dates_processor(request: Request) -> dict[str, Any]:
    """Inject the two footer dates (last vote, last history scrape) into templates.

    Tolerates the startup race (app.state.data not set yet) and invalid
    ?period= query params.

    Args:
        request: The incoming request (starlette passes this to processors).

    Returns:
        Context dict with ``last_vote_date`` and ``histories_loaded`` (str or None).
    """
    svc = getattr(request.app.state, "data", None)
    empty: dict[str, Any] = {"last_vote_date": None, "histories_loaded": None}
    if svc is None:
        return empty
    try:
        period = int(request.query_params.get("period", DEFAULT_PERIOD))
    except (TypeError, ValueError):
        period = DEFAULT_PERIOD
    try:
        return {
            "last_vote_date": _last_vote_date(svc, period),
            "histories_loaded": _histories_loaded(svc, period),
        }
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        logger.warning("Footer dates unavailable for period {}: {}", period, exc)
        return empty

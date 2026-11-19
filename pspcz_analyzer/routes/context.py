"""Jinja2 context processors shared by frontend templates."""

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import Request
from loguru import logger

from pspcz_analyzer.config import (
    CLUB_DISPLAY_NAMES,
    DEFAULT_PERIOD,
    PARQUET_DIR,
    TISKY_HISTORIE_DIR,
    TISKY_META_DIR,
)
from pspcz_analyzer.i18n import get_locale


def format_date(d: date) -> str:
    """Format a date the way the site writes dates ("24. 10. 2026" / "Oct 24, 2026").

    Args:
        d: Date (or datetime) to format.

    Returns:
        Locale-specific date string.
    """
    return d.strftime("%-d. %-m. %Y" if get_locale() == "cs" else "%b %-d, %Y")


def club_name(party: str | None) -> str:
    """Jinja filter: display name of a club abbreviation.

    Args:
        party: Normalised club abbreviation, or None.

    Returns:
        Display name ("?" when unknown).
    """
    return CLUB_DISPLAY_NAMES.get(party, party) if party else "?"


def excuses_as_of(cache_dir: Path, period: int) -> str | None:
    """Date the period's excuse data was last parsed (psp.cz back-fills excuses).

    Args:
        cache_dir: Data cache root.
        period: Electoral period number.

    Returns:
        Formatted date, or None when no excuse data is cached.
    """
    f = cache_dir / PARQUET_DIR / f"omluvy_{period}.parquet"
    return format_date(datetime.fromtimestamp(f.stat().st_mtime)) if f.is_file() else None


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

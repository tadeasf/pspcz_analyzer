"""Jinja2 context processors shared by frontend templates."""

from datetime import datetime
from typing import Any

from fastapi import Request

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.i18n import get_locale, gettext

_FRESHNESS_MINUTE_S = 60
_FRESHNESS_HOUR_S = 3600
_FRESHNESS_DAY_S = 86400


def _relative_time(ts: float, now: float) -> str:
    """Format an epoch timestamp as a translated relative string."""
    delta = max(0.0, now - ts)
    if delta < _FRESHNESS_MINUTE_S:
        return gettext("freshness.just_now")
    if delta < _FRESHNESS_HOUR_S:
        minutes = int(delta // _FRESHNESS_MINUTE_S)
        return gettext("freshness.minutes").format(n=minutes)
    if delta < _FRESHNESS_DAY_S:
        hours = int(delta // _FRESHNESS_HOUR_S)
        return gettext("freshness.hours").format(n=hours)
    days = int(delta // _FRESHNESS_DAY_S)
    return gettext("freshness.days").format(n=days)


def _absolute_time(ts: float) -> str:
    """Format an epoch timestamp in the locale's conventional form."""
    dt = datetime.fromtimestamp(ts)
    pattern = "%-d. %-m. %Y, %-H:%M" if get_locale() == "cs" else "%b %-d, %Y, %-H:%M"
    return dt.strftime(pattern)


def data_freshness_processor(request: Request) -> dict[str, Any]:
    """Inject data_updated (relative + absolute) into all frontend templates.

    The timestamps come from DataReader's watcher mtime snapshots (30 s
    granularity). Tolerates the startup race (app.state.data not set yet)
    and invalid ?period= query params.

    Args:
        request: The incoming request (starlette passes this to processors).

    Returns:
        Context dict with data_updated=None or {relative, absolute} strings.
    """
    svc = getattr(request.app.state, "data", None)
    if svc is None:
        return {"data_updated": None}
    try:
        period = int(request.query_params.get("period", DEFAULT_PERIOD))
    except (TypeError, ValueError):
        period = DEFAULT_PERIOD
    ts = svc.last_updated(period)
    if not isinstance(ts, (int, float)) or ts <= 0:
        return {"data_updated": None}
    return {
        "data_updated": {
            "relative": _relative_time(ts, datetime.now().timestamp()),
            "absolute": _absolute_time(ts),
        }
    }

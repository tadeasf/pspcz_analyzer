"""Homepage legislation overview: recent activity + currently active laws.

Surfaces the freshest legislative content for the dashboard so users are led
straight to laws (zákony) and amendments (pozměňovací návrhy): bills that have
moved most recently, and bills still being decided. Each item links a law to
its amendments and tags who drives the change (government vs opposition).
"""

from __future__ import annotations

import re

from pspcz_analyzer.config import TERMINAL_TISK_STATUSES
from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.affiliation import amendment_driver

_CZ_DATE_RE = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})")


def _parse_cz_date(value: str | None) -> tuple[int, int, int] | None:
    """Parse a Czech 'd. m. yyyy' date string into a sortable (y, m, d) tuple."""
    if not value:
        return None
    m = _CZ_DATE_RE.search(value)
    if not m:
        return None
    day, month, year = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return (year, month, day)


def _latest_stage(tisk: TiskInfo) -> tuple[tuple[int, int, int] | None, str]:
    """Return (sortable_date, display_date) of a tisk's most recent dated stage."""
    best: tuple[int, int, int] | None = None
    best_display = ""
    if tisk.history and tisk.history.stages:
        for stage in tisk.history.stages:
            parsed = _parse_cz_date(stage.date)
            if parsed is not None and (best is None or parsed > best):
                best = parsed
                best_display = stage.date or ""
    return best, best_display


def _amendment_info_for_ct(data: PeriodData, ct: int) -> dict:
    """Collect amendment count, result, link and driver for a bill's ct."""
    for (schuze, bod), bill in data.amendment_data.items():
        if bill.ct == ct:
            parties = [p for a in bill.amendments for p in a.submitter_parties]
            return {
                "amendment_count": bill.amendment_count,
                "final_result": bill.final_vote.result if bill.final_vote else "",
                "has_amendments": True,
                "amendment_link": {"schuze": schuze, "bod": bod},
                "driver": amendment_driver(parties, data.coalition_parties),
            }
    return {
        "amendment_count": 0,
        "final_result": "",
        "has_amendments": False,
        "amendment_link": None,
        "driver": "unknown",
    }


def _overview_row(tisk: TiskInfo, data: PeriodData, lang: str, latest_display: str) -> dict:
    """Build a compact overview row dict for the homepage feed."""
    status = tisk.history.current_status if tisk.history else "projednáváno"
    topics = tisk.topics_en if (lang == "en" and tisk.topics_en) else tisk.topics
    row = {
        "ct": tisk.ct,
        "nazev": tisk.nazev,
        "status": status,
        "topics": topics[:3],
        "url": tisk.url,
        "latest_date": latest_display,
    }
    row.update(_amendment_info_for_ct(data, tisk.ct))
    return row


def _unique_tisky(data: PeriodData) -> list[TiskInfo]:
    """Deduplicate tisk_lookup entries by ct."""
    seen: set[int] = set()
    unique: list[TiskInfo] = []
    for tisk in data.tisk_lookup.values():
        if tisk.ct not in seen:
            seen.add(tisk.ct)
            unique.append(tisk)
    return unique


def recent_activity(data: PeriodData, lang: str = "cs", limit: int = 8) -> list[dict]:
    """Bills that moved most recently, newest legislative step first.

    Args:
        data: Period data with tisk_lookup and amendment_data.
        lang: Language code for topic labels.
        limit: Maximum number of rows to return.

    Returns:
        List of overview row dicts ordered by most recent dated stage.
    """
    dated: list[tuple[tuple[int, int, int], str, TiskInfo]] = []
    for tisk in _unique_tisky(data):
        latest, display = _latest_stage(tisk)
        if latest is not None:
            dated.append((latest, display, tisk))

    dated.sort(key=lambda item: item[0], reverse=True)
    return [_overview_row(t, data, lang, display) for _, display, t in dated[:limit]]


def active_laws(data: PeriodData, lang: str = "cs", limit: int = 8) -> list[dict]:
    """Bills still being decided (non-terminal status), newest activity first.

    Args:
        data: Period data with tisk_lookup and amendment_data.
        lang: Language code for topic labels.
        limit: Maximum number of rows to return.

    Returns:
        List of overview row dicts for active bills.
    """
    active: list[tuple[tuple[int, int, int] | None, str, TiskInfo]] = []
    for tisk in _unique_tisky(data):
        status = tisk.history.current_status if tisk.history else "projednáváno"
        if status in TERMINAL_TISK_STATUSES:
            continue
        latest, display = _latest_stage(tisk)
        active.append((latest, display, tisk))

    # Sort: bills with a known recent date first (desc), undated last, then by ct desc
    active.sort(key=lambda item: (item[0] or (0, 0, 0), item[2].ct), reverse=True)
    return [_overview_row(t, data, lang, display) for _, display, t in active[:limit]]

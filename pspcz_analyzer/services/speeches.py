"""Speech counts per agenda item from the steno day pages (#108 "Teď se o tom mluví")."""

import html
import json
import re
from datetime import date
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import PERIOD_YEARS
from pspcz_analyzer.services.amendments.steno_scraper import (
    _BOD_ANCHOR_RE,
    _DAY_PAGE_LINK_RE,
    PSP_STENO_BASE_TEMPLATE,
    _download_day_page,
    _download_index,
    _find_bod_day_pages,
)

VOTE_STENO_DIR = "vote_steno"

SPEECHES_DIR = "speeches"
_SECTION_RE = re.compile(r'<a\s+name="(q\d+)"', re.I)
_SPEAKER_RE = re.compile(r'<a href="s\d+\.htm#r\d+">([^<]+)</a>', re.I)
_CHAIR_RE = re.compile(r"^(?:Místo)?předsed(?:a|kyně) PSP", re.I)
_H1_DATE_RE = re.compile(r"(\d{1,2})\.\s*([a-zěščřžýáíéůú]+)\s+(\d{4})")
_MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "ledna",
            "února",
            "března",
            "dubna",
            "května",
            "června",
            "července",
            "srpna",
            "září",
            "října",
            "listopadu",
            "prosince",
        ],
        1,
    )
}


def _day_date(day_html: str) -> str:
    """ISO date from a day page heading ("24. března 2026"), "" if not found."""
    m = _H1_DATE_RE.search(html.unescape(day_html[:5000]).replace("\xa0", " "))
    if not m or m.group(2) not in _MONTHS:
        return ""
    return date(int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1))).isoformat()


def speech_counts(index_html: str, day_pages: dict[str, str]) -> dict[int, dict[str, int]]:
    """Speeches per agenda item and day, without the chair's procedural turns.

    Args:
        index_html: Sitting index (bod anchors ``bN`` followed by day-page links).
        day_pages: ``{day page file: html}`` (TOC of speakers under ``qN`` anchors).

    Returns:
        ``{bod: {iso date: speeches}}``.
    """
    out: dict[int, dict[str, int]] = {}
    for bod in {int(m.group(1)) for m in _BOD_ANCHOR_RE.finditer(index_html)}:
        for page, anchor in _find_bod_day_pages(index_html, bod):
            day_html = day_pages.get(page, "")
            starts = [(m.group(1), m.start()) for m in _SECTION_RE.finditer(day_html)]
            for i, (q, pos) in enumerate(starts):
                if q != anchor:
                    continue
                end = starts[i + 1][1] if i + 1 < len(starts) else len(day_html)
                names = _SPEAKER_RE.findall(day_html[pos:end])
                n = sum(not _CHAIR_RE.match(html.unescape(x).strip()) for x in names)
                if n:
                    day = out.setdefault(bod, {})
                    key = _day_date(day_html)
                    day[key] = day.get(key, 0) + n
    return out


def sitting_speeches(period: int, schuze: int, cache_dir: Path) -> dict[int, dict[str, int]]:
    """Speech counts of a sitting from the (already cached) index and day pages."""
    index = _download_index(period, schuze, cache_dir)
    if not index:
        return {}
    base = PSP_STENO_BASE_TEMPLATE.format(year=PERIOD_YEARS[period], session=schuze)
    days = {m.group(2) + ".html" for m in _DAY_PAGE_LINK_RE.finditer(index)}
    pages = {d: _download_day_page(base, d, period, schuze, cache_dir) or "" for d in days}
    return speech_counts(index, pages)


def load_speeches(cache_dir: Path, period: int) -> dict[tuple[int, int], dict[str, int]]:
    """All speech counts of a period: ``{(schuze, bod): {iso date: speeches}}``."""
    out: dict[tuple[int, int], dict[str, int]] = {}
    for path in (cache_dir / VOTE_STENO_DIR / str(period) / SPEECHES_DIR).glob("*.json"):
        try:
            schuze = int(path.stem)
            out.update({(schuze, int(b)): v for b, v in json.loads(path.read_text()).items()})
        except (ValueError, OSError):
            logger.opt(exception=True).warning("Unreadable speeches file {}", path)
    return out

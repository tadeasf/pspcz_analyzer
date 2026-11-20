"""Download the steno pages of a sitting for vote anchoring (#102)."""

import time
from datetime import date, datetime, timedelta
from pathlib import Path

from pspcz_analyzer.config import PERIOD_YEARS, PSP_REQUEST_DELAY
from pspcz_analyzer.services.amendments.steno_scraper import (
    _DAY_PAGE_LINK_RE,
    _SUBPAGE_LINK_RE,
    _SUBPAGE_NUM_RE,
    PSP_STENO_BASE_TEMPLATE,
    _download_day_page,
    _download_index,
    _download_subpage,
    _steno_cache_dir,
)

RECENT_DAYS = 21  # sittings this fresh get their index and day pages re-fetched
_EXTRA_DAYS = 2  # unlinked day pages probed after the last linked one


def is_recent(datum: str) -> bool:
    """Whether a vote date ("d.m.YYYY") is within RECENT_DAYS (its steno may still grow)."""
    try:
        return datetime.strptime(datum, "%d.%m.%Y").date() >= date.today() - timedelta(
            days=RECENT_DAYS
        )
    except ValueError:
        return False


def _day_pages(index: str, schuze: int) -> list[str]:
    """Day pages linked from the index plus the next few, which the index sometimes omits.

    psp.cz left day 6 of sitting 10 (``10-6.html``) out of the index; missing pages are
    negatively cached, so probing costs one request per sitting and run at most.
    """
    days = sorted({m.group(2) for m in _DAY_PAGE_LINK_RE.finditer(index)}, key=_day_no)
    last = max((_day_no(d) for d in days), default=0)
    extra = [f"{schuze}-{n}" for n in range(last + 1, last + 1 + _EXTRA_DAYS)]
    return [d + ".html" for d in days + extra]


def _day_no(day: str) -> int:
    return int(day.rsplit("-", 1)[1])


def sitting_pages(
    period: int, schuze: int, cache_dir: Path, refresh: bool = False
) -> list[tuple[str, str]]:
    """Download (or read cached) all steno sub-pages of a sitting, in order.

    Day-page TOCs skip pages without a speaker anchor, so the whole numeric range
    between the first and last linked page is fetched.

    Args:
        period: Electoral period.
        schuze: Sitting number.
        cache_dir: Base cache directory.
        refresh: Re-fetch the index and day pages (sitting may still grow).

    Returns:
        ``(page name, html)`` list; empty when the steno is not published.
    """
    sdir = _steno_cache_dir(cache_dir, period)
    if refresh:
        for stale in [sdir / f"index_{schuze}.html", *sdir.glob(f"{schuze}_day_*")]:
            stale.unlink(missing_ok=True)
    index = _download_index(period, schuze, cache_dir)
    if not index:
        return []
    base = PSP_STENO_BASE_TEMPLATE.format(year=PERIOD_YEARS[period], session=schuze)
    nums: set[int] = set()
    for day in _day_pages(index, schuze):
        day_html = _download_day_page(base, day, period, schuze, cache_dir) or ""
        if not day_html:
            continue
        for link in _SUBPAGE_LINK_RE.findall(day_html):
            if m := _SUBPAGE_NUM_RE.match(link):
                nums.add(int(m.group(1)))
    pages: list[tuple[str, str]] = []
    for n in range(min(nums), max(nums) + 1) if nums else ():
        name = f"s{n:06d}.htm"
        cached = (sdir / f"{schuze}_sub_{name.replace('.htm', '.html')}").exists()
        page = _download_subpage(base, name, period, schuze, cache_dir)
        if not cached:
            time.sleep(PSP_REQUEST_DELAY)
        if page:
            pages.append((name, page))
    return pages

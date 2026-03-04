"""Download and cache stenographic record HTML pages from psp.cz.

The steno structure for each session (schůze) has three levels:
  1. Index page: .../{NNN}schuz/index.htm — lists all agenda items (bods) with
     anchors like <a name="b1"> and links to day-pages (e.g. 45-3.html#q230).
  2. Day-pages: .../{NNN}schuz/45-3.html — TOC for one day of the session,
     listing speakers with links to individual transcript sub-pages.
  3. Sub-pages: .../{NNN}schuz/s045062.htm — actual speech transcript text.

The amendment voting text (e.g. "přikročíme k hlasování o pozměňovacích
návrzích") lives in the sub-pages, so we must follow all three levels.

This module handles:
- Parsing session index pages to find which day-pages discuss a given bod
- Extracting steno sub-page links from day-pages
- Downloading and caching all pages
"""

import re
import time
from enum import StrEnum
from html import unescape as html_unescape
from pathlib import Path

import httpx
from loguru import logger
from selectolax.parser import HTMLParser

from pspcz_analyzer.config import (
    PERIOD_YEARS,
    PSP_REQUEST_DELAY,
    UNL_ENCODING,
)


class StenoFailure(StrEnum):
    """Reason why steno transcript lookup failed for a given bod."""

    NO_YEAR_MAPPING = "no_year_mapping"
    INDEX_DOWNLOAD_FAILED = "index_download_failed"
    BOD_NOT_IN_INDEX = "bod_not_in_index"
    NO_SUBPAGES = "no_subpages"
    NO_AMENDMENT_START = "no_amendment_start"
    SUBPAGE_DOWNLOAD_FAILED = "subpage_download_failed"


# URL templates for steno records
PSP_STENO_INDEX_TEMPLATE = "https://www.psp.cz/eknih/{year}ps/stenprot/{session:03d}schuz/index.htm"
PSP_STENO_BASE_TEMPLATE = "https://www.psp.cz/eknih/{year}ps/stenprot/{session:03d}schuz/"

# Regex for bod anchor in index page: <a name="b1" id="b1">
_BOD_ANCHOR_RE = re.compile(r'<a\s+[^>]*?(?:name|id)="b(\d+)"', re.IGNORECASE)

# Regex for day-page links in index: href="45-3.html#q230"
_DAY_PAGE_LINK_RE = re.compile(r'href="((\d+-\d+)\.html(?:#(q\d+))?)"', re.IGNORECASE)

# Regex for steno sub-page links in day-pages: href="s045062.htm#r1"
_SUBPAGE_LINK_RE = re.compile(r'href="(s\d+\.htm)', re.IGNORECASE)

# Section anchors in day-pages: <a name="q230" id="q230">
_SECTION_ANCHOR_RE = re.compile(r'<a\s+[^>]*?(?:name|id)="(q\d+)"', re.IGNORECASE)

# Encoding for steno HTML (matches UNL encoding)
_STENO_ENCODINGS = ["windows-1250", "iso-8859-2", "utf-8"]

# Regex to detect start of amendment voting section in HTML
_AMENDMENT_START_RE = re.compile(
    r"(?:přikročíme|přistoupíme).*?k\s+hlasování\s+o\s+(?:pozměňovac|návrzích)",
    re.IGNORECASE | re.DOTALL,
)

# Regex to detect transition to a different bod (agenda item boundary)
_BOD_BOUNDARY_RE = re.compile(
    r"(?:Dalším\s+bodem|Přistoupíme\s+k\s+bodu|Přistoupíme\s+k\s+projednávání"
    r"|Dalším\s+(?:projednávaným\s+)?bodem\s+(?:je|bude|pořadu))",
    re.IGNORECASE,
)


def _has_amendment_start(html: str) -> bool:
    """Check if HTML contains the start of the amendment voting section.

    Strips HTML tags and searches for the characteristic phrase
    "přikročíme k hlasování o pozměňovacích návrzích" that marks where
    the chair begins the amendment voting procedure.

    Args:
        html: Raw HTML content of a steno sub-page.

    Returns:
        True if the amendment voting start pattern is found.
    """
    text = html_unescape(HTMLParser(html).text(separator=" ", strip=True) or "")
    return bool(_AMENDMENT_START_RE.search(text))


def _is_bod_boundary(html: str) -> bool:
    """Check if HTML contains a transition to a different agenda item (bod).

    Args:
        html: Raw HTML content of a steno sub-page.

    Returns:
        True if the page starts a new/different bod.
    """
    text = html_unescape(HTMLParser(html).text(separator=" ", strip=True) or "")
    return bool(_BOD_BOUNDARY_RE.search(text))


def _detect_decode(content: bytes) -> str:
    """Try multiple encodings to decode steno HTML content.

    Args:
        content: Raw bytes from HTTP response.

    Returns:
        Decoded string.
    """
    for enc in _STENO_ENCODINGS:
        try:
            return content.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return content.decode(UNL_ENCODING, errors="replace")


def _steno_cache_dir(cache_dir: Path, period: int) -> Path:
    """Get the steno cache directory for a period.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period number.

    Returns:
        Path to steno cache directory.
    """
    d = cache_dir / "steno" / str(period)
    d.mkdir(parents=True, exist_ok=True)
    return d


_NEGATIVE_CACHE_MARKER = "<<NEGATIVE_CACHE>>"


def _download_cached(
    url: str,
    cache_file: Path,
) -> str | None:
    """Download a URL with caching and rate limiting.

    Uses negative caching: when a download fails (404, timeout, etc.),
    a sentinel marker file is written so subsequent calls skip the HTTP
    request entirely.

    Args:
        url: URL to download.
        cache_file: Local file to cache the result.

    Returns:
        HTML content as string, or None on failure.
    """
    if cache_file.exists():
        content = cache_file.read_text(encoding="utf-8", errors="replace")
        if content == _NEGATIVE_CACHE_MARKER:
            return None
        return content

    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        html = _detect_decode(resp.content)
        cache_file.write_text(html, encoding="utf-8")
        time.sleep(PSP_REQUEST_DELAY)
        return html
    except httpx.HTTPError:
        logger.warning("[amendment pipeline] Failed to fetch: {}", url)
        cache_file.write_text(_NEGATIVE_CACHE_MARKER, encoding="utf-8")
        return None


# ── Index page parsing ────────────────────────────────────────────────────


def _download_index(
    period: int,
    schuze: int,
    cache_dir: Path,
) -> str | None:
    """Download (or retrieve from cache) the session index page.

    Args:
        period: Electoral period number.
        schuze: Session number.
        cache_dir: Base cache directory.

    Returns:
        Index page HTML, or None on failure.
    """
    year = PERIOD_YEARS.get(period, "")
    if not year:
        logger.warning("[amendment pipeline] No year mapping for period {}", period)
        return None

    url = PSP_STENO_INDEX_TEMPLATE.format(year=year, session=schuze)
    sdir = _steno_cache_dir(cache_dir, period)
    cache_file = sdir / f"index_{schuze}.html"
    return _download_cached(url, cache_file)


def _find_bod_day_pages(
    index_html: str,
    target_bod: int,
) -> list[tuple[str, str | None]]:
    """Parse the index page to find day-page references for a specific bod.

    The index page has anchors like <a name="b1" id="b1"> followed by
    links like <a href="45-3.html#q230">Projednávání</a>.

    Args:
        index_html: HTML content of the index page.
        target_bod: Agenda item number to search for.

    Returns:
        List of (day_page_filename, anchor_id_or_none) tuples.
        E.g. [("45-3.html", "q230"), ("45-3.html", "q310")]
    """
    # Find all bod anchors and their positions
    bod_positions: list[tuple[int, int]] = []
    for m in _BOD_ANCHOR_RE.finditer(index_html):
        bod_positions.append((int(m.group(1)), m.start()))

    # Find the section for our target bod
    target_start: int | None = None
    target_end: int | None = None
    for i, (bod_num, pos) in enumerate(bod_positions):
        if bod_num == target_bod:
            target_start = pos
            target_end = bod_positions[i + 1][1] if i + 1 < len(bod_positions) else len(index_html)
            break

    if target_start is None:
        return []

    section = index_html[target_start:target_end]

    # Extract day-page links from this section
    refs: list[tuple[str, str | None]] = []
    for m in _DAY_PAGE_LINK_RE.finditer(section):
        page_file = m.group(1).split("#")[0]  # e.g. "45-3.html"
        anchor_id = m.group(3)  # e.g. "q230" or None
        refs.append((page_file, anchor_id))

    return refs


# ── Day-page processing ──────────────────────────────────────────────────


def _download_day_page(
    base_url: str,
    page_file: str,
    period: int,
    schuze: int,
    cache_dir: Path,
) -> str | None:
    """Download a day-page (e.g. 45-3.html).

    Args:
        base_url: Base URL for this session.
        page_file: Day-page filename (e.g. "45-3.html").
        period: Electoral period number.
        schuze: Session number.
        cache_dir: Base cache directory.

    Returns:
        Day-page HTML, or None on failure.
    """
    url = base_url + page_file
    sdir = _steno_cache_dir(cache_dir, period)
    safe_name = page_file.replace("/", "_")
    cache_file = sdir / f"{schuze}_day_{safe_name}"
    return _download_cached(url, cache_file)


def _extract_subpage_links(
    day_html: str,
    anchor_id: str | None,
) -> list[str]:
    """Extract steno sub-page filenames from a day-page section.

    If anchor_id is given, extracts links only from the section between
    that anchor and the next section anchor (e.g. between q230 and q310).
    Otherwise extracts from the entire page.

    Args:
        day_html: HTML content of the day-page.
        anchor_id: Section anchor ID (e.g. "q230"), or None for full page.

    Returns:
        Ordered list of unique sub-page filenames (e.g. ["s045062.htm"]).
    """
    if anchor_id:
        # Find section start
        start_pattern = re.compile(rf'(?:name|id)="{re.escape(anchor_id)}"', re.IGNORECASE)
        start_match = start_pattern.search(day_html)
        if not start_match:
            # Anchor not found; fall back to full page
            section = day_html
        else:
            start_pos = start_match.end()
            # Find the next section anchor after this one
            next_match = _SECTION_ANCHOR_RE.search(day_html, pos=start_pos)
            section = (
                day_html[start_pos : next_match.start()] if next_match else day_html[start_pos:]
            )
    else:
        section = day_html

    # Extract unique sub-page filenames, preserving order
    seen: set[str] = set()
    subpages: list[str] = []
    for m in _SUBPAGE_LINK_RE.finditer(section):
        fname = m.group(1)
        if fname not in seen:
            seen.add(fname)
            subpages.append(fname)

    return subpages


# ── Sub-page downloading ─────────────────────────────────────────────────


def _download_subpage(
    base_url: str,
    subpage_name: str,
    period: int,
    schuze: int,
    cache_dir: Path,
) -> str | None:
    """Download a single steno transcript sub-page (e.g. s045062.htm).

    Args:
        base_url: Base URL for this session.
        subpage_name: Sub-page filename (e.g. "s045062.htm").
        period: Electoral period number.
        schuze: Session number.
        cache_dir: Base cache directory.

    Returns:
        HTML content as string, or None on failure.
    """
    url = base_url + subpage_name
    sdir = _steno_cache_dir(cache_dir, period)
    cache_file = sdir / f"{schuze}_sub_{subpage_name.replace('.htm', '.html')}"
    return _download_cached(url, cache_file)


# ── Public API ────────────────────────────────────────────────────────────


def find_steno_for_bod(
    period: int,
    schuze: int,
    bod: int,
    _bod_nazev: str,
    cache_dir: Path,
) -> tuple[str | None, str, StenoFailure | None]:
    """Find and download steno transcript pages for a specific agenda item.

    Follows the three-level psp.cz steno structure:
      index → day-pages → transcript sub-pages

    Args:
        period: Electoral period number.
        schuze: Session number.
        bod: Agenda item number.
        _bod_nazev: Title/name of the agenda item (unused, kept for API compat).
        cache_dir: Base cache directory.

    Returns:
        Tuple of (concatenated transcript HTML or None, first sub-page URL,
        StenoFailure reason or None on success).
    """
    year = PERIOD_YEARS.get(period, "")
    if not year:
        logger.warning("[amendment pipeline] No year mapping for period {}", period)
        return None, "", StenoFailure.NO_YEAR_MAPPING

    base_url = PSP_STENO_BASE_TEMPLATE.format(year=year, session=schuze)

    # Step 1: Download and parse the index page
    index_html = _download_index(period, schuze, cache_dir)
    if not index_html:
        return None, "", StenoFailure.INDEX_DOWNLOAD_FAILED

    # Step 2: Find which day-pages discuss this bod
    day_refs = _find_bod_day_pages(index_html, bod)
    if not day_refs:
        logger.debug(
            "[amendment pipeline] Bod {} not found in steno index for period={} schuze={}",
            bod,
            period,
            schuze,
        )
        return None, "", StenoFailure.BOD_NOT_IN_INDEX

    # Step 3: Download day-pages and extract sub-page links
    all_subpages: list[str] = []
    seen_subpages: set[str] = set()
    for page_file, anchor_id in day_refs:
        day_html = _download_day_page(base_url, page_file, period, schuze, cache_dir)
        if day_html is None:
            continue

        subpages = _extract_subpage_links(day_html, anchor_id)
        for sp in subpages:
            if sp not in seen_subpages:
                seen_subpages.add(sp)
                all_subpages.append(sp)

    if not all_subpages:
        logger.debug(
            "[amendment pipeline] No steno sub-pages found for period={} schuze={} bod={}",
            period,
            schuze,
            bod,
        )
        return None, "", StenoFailure.NO_SUBPAGES

    logger.debug(
        "[amendment pipeline] Found {} steno sub-pages for period={} schuze={} bod={}",
        len(all_subpages),
        period,
        schuze,
        bod,
    )

    # Step 4: Download sub-pages in reverse order with early exit
    # Amendment voting always happens at the END of a bod's discussion,
    # so we search backwards to find the start pattern, avoiding downloading
    # all 100-220 sub-pages when only the last 3-10 contain useful data.
    collected: list[tuple[str, str]] = []  # (subpage_name, html)
    found_start = False
    start_idx: int | None = None

    for i, subpage_name in enumerate(reversed(all_subpages)):
        html = _download_subpage(base_url, subpage_name, period, schuze, cache_dir)
        if html is None:
            continue
        collected.append((subpage_name, html))

        if _has_amendment_start(html):
            found_start = True
            # Convert reversed index back to forward index
            start_idx = len(all_subpages) - 1 - i
            break

    if not collected:
        return None, "", StenoFailure.SUBPAGE_DOWNLOAD_FAILED

    if not found_start:
        # No amendment voting section found in the last N pages
        return None, "", StenoFailure.NO_AMENDMENT_START

    # Restore chronological order (we collected in reverse)
    collected.reverse()

    # Step 4b: Collect forward sub-pages after the start page
    # Amendment voting often spans 2-5 consecutive sub-pages. Continue
    # forward until hitting a page that starts a different bod.
    assert start_idx is not None
    last_collected = collected[-1][0]
    last_collected_idx = all_subpages.index(last_collected)
    for fwd_name in all_subpages[last_collected_idx + 1 :]:
        fwd_html = _download_subpage(base_url, fwd_name, period, schuze, cache_dir)
        if fwd_html is None:
            continue
        if _is_bod_boundary(fwd_html):
            break
        collected.append((fwd_name, fwd_html))

    first_url = base_url + collected[0][0]
    combined_parts = [html for _, html in collected]

    return "\n".join(combined_parts), first_url, None

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
from pathlib import Path

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    PERIOD_YEARS,
    PSP_REQUEST_DELAY,
    STENO_MAX_SUBPAGES_PER_BOD,
    STENO_NEGATIVE_CACHE_TTL,
    STENO_SUBPAGE_GAP_FILL,
    UNL_ENCODING,
)


class StenoFailure(StrEnum):
    """Reason why steno transcript lookup failed for a given bod."""

    NO_YEAR_MAPPING = "no_year_mapping"
    INDEX_DOWNLOAD_FAILED = "index_download_failed"
    BOD_NOT_IN_INDEX = "bod_not_in_index"
    NO_SUBPAGES = "no_subpages"
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

# Parses a sub-page filename into its numeric part: "s017212.htm" -> "017212".
_SUBPAGE_NUM_RE = re.compile(r"^s(\d+)\.htm$", re.IGNORECASE)

# Encoding for steno HTML (matches UNL encoding)
_STENO_ENCODINGS = ["windows-1250", "iso-8859-2", "utf-8"]


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


def _parse_negative_expiry(content: str) -> float | None:
    """Interpret cached file content as a negative-cache marker.

    Args:
        content: Full content of a steno cache file.

    Returns:
        None if the content is real HTML (a positive cache hit), otherwise
        the marker's expiry epoch in seconds. The bare legacy marker written
        by older versions (no expiry suffix) is reported as ``0.0`` — i.e.
        already expired — so permanently-poisoned caches retry once.
    """
    if content == _NEGATIVE_CACHE_MARKER:
        return 0.0
    prefix = f"{_NEGATIVE_CACHE_MARKER}:"
    if not content.startswith(prefix):
        return None
    try:
        return float(content[len(prefix) :])
    except ValueError:
        # Unparseable marker — treat as expired so it is retried and rewritten.
        return 0.0


def _write_negative_cache(cache_file: Path) -> None:
    """Write a negative-cache marker that expires after STENO_NEGATIVE_CACHE_TTL."""
    expires = time.time() + STENO_NEGATIVE_CACHE_TTL
    cache_file.write_text(f"{_NEGATIVE_CACHE_MARKER}:{expires}", encoding="utf-8")


def _fetch_steno_page(url: str) -> str | None:
    """Fetch a URL, caching 404s negatively but leaving transient failures retryable.

    Args:
        url: URL to download.

    Returns:
        Decoded HTML, or None if the page is missing or the request failed.
    """
    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            # Genuinely missing — cache the miss so reruns skip the request.
            logger.warning("[amendment pipeline] Not found (404): {}", url)
            raise
        # 5xx etc. are transient; don't poison the cache.
        logger.warning("[amendment pipeline] HTTP {} for {}", exc.response.status_code, url)
        return None
    except httpx.HTTPError:
        # Timeouts / connection errors are transient; don't poison the cache.
        logger.warning("[amendment pipeline] Failed to fetch: {}", url)
        return None

    html = _detect_decode(resp.content)
    time.sleep(PSP_REQUEST_DELAY)
    return html


def _download_cached(
    url: str,
    cache_file: Path,
) -> str | None:
    """Download a URL with positive caching and TTL-bounded negative caching.

    Only genuine HTTP 404s are negative-cached (with an expiry), so a page
    psp.cz publishes later is picked up after STENO_NEGATIVE_CACHE_TTL.
    Transient failures (timeouts, 5xx) return None without writing a marker
    and are retried on the next pipeline run — previously ANY HTTP error
    poisoned the cache permanently.

    Args:
        url: URL to download.
        cache_file: Local file to cache the result.

    Returns:
        HTML content as string, or None on failure or negative-cached miss.
    """
    if cache_file.exists():
        content = cache_file.read_text(encoding="utf-8", errors="replace")
        expiry = _parse_negative_expiry(content)
        if expiry is None:
            return content
        if time.time() < expiry:
            return None
        # Expired negative marker — fall through and retry the download.

    try:
        html = _fetch_steno_page(url)
    except httpx.HTTPStatusError:
        _write_negative_cache(cache_file)
        return None

    if html is None:
        return None
    cache_file.write_text(html, encoding="utf-8")
    return html


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


def _index_anchors_by_page(index_html: str) -> dict[str, list[str]]:
    """Map each day-page filename to all section anchors the index references.

    Every agenda item (bod) the index links onto a given day-page contributes
    its start anchor (e.g. "q230"). These anchors mark agenda-item boundaries
    within a day-page, used to bound one bod's span.

    Args:
        index_html: HTML content of the session index page.

    Returns:
        Mapping of day-page filename -> ordered unique anchor IDs.
    """
    by_page: dict[str, list[str]] = {}
    for m in _DAY_PAGE_LINK_RE.finditer(index_html):
        page_file = m.group(1).split("#")[0]
        anchor = m.group(3)
        if not anchor:
            continue
        anchors = by_page.setdefault(page_file, [])
        if anchor not in anchors:
            anchors.append(anchor)
    return by_page


def _find_anchor(day_html: str, anchor: str) -> re.Match[str] | None:
    """Locate a section anchor (name=/id=) within a day-page.

    Args:
        day_html: HTML content of the day-page.
        anchor: Anchor ID to find (e.g. "q230").

    Returns:
        The regex match, or None if absent.
    """
    return re.search(rf'(?:name|id)="{re.escape(anchor)}"', day_html, re.IGNORECASE)


def _earliest_anchor_pos(day_html: str, target_anchors: list[str | None]) -> int:
    """Return the start offset of a bod's span on a day-page.

    A None anchor (index link without a #fragment) means the bod begins at the
    top of the page.

    Args:
        day_html: HTML content of the day-page.
        target_anchors: Anchors the index assigned to this bod on this page.

    Returns:
        Character offset where the bod's span starts.
    """
    if any(a is None for a in target_anchors):
        return 0
    positions: list[int] = []
    for anchor in target_anchors:
        if anchor is None:
            continue
        match = _find_anchor(day_html, anchor)
        if match:
            positions.append(match.end())
    return min(positions) if positions else 0


def _next_boundary_pos(day_html: str, boundary_anchors: list[str], start_pos: int) -> int:
    """Return the end offset of a bod's span (the next other-bod anchor).

    Args:
        day_html: HTML content of the day-page.
        boundary_anchors: Anchors of OTHER bods on this page.
        start_pos: Offset where the target bod's span starts.

    Returns:
        Character offset where the next agenda item begins, or end-of-page.
    """
    end_pos = len(day_html)
    for anchor in boundary_anchors:
        match = _find_anchor(day_html, anchor)
        if match and start_pos <= match.start() < end_pos:
            end_pos = match.start()
    return end_pos


def _subpage_span_for_bod(
    day_html: str,
    target_anchors: list[str | None],
    boundary_anchors: list[str],
) -> list[str]:
    """Extract every sub-page link belonging to one bod's span on a day-page.

    The span runs from the bod's earliest target anchor to the next *other-bod*
    anchor (or end-of-page). Speaker sub-anchors within the bod are NOT treated
    as boundaries, so intermediate voting sub-pages are captured (unlike the old
    "stop at the next q### section" logic, which truncated multi-speaker bods).

    Args:
        day_html: HTML content of the day-page.
        target_anchors: Anchors the index assigned to this bod on this page.
        boundary_anchors: Anchors of other bods on this page.

    Returns:
        Ordered list of unique sub-page filenames (e.g. ["s045062.htm"]).
    """
    start_pos = _earliest_anchor_pos(day_html, target_anchors)
    end_pos = _next_boundary_pos(day_html, boundary_anchors, start_pos)
    section = day_html[start_pos:end_pos]

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


def _fill_subpage_gaps(subpages: list[str], max_gap: int) -> list[str]:
    """Fill small numeric gaps between collected steno sub-pages.

    Day-page TOCs link sub-pages by speaker, so a voting-continuation page (no
    speaker anchor) is skipped even though it sits mid-bod. Given the collected
    pages, insert any sub-page whose number falls in a gap of at most ``max_gap``
    between two consecutive collected pages (e.g. s017212 between s017211 and
    s017213). Larger gaps (separate sitting days/segments) are left alone.

    Args:
        subpages: Collected sub-page filenames, any order.
        max_gap: Largest numeric gap to bridge.

    Returns:
        Sub-page filenames including fillers, in ascending numeric order;
        any names that don't parse are kept (ordered first).
    """
    parsed: list[tuple[int, int, str]] = []  # (number, zero-pad width, name)
    unparsed: list[str] = []
    for name in subpages:
        m = _SUBPAGE_NUM_RE.match(name)
        if m:
            digits = m.group(1)
            parsed.append((int(digits), len(digits), name))
        else:
            unparsed.append(name)
    if not parsed:
        return subpages
    parsed.sort(key=lambda t: t[0])

    result: list[str] = []
    for i, (num, width, name) in enumerate(parsed):
        result.append(name)
        if i + 1 < len(parsed):
            nxt = parsed[i + 1][0]
            if 1 < nxt - num <= max_gap:
                result.extend(f"s{n:0{width}d}.htm" for n in range(num + 1, nxt))
    return [*unparsed, *result]


def _collect_bod_subpages(
    index_html: str,
    base_url: str,
    period: int,
    schuze: int,
    bod: int,
    cache_dir: Path,
) -> list[str] | None:
    """Collect the complete, ordered set of steno sub-pages for one bod.

    Unions sub-pages across every day-page the index links for this bod, each
    scoped to the bod's anchor span (start anchor → next other-bod anchor).

    Args:
        index_html: HTML content of the session index page.
        base_url: Base URL for this session's steno pages.
        period: Electoral period number.
        schuze: Session number.
        bod: Agenda item number.
        cache_dir: Base cache directory.

    Returns:
        Ordered unique sub-page filenames, an empty list if none were found, or
        None if the bod is absent from the index.
    """
    day_refs = _find_bod_day_pages(index_html, bod)
    if not day_refs:
        logger.debug(
            "[amendment pipeline] Bod {} not found in steno index for period={} schuze={}",
            bod,
            period,
            schuze,
        )
        return None

    anchors_by_page = _index_anchors_by_page(index_html)
    target_by_page: dict[str, list[str | None]] = {}
    for page_file, anchor_id in day_refs:
        target_by_page.setdefault(page_file, []).append(anchor_id)

    all_subpages: list[str] = []
    seen: set[str] = set()
    for page_file, target_anchors in target_by_page.items():
        day_html = _download_day_page(base_url, page_file, period, schuze, cache_dir)
        if day_html is None:
            continue
        boundary = [a for a in anchors_by_page.get(page_file, []) if a not in target_anchors]
        for sp in _subpage_span_for_bod(day_html, target_anchors, boundary):
            if sp not in seen:
                seen.add(sp)
                all_subpages.append(sp)

    if all_subpages:
        filled = _fill_subpage_gaps(all_subpages, STENO_SUBPAGE_GAP_FILL)
        added = [p for p in filled if p not in seen]
        if added:
            logger.debug(
                "[amendment pipeline] Gap-filled {} continuation sub-page(s) for "
                "period={} schuze={} bod={}: {}",
                len(added),
                period,
                schuze,
                bod,
                added,
            )
        all_subpages = filled
    return all_subpages


def _download_all_subpages(
    base_url: str,
    subpages: list[str],
    period: int,
    schuze: int,
    cache_dir: Path,
) -> list[tuple[str, str]]:
    """Download every sub-page in order, skipping failures.

    Args:
        base_url: Base URL for this session's steno pages.
        subpages: Ordered sub-page filenames to download.
        period: Electoral period number.
        schuze: Session number.
        cache_dir: Base cache directory.

    Returns:
        List of (subpage_name, html) in order for pages that downloaded.
    """
    collected: list[tuple[str, str]] = []
    for name in subpages:
        html = _download_subpage(base_url, name, period, schuze, cache_dir)
        if html is None:
            continue
        collected.append((name, html))
    return collected


def find_steno_for_bod(
    period: int,
    schuze: int,
    bod: int,
    cache_dir: Path,
) -> tuple[str | None, str, StenoFailure | None]:
    """Find and download steno transcript pages for a specific agenda item.

    Follows the three-level psp.cz steno structure (index → day-pages →
    transcript sub-pages) and returns the concatenated transcript for the bod.
    Rather than guessing where amendment voting starts from a narrative phrase,
    it captures the bod's whole span and lets the parser + cross-validation
    against the official vote numbers isolate the amendment votes.

    Args:
        period: Electoral period number.
        schuze: Session number.
        bod: Agenda item number.
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

    index_html = _download_index(period, schuze, cache_dir)
    if not index_html:
        return None, "", StenoFailure.INDEX_DOWNLOAD_FAILED

    all_subpages = _collect_bod_subpages(index_html, base_url, period, schuze, bod, cache_dir)
    if all_subpages is None:
        return None, "", StenoFailure.BOD_NOT_IN_INDEX
    if not all_subpages:
        logger.debug(
            "[amendment pipeline] No steno sub-pages found for period={} schuze={} bod={}",
            period,
            schuze,
            bod,
        )
        return None, "", StenoFailure.NO_SUBPAGES

    if len(all_subpages) > STENO_MAX_SUBPAGES_PER_BOD:
        logger.warning(
            "[amendment pipeline] Bod {} has {} steno sub-pages (period={} schuze={}); "
            "keeping last {} (voting is at the end)",
            bod,
            len(all_subpages),
            period,
            schuze,
            STENO_MAX_SUBPAGES_PER_BOD,
        )
        all_subpages = all_subpages[-STENO_MAX_SUBPAGES_PER_BOD:]

    logger.debug(
        "[amendment pipeline] Found {} steno sub-pages for period={} schuze={} bod={}",
        len(all_subpages),
        period,
        schuze,
        bod,
    )

    collected = _download_all_subpages(base_url, all_subpages, period, schuze, cache_dir)
    if not collected:
        return None, "", StenoFailure.SUBPAGE_DOWNLOAD_FAILED

    first_url = base_url + collected[0][0]
    combined_parts = [html for _, html in collected]
    return "\n".join(combined_parts), first_url, None

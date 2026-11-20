"""Amendment outputs built from steno pages that psp.cz replaced since the last run.

The parse stage rebuilds every bill from the cached steno each run, so letters, fates and
stances follow a changed page by themselves. What survives a run is the letter summaries
(carried over by ``carry_summaries``, the LLM saw the steno description) and the admin
corrections. For a bill whose steno text changed: its summaries are not carried over, and
letters a person corrected are listed in ``amendments/{period}/steno_changes.json`` for a
new review (the correction itself stays and is still shown).
"""

import json
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

from pspcz_analyzer.fileio import write_json_atomic
from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.services.amendments.steno_scraper import _steno_cache_dir
from pspcz_analyzer.services.steno_page_state import clear_stale, stale_pages

CHANGES_FILE = "steno_changes.json"
BillPages = dict[tuple[int, int], list[str]]  # (schuze, bod) -> steno sub-pages read


def changes_path(cache_dir: Path, period: int) -> Path:
    """Path of a period's list of letters returned to review."""
    return cache_dir / "amendments" / str(period) / CHANGES_FILE


def load_changes(cache_dir: Path, period: int) -> dict[str, dict]:
    """``{"ct:LETTER": {"at", "schuze", "bod", "old_summary", "old_description"}}``."""
    path = changes_path(cache_dir, period)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        logger.opt(exception=True).warning("[amendment pipeline] unreadable {}", path)
        return {}


def stale_bills(cache_dir: Path, period: int, used: BillPages) -> set[tuple[int, int]]:
    """Bills whose steno pages changed since the amendment pipeline last read them.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period.
        used: Pages each bill was parsed from in this run.

    Returns:
        ``(schuze, bod)`` of the affected bills.
    """
    sdir = _steno_cache_dir(cache_dir, period)
    stale: dict[int, set[str]] = {}
    out: set[tuple[int, int]] = set()
    for (schuze, bod), names in used.items():
        if schuze not in stale:
            stale[schuze] = stale_pages(sdir, schuze)
        if stale[schuze] & set(names):
            out.add((schuze, bod))
    return out


def record_reviews(
    cache_dir: Path,
    period: int,
    stale: set[tuple[int, int]],
    previous: dict[tuple[int, int], BillAmendmentData],
    corrected: set[str],
) -> int:
    """Put corrected letters of changed bills back into review.

    Args:
        cache_dir: Base cache directory.
        period: Electoral period.
        stale: Bills whose steno changed.
        previous: Bills of the previous run (old summaries and descriptions).
        corrected: ``"ct:LETTER"`` keys with an admin correction.

    Returns:
        Number of letters added to the review list.
    """
    now = datetime.now(UTC).isoformat(timespec="seconds")
    changes = load_changes(cache_dir, period)
    added = 0
    for key in sorted(stale):
        old = previous.get(key)
        if old is None:
            continue
        for amend in old.amendments:
            ref = f"{old.ct}:{amend.letter.strip().upper()}"
            if ref not in corrected:
                continue
            changes[ref] = {
                "at": now,
                "schuze": key[0],
                "bod": key[1],
                "old_summary": amend.summary,
                "old_description": amend.description,
            }
            added += 1
    if added:
        write_json_atomic(changes, changes_path(cache_dir, period))
        logger.info("[amendment pipeline] {} corrected letters back in review", added)
    return added


def mark_read(cache_dir: Path, period: int, used: BillPages) -> None:
    """Clear the change flag of every page this run parsed bills from."""
    sdir = _steno_cache_dir(cache_dir, period)
    by_sitting: dict[int, set[str]] = {}
    for (schuze, _bod), names in used.items():
        by_sitting.setdefault(schuze, set()).update(names)
    for schuze, names in by_sitting.items():
        clear_stale(sdir, schuze, names)

"""Admin view of steno authorization: page states and corrections whose steno changed.

A correction (``overrides``) was made on the text the admin saw. When psp.cz replaces
the steno under it later (services/vote_steno_changes, amendments/steno_changes), the
item is listed here with the old and the new AI wording until the correction is saved
again. The correction stays on the public site meanwhile.
"""

from pathlib import Path

from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.cache_manager import load_amendments
from pspcz_analyzer.services.amendments.steno_changes import load_changes
from pspcz_analyzer.services.amendments.steno_scraper import _steno_cache_dir
from pspcz_analyzer.services.steno_page_state import status_counts
from pspcz_analyzer.services.vote_steno import load_vote_steno


def _vote_rows(data: PeriodData, cache_dir: Path) -> list[dict]:
    """Corrected votes whose steno text changed after the correction."""
    corrections = data.overrides.get("vote_question", {})
    rows = []
    for vid, rec in load_vote_steno(cache_dir, data.period).items():
        old, fix = rec.get("previous"), corrections.get(str(vid))
        if not old or not fix or fix.get("at", "") >= old.get("replaced_at", ""):
            continue
        rows.append(
            {
                "id": vid,
                "cislo": rec.get("cislo"),
                "url": rec.get("url", ""),
                "corrected": fix.get("question", ""),
                "old_question": old.get("question", ""),
                "old_quote": old.get("quote", ""),
                "new_question": rec.get("question", ""),
                "new_quote": rec.get("quote", ""),
                "changed_at": old.get("replaced_at", ""),
            }
        )
    return sorted(rows, key=lambda r: r["changed_at"], reverse=True)


def _letter_rows(data: PeriodData, cache_dir: Path) -> list[dict]:
    """Corrected amendment letters whose bill's steno text changed after the correction."""
    corrections = data.overrides.get("amendment_summary", {})
    changes = load_changes(cache_dir, data.period)
    raw = load_amendments(cache_dir, data.period) if changes else {}
    rows = []
    for ref, ch in changes.items():
        fix = corrections.get(ref)
        if not fix or fix.get("at", "") >= ch.get("at", ""):
            continue
        letter = ref.partition(":")[2]
        bill = raw.get((ch["schuze"], ch["bod"]))
        amend = next(
            (a for a in (bill.amendments if bill else []) if a.letter.strip().upper() == letter),
            None,
        )
        rows.append(
            {
                "ref": ref,
                "schuze": ch["schuze"],
                "bod": ch["bod"],
                "corrected": fix.get("summary", ""),
                "old_summary": ch.get("old_summary", ""),
                "new_summary": amend.summary if amend else "",
                "changed_at": ch.get("at", ""),
            }
        )
    return sorted(rows, key=lambda r: r["changed_at"], reverse=True)


def steno_report(data: PeriodData, cache_dir: Path) -> dict:
    """Page states of the period's steno and the corrections to review again.

    Args:
        data: Loaded period data (with its overrides).
        cache_dir: Base cache directory (raw pipeline outputs, before corrections).

    Returns:
        ``{"pages": {status: count}, "votes": [...], "letters": [...]}``.
    """
    return {
        "pages": status_counts(_steno_cache_dir(cache_dir, data.period)),
        "votes": _vote_rows(data, cache_dir),
        "letters": _letter_rows(data, cache_dir),
    }

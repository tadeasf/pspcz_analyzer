"""Data quality report for the admin (#100, #97): parser scores and AI check results."""

from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendment_letters import bill_letters, unassigned_votes


def _bill_rows(data: PeriodData) -> list[dict]:
    """One row per bill with amendments: parser score, warnings, unknown fates."""
    rows = []
    for (schuze, bod), bill in sorted(data.amendment_data.items()):
        letters = bill_letters(bill, data.coalition_for_bill(bill))
        rows.append(
            {
                "schuze": schuze,
                "bod": bod,
                "ct": bill.ct,
                "nazev": bill.tisk_nazev,
                "confidence": bill.parse_confidence,
                "warnings": bill.parse_warnings,
                "unlinked": bill.unlinked_amendment_count,
                "unassigned_votes": unassigned_votes(bill),
                "letters": len(letters),
                "unknown_fate": sum(x["fate"] == "unknown" for x in letters),
                "no_summary": sum(not x["amend"].summary for x in letters),
                "summary_warning": sum(x["amend"].summary_warning for x in letters),
            }
        )
    return sorted(rows, key=lambda r: r["confidence"])


def _flagged_briefs(data: PeriodData) -> list[dict]:
    """Bills whose AI brief failed the automatic check."""
    seen: dict[int, dict] = {}
    for t in data.tisk_lookup.values():
        if t.brief and t.brief.get("status") != "ok":
            seen[t.ct] = {"ct": t.ct, "nazev": t.nazev, "issues": t.brief.get("issues", [])}
    return sorted(seen.values(), key=lambda r: r["ct"])


def quality_report(data: PeriodData) -> dict:
    """Everything the admin needs to review a period's derived data.

    Args:
        data: Loaded period data.

    Returns:
        Dict with ``bills``, ``briefs`` (total, flagged list) and ``votes``
        (total, with steno record, with question, uncertain) plus ``anchor_checks``
        (agenda items whose steno-anchored vote count is off).
    """
    briefs = {t.ct for t in data.tisk_lookup.values() if t.brief}
    recs = data.vote_steno.values()
    return {
        "bills": _bill_rows(data),
        "briefs_total": len(briefs),
        "briefs_flagged": _flagged_briefs(data),
        "votes_total": data.votes.height,
        "votes_steno": len(data.vote_steno),
        "votes_question": sum(r.get("status") == "ok" for r in recs),
        "votes_uncertain": sum(r.get("status") == "uncertain" for r in recs),
        "anchor_checks": data.anchor_checks,
    }

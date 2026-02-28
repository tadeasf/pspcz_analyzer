"""High-level query functions for amendment voting data.

Provides paginated listing and detail views for bills with amendment data,
following the same pattern as votes_service.py.
"""

import polars as pl

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData


def _amendment_to_dict(amend: AmendmentVote, lang: str = "cs") -> dict:
    """Convert an AmendmentVote to a template-friendly dict.

    Args:
        amend: The amendment vote to convert.
        lang: Language code for summary selection.

    Returns:
        Dict suitable for template rendering.
    """
    summary = amend.summary_en if (lang == "en" and amend.summary_en) else amend.summary
    return {
        "letter": amend.letter,
        "vote_number": amend.vote_number,
        "id_hlasovani": amend.id_hlasovani,
        "submitter_names": amend.submitter_names,
        "submitter_party": amend.submitter_party,
        "description": amend.description,
        "committee_stance": amend.committee_stance,
        "proposer_stance": amend.proposer_stance,
        "result": amend.result,
        "is_revote": amend.is_revote,
        "is_withdrawn": amend.is_withdrawn,
        "is_final_vote": amend.is_final_vote,
        "is_leg_tech": amend.is_leg_tech,
        "grouped_with": amend.grouped_with,
        "summary": summary,
    }


def _bill_summary(bill: BillAmendmentData) -> dict:
    """Build a summary dict for a bill's amendment data.

    Args:
        bill: Bill amendment data.

    Returns:
        Dict with bill-level summary fields for template rendering.
    """
    accepted = sum(1 for a in bill.amendments if a.result == "accepted")
    rejected = sum(1 for a in bill.amendments if a.result == "rejected")
    withdrawn = sum(1 for a in bill.amendments if a.is_withdrawn)

    final_result = ""
    if bill.final_vote:
        final_result = bill.final_vote.result

    return {
        "schuze": bill.schuze,
        "bod": bill.bod,
        "ct": bill.ct,
        "tisk_nazev": bill.tisk_nazev,
        "amendment_count": bill.amendment_count,
        "accepted": accepted,
        "rejected": rejected,
        "withdrawn": withdrawn,
        "final_result": final_result,
        "parse_confidence": bill.parse_confidence,
        "steno_url": bill.steno_url,
    }


def list_amendment_bills(
    data: PeriodData,
    search: str = "",
    page: int = 1,
    per_page: int = 20,
) -> dict:
    """List bills that have amendment voting data, with optional search.

    Args:
        data: Period data containing amendment_data.
        search: Optional text search filter on tisk name.
        page: Page number (1-based).
        per_page: Results per page.

    Returns:
        Dict with keys: rows, total, page, per_page, total_pages.
    """
    bills = list(data.amendment_data.values())

    # Filter by search text
    if search:
        search_lower = search.lower()
        bills = [b for b in bills if search_lower in b.tisk_nazev.lower()]

    # Sort by schuze desc, then bod desc
    bills.sort(key=lambda b: (b.schuze, b.bod), reverse=True)

    total = len(bills)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))

    offset = (page - 1) * per_page
    page_bills = bills[offset : offset + per_page]

    rows = [_bill_summary(b) for b in page_bills]

    return {
        "rows": rows,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


def amendment_detail(
    data: PeriodData,
    schuze: int,
    bod: int,
    lang: str = "cs",
) -> dict | None:
    """Get full detail for one bill's amendments.

    Args:
        data: Period data containing amendment_data.
        schuze: Session number.
        bod: Agenda item number.
        lang: Language code for summaries.

    Returns:
        Dict with bill info and amendment list, or None if not found.
    """
    bill = data.get_amendments(schuze, bod)
    if bill is None:
        return None

    amendments = [_amendment_to_dict(a, lang) for a in bill.amendments]
    final = _amendment_to_dict(bill.final_vote, lang) if bill.final_vote else None

    # Get tisk info for additional context
    tisk = data.get_tisk(schuze, bod)
    tisk_url = tisk.url if tisk else ""

    return {
        "schuze": bill.schuze,
        "bod": bill.bod,
        "ct": bill.ct,
        "tisk_nazev": bill.tisk_nazev,
        "tisk_url": tisk_url,
        "steno_url": bill.steno_url,
        "amendments": amendments,
        "final_vote": final,
        "amendment_count": bill.amendment_count,
        "parse_confidence": bill.parse_confidence,
        "parse_warnings": bill.parse_warnings,
    }


def _vote_label(code: str) -> str:
    """Map psp.cz vote result code to display label.

    Args:
        code: Single-character vote code (A, B, C, F, @, M).

    Returns:
        Human-readable label.
    """
    match code:
        case "A":
            return "YES"
        case "B":
            return "NO"
        case "C":
            return "ABSTAINED"
        case "F":
            return "DID_NOT_VOTE"
        case "@":
            return "Absent"
        case "M":
            return "Excused"
        case _:
            return "Unknown"


def amendment_mp_votes(
    data: PeriodData,
    id_hlasovani: int,
) -> dict | None:
    """Get per-MP vote breakdown for a single amendment vote.

    Reuses the same join pattern as vote_detail in votes_service.

    Args:
        data: Period data.
        id_hlasovani: Vote ID to look up.

    Returns:
        Dict with party_breakdown and mp_votes, or None if not found.
    """
    vote_row = data.votes.filter(pl.col("id_hlasovani") == id_hlasovani)
    if vote_row.height == 0:
        return None

    mp_rows = data.mp_votes.filter(pl.col("id_hlasovani") == id_hlasovani)
    mp_detail = mp_rows.join(data.mp_info, on="id_poslanec", how="left")

    # Build party breakdown
    party_breakdown: list[dict] = []
    parties = mp_detail.get_column("party").unique().sort().to_list()
    for party in parties:
        party_df = mp_detail.filter(pl.col("party") == party)
        results = party_df.get_column("vysledek").to_list()
        party_breakdown.append(
            {
                "party": party,
                "yes": results.count("A"),
                "no": results.count("B"),
                "abstained": results.count("C"),
                "passive": results.count("F"),
                "absent": results.count("@"),
                "excused": results.count("M"),
                "total": len(results),
            }
        )

    # Build individual MP votes
    mp_votes: list[dict] = []
    for row in mp_detail.sort("prijmeni").to_dicts():
        mp_votes.append(
            {
                "jmeno": row.get("jmeno", ""),
                "prijmeni": row.get("prijmeni", ""),
                "party": row.get("party", ""),
                "vote_code": row.get("vysledek", ""),
                "vote_label": _vote_label(row.get("vysledek", "")),
            }
        )

    vote_info = vote_row.to_dicts()[0]
    return {
        "id_hlasovani": id_hlasovani,
        "pro": vote_info.get("pro", 0),
        "proti": vote_info.get("proti", 0),
        "zdrzel": vote_info.get("zdrzel", 0),
        "nehlasoval": vote_info.get("nehlasoval", 0),
        "vysledek": vote_info.get("vysledek", ""),
        "party_breakdown": party_breakdown,
        "mp_votes": mp_votes,
    }

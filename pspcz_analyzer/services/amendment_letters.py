"""Amendment letters: the counting unit for amendments (#93, #103).

One row per letter of the consolidated amendment document, with its fate and
the side that submitted it. Legislative-technical fixes are not letters, a
re-vote replaces the original result instead of adding a letter, and grouped
votes ("E1 a F2") give one letter each. Votes without a letter are counted
separately as votes we could not assign.
"""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.services.affiliation import amendment_driver

COMMITTEE = "committee"
SIDES = ("opposition", "government", "mixed", COMMITTEE, "unknown")
# "pending" = the bill's 3rd reading has not been voted yet (BillAmendmentData.is_pending)
FATES = ("accepted", "rejected", "not_voted", "withdrawn", "pending", "unknown")


def _is_committee(amend: AmendmentVote) -> bool:
    """True when the amendment was submitted by a committee, not by MPs."""
    names = amend.pdf_submitter_names or amend.submitter_names
    return any("výbor" in n.lower() for n in names)


def _fate(amend: AmendmentVote, bill_voted: bool, all_assigned: bool, pending: bool = False) -> str:
    """Fate of one amendment entry.

    Args:
        amend: The amendment entry.
        bill_voted: True when the final vote of the 3rd reading was found.
        all_assigned: True when every 3rd-reading vote is tied to a letter, so
            a letter without a vote really was not voted on.
        pending: True when the bill's 3rd reading has not been voted yet.

    Returns:
        One of FATES.
    """
    if amend.is_withdrawn:
        return "withdrawn"
    if amend.is_unvotable:
        return "not_voted"
    if pending:
        return "pending"
    if not amend.vote_number:
        return "not_voted" if bill_voted and all_assigned else "unknown"
    match amend.result:
        case "accepted" | "rejected":
            return amend.result
        case _:
            return "unknown"


def _side(amend: AmendmentVote, coalition: frozenset[str]) -> str:
    """Who submitted the amendment: one of SIDES."""
    if _is_committee(amend):
        return COMMITTEE
    return amendment_driver(amend.submitter_parties, coalition)


def _letter_keys(amend: AmendmentVote) -> list[str]:
    """All letters an entry stands for (its own plus the grouped ones)."""
    keys = [amend.letter, *amend.grouped_with]
    return [k.strip().upper() for k in keys if k.strip()]


def _revoted_keys(amend: AmendmentVote, letters: dict[str, dict]) -> list[str]:
    """Letters a letter-less re-vote repeats, found via the challenged vote number."""
    return [
        k
        for k, row in letters.items()
        if amend.original_vote_number and row["amend"].vote_number == amend.original_vote_number
    ]


def bill_letters(bill: BillAmendmentData, coalition: frozenset[str]) -> list[dict]:
    """Letters of one bill with fate and side, in document order.

    Args:
        bill: Amendment data of one bill.
        coalition: Government clubs at the bill's 3rd reading.

    Returns:
        One dict per letter with keys letter, amend, fate, side, voted_with.
    """
    bill_voted = bill.final_vote is not None
    all_assigned = unassigned_votes(bill) == 0
    letters: dict[str, dict] = {}
    for amend in bill.amendments:
        keys = _letter_keys(amend)
        if amend.is_revote and not keys:
            keys = _revoted_keys(amend, letters)
        if amend.is_leg_tech or amend.is_procedure or amend.is_final_vote or not keys:
            continue
        for key in keys:
            if key in letters and not amend.is_revote:
                continue
            letters[key] = {
                "letter": key,
                "amend": amend,
                "fate": _fate(amend, bill_voted, all_assigned, bill.is_pending),
                "side": _side(amend, coalition),
                "voted_with": [k for k in keys if k != key],
            }
    return list(letters.values())


def unassigned_votes(bill: BillAmendmentData) -> int:
    """Votes of the 3rd reading we could not tie to any letter."""
    return sum(
        1
        for a in bill.amendments
        if a.vote_number
        and not _letter_keys(a)
        and not (a.is_leg_tech or a.is_procedure or a.is_revote)
    )


def tally(letters: list[dict]) -> dict:
    """Submitted / accepted counts per side, plus fate counts.

    Fate counts always add up to the total.

    Args:
        letters: Output of bill_letters (one or many bills concatenated).

    Returns:
        Dict with total, accepted, by_side {side: {submitted, accepted}}, by_fate.
    """
    by_side = {s: {"submitted": 0, "accepted": 0} for s in SIDES}
    by_fate = dict.fromkeys(FATES, 0)
    for row in letters:
        by_side[row["side"]]["submitted"] += 1
        by_side[row["side"]]["accepted"] += row["fate"] == "accepted"
        by_fate[row["fate"]] += 1
    return {
        "total": len(letters),
        "accepted": by_fate["accepted"],
        "by_side": by_side,
        "by_fate": by_fate,
    }

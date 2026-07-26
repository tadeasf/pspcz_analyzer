"""Display tokens for vote codes and vote outcomes.

These tokens are *machine-facing*: Jinja2 templates compare them to
select CSS classes and then render their own localized strings via
gettext (see vote_detail.html, law_detail.html, loyalty_table.html).
They must never be localized here — templates would silently fall
through to their else branches.
"""

from pspcz_analyzer.models.enums import VoteResult

# Per-MP vote code -> display token. vote_detail.html compares
# YES/NO/ABSTAINED/Absent/Excused; any other value (incl. DID_NOT_VOTE)
# renders in the neutral branch.
_MP_VOTE_LABELS: dict[str, str] = {
    VoteResult.YES: "YES",
    VoteResult.NO: "NO",
    VoteResult.ABSTAINED: "ABSTAINED",
    VoteResult.DID_NOT_VOTE: "DID_NOT_VOTE",
    VoteResult.ABSENT: "Absent",
    VoteResult.EXCUSED: "Excused",
}

# Aggregate vote outcome code (hl_hlasovani.vysledek) -> display token.
# law_detail.html compares passed/rejected/void.
_VOTE_OUTCOME_LABELS: dict[str, str] = {
    "A": "passed",
    "R": "rejected",
    "Z": "void",
}


def mp_vote_label(code: str) -> str:
    """Map a psp.cz per-MP vote code (A/B/C/F/@/M) to a display token.

    Args:
        code: Single-character vote result code (may be empty).

    Returns:
        The display token; the raw code for unknown non-empty codes,
        "?" for an empty code.
    """
    if label := _MP_VOTE_LABELS.get(code):
        return label
    return code or "?"


def vote_outcome_label(vysledek: str) -> str:
    """Map an aggregate vote outcome code (A/R/Z) to a display token.

    Args:
        vysledek: Vote outcome code from the votes table.

    Returns:
        "passed" / "rejected" / "void", or the raw code when unknown.
    """
    return _VOTE_OUTCOME_LABELS.get(vysledek, vysledek)

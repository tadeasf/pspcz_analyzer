"""Display tokens for vote codes and vote outcomes.

These tokens are *machine-facing*: Jinja2 templates compare them to
select CSS classes and then render their own localized strings via
gettext (see vote_detail.html, law_detail.html, loyalty_table.html).
They must never be localized here — templates would silently fall
through to their else branches.
"""

from pspcz_analyzer.services.vote_codes import vote_category

# Vote category -> display token. vote_detail.html compares
# YES/NO/ABSTAINED/Absent/Excused; anything else renders in the neutral branch.
_CATEGORY_LABELS: dict[str, str] = {
    "yes": "YES",
    "no": "NO",
    "abstained": "ABSTAINED",
    "absent": "Absent",
    "excused": "Excused",
}

# Aggregate vote outcome code (hl_hlasovani.vysledek) -> display token.
# law_detail.html compares passed/rejected/void.
_VOTE_OUTCOME_LABELS: dict[str, str] = {
    "A": "passed",
    "R": "rejected",
    "Z": "void",
}


def mp_vote_label(code: str) -> str:
    """Map a psp.cz per-MP vote code to a display token via its category.

    Args:
        code: Single-character vote result code (may be empty).

    Returns:
        The display token; the raw code for unknown non-empty codes,
        "?" for an empty code.
    """
    if label := _CATEGORY_LABELS.get(vote_category(code)):
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

"""Resolve inflected Czech submitter names from steno to MP IDs.

Uses difflib fuzzy matching against the mp_info DataFrame to handle
Czech instrumental case inflections (e.g. "Bartošem" → "Bartoš").
"""

import difflib

import polars as pl
from loguru import logger

from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.utils.text import normalize_czech

# Minimum similarity ratio for a name match (handles instrumental case)
_MATCH_THRESHOLD = 0.7


def _match_name_to_mp(
    name: str,
    mp_rows: list[dict],
) -> tuple[int, str] | None:
    """Find the best MP match for a submitter name.

    Args:
        name: Inflected name from steno text (e.g. "Bartošem").
        mp_rows: Pre-computed list of dicts from mp_info.to_dicts().

    Returns:
        (id_poslanec, party) of the best match, or None.
    """
    norm_name = normalize_czech(name)
    best_ratio = 0.0
    best_match: tuple[int, str] | None = None

    for row in mp_rows:
        prijmeni = row.get("prijmeni", "")
        norm_prijmeni = normalize_czech(prijmeni)
        ratio = difflib.SequenceMatcher(None, norm_name, norm_prijmeni).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = (row["id_poslanec"], row.get("party", ""))

    if best_ratio >= _MATCH_THRESHOLD and best_match is not None:
        return best_match
    return None


def resolve_submitter_ids(
    bills: list[BillAmendmentData],
    mp_info: pl.DataFrame,
) -> None:
    """Resolve submitter names to MP IDs and party affiliations.

    Prefers pdf_submitter_names (nominative case, high-confidence match)
    over steno submitter_names (inflected, requires fuzzy matching).
    Resolves ALL names from both sources, not just the first match.

    Mutates AmendmentVote objects in-place, populating submitter_ids
    and submitter_parties fields.

    Args:
        bills: List of bill amendment data with parsed submitter_names.
        mp_info: DataFrame with id_poslanec, prijmeni, party columns.
    """
    mp_rows = mp_info.to_dicts()
    resolved_count = 0

    for bill in bills:
        all_amends = list(bill.amendments)
        if bill.final_vote:
            all_amends.append(bill.final_vote)

        for amend in all_amends:
            # Collect all candidate names: prefer PDF (nominative) then steno (inflected)
            candidate_names = list(amend.pdf_submitter_names) + [
                n for n in amend.submitter_names if n not in amend.pdf_submitter_names
            ]
            for name in candidate_names:
                result = _match_name_to_mp(name, mp_rows)
                if result is not None:
                    mp_id, party = result
                    if mp_id not in amend.submitter_ids:  # deduplicate
                        amend.submitter_ids.append(mp_id)
                        amend.submitter_parties.append(party)
                        resolved_count += 1

    logger.info(
        "[amendment pipeline] Resolved {} submitter names to MP IDs",
        resolved_count,
    )

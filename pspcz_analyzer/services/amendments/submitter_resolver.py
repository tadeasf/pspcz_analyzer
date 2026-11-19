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
# Ratio at which a name match is considered exact — short-circuits the scan
# so ties between several MPs (same surname) can't misattribute the match.
_EXACT_MATCH_RATIO = 0.99


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
        full = normalize_czech(f"{row.get('jmeno', '')} {prijmeni}")
        # Steno has inflected surnames ("Bartošem"), the PDF full names ("Petr
        # Bendl", which scores only 0.67 against the bare surname).
        ratio = max(
            difflib.SequenceMatcher(None, norm_name, normalize_czech(prijmeni)).ratio(),
            difflib.SequenceMatcher(None, norm_name, full).ratio(),
        )
        if ratio >= _EXACT_MATCH_RATIO:
            return (row["id_poslanec"], row.get("party", ""))
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

    Uses pdf_submitter_names (nominative case, high-confidence match) when
    present, otherwise the steno submitter_names (inflected, fuzzy matched).
    Resolves all names of the chosen source, not just the first match.

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
            # PDF names (nominative, from the consolidated document) are
            # authoritative; inflected steno names only when the PDF has none,
            # since fuzzy-matching both gave one submitter two clubs ("napříč").
            candidate_names = list(amend.pdf_submitter_names or amend.submitter_names)
            for name in candidate_names:
                if "výbor" in name.lower():  # committee letter, not an MP
                    continue
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

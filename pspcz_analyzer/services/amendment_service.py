"""Amendment helpers shared by the laws pages and the per-MP vote breakdown.

The amendments section itself (listing, per-law page) is amendment_listing.
"""

from collections import Counter

import polars as pl

from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.affiliation import amendment_driver
from pspcz_analyzer.services.labels import mp_vote_label
from pspcz_analyzer.services.vote_codes import CATEGORY_KEYS, vote_category


def _bill_driver(bill: BillAmendmentData, coalition: frozenset[str]) -> str:
    """Aggregate the gov/opposition driver across all of a bill's amendments."""
    parties = [p for a in bill.amendments for p in a.submitter_parties]
    return amendment_driver(parties, coalition)


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
    mp_detail = mp_rows.join(data.mp_info.drop("party"), on="id_poslanec", how="left")

    # Build party breakdown
    party_breakdown: list[dict] = []
    parties = mp_detail.get_column("party").unique().sort().to_list()
    for party in parties:
        party_df = mp_detail.filter(pl.col("party") == party)
        counts = Counter(vote_category(c) for c in party_df.get_column("vysledek").to_list())
        party_breakdown.append(
            {
                "party": party,
                **{key: counts[key] for key in CATEGORY_KEYS},
                "total": party_df.height,
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
                "vote_label": mp_vote_label(row.get("vysledek", "")),
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

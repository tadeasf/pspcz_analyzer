"""Party loyalty / rebellion analysis."""

from collections import Counter

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.party_voting import party_majority_direction
from pspcz_analyzer.utils.text import matches_tokens, normalize_czech, search_tokens


def _top_bod(votes: list[dict]) -> dict | None:
    """Agenda item with the most votes against the club (repeated votes count once per vote)."""
    counts = Counter((v["schuze"], v["bod"]) for v in votes if v["bod"] and v["bod"] > 0)
    if not counts:
        return None
    (schuze, bod), n = counts.most_common(1)[0]
    name = next(v["nazev_dlouhy"] for v in votes if (v["schuze"], v["bod"]) == (schuze, bod))
    return {"schuze": schuze, "bod": bod, "nazev": name, "count": n}


def filter_sort_page(
    rows: list[dict], search: str, party: str, sort: str, page: int, per_page: int
) -> dict:
    """Filter, sort and paginate loyalty rows for the list views (#115).

    Args:
        rows: Output of compute_loyalty(top=None).
        search: Name search (diacritics-insensitive, word prefixes).
        party: Club abbreviation, empty for all.
        sort: "rate" (highest first), "rate_asc", "name" or "club".
        page: 1-based page.
        per_page: Rows per page (0 = all).

    Returns:
        Dict with rows, total, page, total_pages, first, last.
    """
    tokens = search_tokens(search)
    kept = [
        r
        for r in rows
        if (not party or r["party"] == party)
        and (not tokens or matches_tokens(tokens, r["jmeno"], r["prijmeni"]))
    ]
    match sort:
        case "name":
            kept.sort(
                key=lambda r: (
                    normalize_czech(r["prijmeni"] or ""),
                    normalize_czech(r["jmeno"] or ""),
                )
            )
        case "club":
            kept.sort(key=lambda r: (r["party"] or "", -r["rebellion_pct"]))
        case "rate_asc":
            kept.sort(key=lambda r: r["rebellion_pct"])
        case _:
            kept.sort(key=lambda r: r["rebellion_pct"], reverse=True)
    total = len(kept)
    size = per_page or max(total, 1)
    total_pages = max(1, (total + size - 1) // size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * size
    return {
        "rows": kept[start : start + size],
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "first": start + 1 if total else 0,
        "last": min(start + size, total),
    }


def compute_loyalty(
    data: PeriodData,
    top: int | None = 30,
    party_filter: str | None = None,
) -> list[dict]:
    """Compute rebellion rates for MPs.

    For each vote, determine the party's majority direction (YES vs NO).
    An MP "rebels" when they actively vote against that majority.

    Returns a list of dicts sorted by rebellion rate descending.
    """
    # Exclude void votes
    void_ids = data.void_votes.get_column("id_hlasovani")
    mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    # Only active votes (YES or NO)
    active_results = {VoteResult.YES, VoteResult.NO}
    active_votes = mp_votes.filter(pl.col("vysledek").is_in(active_results))

    # Map id_poslanec -> id_osoba via mp_info; party is the club at the vote date
    active_votes = active_votes.join(
        data.mp_info.select("id_poslanec", "id_osoba"),
        on="id_poslanec",
        how="inner",
    )

    # For each vote + party, compute majority direction
    party_majority = party_majority_direction(active_votes)

    # Join back to individual votes
    with_direction = active_votes.join(
        party_majority.select("id_hlasovani", "party", "party_direction"),
        on=["id_hlasovani", "party"],
        how="inner",
    )

    # Flag rebellions
    with_direction = with_direction.with_columns(
        (pl.col("vysledek") != pl.col("party_direction")).alias("is_rebellion")
    )

    # Collect rebellion vote details before aggregating
    rebellions_df = (
        with_direction.filter(pl.col("is_rebellion"))
        .join(
            data.votes.select("id_hlasovani", "datum", "nazev_dlouhy", "schuze", "bod"),
            on="id_hlasovani",
            how="left",
        )
        .select(
            "id_poslanec",
            "id_hlasovani",
            "datum",
            "nazev_dlouhy",
            "schuze",
            "bod",
            pl.col("vysledek").alias("mp_vote"),
            "party_direction",
        )
    )

    # Build per-MP rebellion vote lists
    rebellion_map: dict[int, list[dict]] = {}
    for row in rebellions_df.iter_rows(named=True):
        mp_id = row["id_poslanec"]
        schuze = row["schuze"]
        bod = row["bod"]
        tisk = data.get_tisk(schuze, bod) if schuze and bod else None
        rebellion_map.setdefault(mp_id, []).append(
            {
                "id_hlasovani": row["id_hlasovani"],
                "datum": row["datum"] or "",
                "nazev_dlouhy": row["nazev_dlouhy"] or "",
                "mp_vote": row["mp_vote"],
                "party_direction": row["party_direction"],
                "schuze": schuze,
                "bod": bod,
                "tisk_url": tisk.url if tisk else None,
            }
        )

    # Aggregate per MP
    per_mp = with_direction.group_by("id_poslanec").agg(
        pl.col("is_rebellion").sum().alias("rebellions"),
        pl.len().alias("active_votes"),
    )

    per_mp = per_mp.with_columns(
        (pl.col("rebellions") / pl.col("active_votes") * 100).alias("rebellion_pct")
    )

    # Join with MP info
    result = per_mp.join(data.mp_info, on="id_poslanec", how="left")

    if party_filter:
        result = result.filter(pl.col("party").str.to_uppercase() == party_filter.upper())

    result = result.sort("rebellion_pct", descending=True)
    if top:
        result = result.head(top)

    rows = result.select(
        "id_poslanec",
        "jmeno",
        "prijmeni",
        "party",
        "active_votes",
        "rebellions",
        "rebellion_pct",
    ).to_dicts()

    # Attach rebellion vote details to each row
    for row in rows:
        votes = rebellion_map.get(row["id_poslanec"], [])
        votes.sort(key=lambda v: v["id_hlasovani"], reverse=True)
        row["rebellion_votes"] = votes
        row["top_bod"] = _top_bod(votes)
        del row["id_poslanec"]

    return rows

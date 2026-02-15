"""Party loyalty / rebellion analysis."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.services.data_service import PeriodData


def compute_loyalty(
    data: PeriodData,
    top: int = 30,
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

    # Map id_poslanec -> id_osoba via mp_info
    active_votes = active_votes.join(
        data.mp_info.select("id_poslanec", "id_osoba", "party"),
        on="id_poslanec",
        how="inner",
    )

    # For each vote + party, compute majority direction
    party_majority = (
        active_votes.group_by(["id_hlasovani", "party"])
        .agg(
            (pl.col("vysledek") == VoteResult.YES).sum().alias("yes_count"),
            (pl.col("vysledek") == VoteResult.NO).sum().alias("no_count"),
        )
        .with_columns(
            pl.when(pl.col("yes_count") > pl.col("no_count"))
            .then(pl.lit(VoteResult.YES))
            .when(pl.col("no_count") > pl.col("yes_count"))
            .then(pl.lit(VoteResult.NO))
            .otherwise(pl.lit(None))
            .alias("party_direction")
        )
        .filter(pl.col("party_direction").is_not_null())
    )

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

    result = result.sort("rebellion_pct", descending=True).head(top)

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
        votes.sort(key=lambda v: v["datum"], reverse=True)
        row["rebellion_votes"] = votes
        del row["id_poslanec"]

    return rows

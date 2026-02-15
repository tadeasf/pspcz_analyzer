"""Attendance / participation rate analysis."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.services.data_service import PeriodData


def compute_attendance(
    data: PeriodData,
    top: int = 30,
    sort: str = "worst",
    party_filter: str | None = None,
) -> list[dict]:
    """Compute attendance rates for MPs.

    Categories:
    - Active: voted YES (A), NO (B), or ABSTAINED (C)
    - Passive: registered but didn't press a button (F)
    - Absent: not registered (@)
    - Excused: excused absence (M)

    Attendance % = active / (total - excused) x 100
    """
    # Exclude void votes
    void_ids = data.void_votes.get_column("id_hlasovani")
    mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    active_set = {VoteResult.YES, VoteResult.NO, VoteResult.ABSTAINED}

    per_mp = mp_votes.group_by("id_poslanec").agg(
        pl.col("vysledek").is_in(active_set).sum().alias("active"),
        (pl.col("vysledek") == VoteResult.YES).sum().alias("yes_votes"),
        (pl.col("vysledek") == VoteResult.NO).sum().alias("no_votes"),
        (pl.col("vysledek") == VoteResult.ABSTAINED).sum().alias("abstained"),
        (pl.col("vysledek") == VoteResult.DID_NOT_VOTE).sum().alias("passive"),
        (pl.col("vysledek") == VoteResult.ABSENT).sum().alias("absent"),
        (pl.col("vysledek") == VoteResult.EXCUSED).sum().alias("excused"),
        pl.len().alias("total"),
    )

    per_mp = per_mp.with_columns(
        (pl.col("active") / (pl.col("total") - pl.col("excused")).cast(pl.Float64) * 100).alias(
            "attendance_pct"
        )
    )

    # Join with MP info
    result = per_mp.join(data.mp_info, on="id_poslanec", how="left")

    if party_filter:
        result = result.filter(pl.col("party").str.to_uppercase() == party_filter.upper())

    if sort == "most_active":
        result = result.sort("active", descending=True).head(top)
    else:
        descending = sort == "best"
        result = result.sort("attendance_pct", descending=descending).head(top)

    return result.select(
        "jmeno",
        "prijmeni",
        "party",
        "active",
        "yes_votes",
        "no_votes",
        "abstained",
        "passive",
        "absent",
        "excused",
        "attendance_pct",
    ).to_dicts()

"""Most active MPs analysis — ranks MPs by participation volume."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.services.data_service import PeriodData


def compute_activity(
    data: PeriodData,
    top: int = 50,
    party_filter: str | None = None,
) -> list[dict]:
    """Rank MPs by total active votes (YES + NO + ABSTAINED).

    Unlike attendance (which shows rates), this ranks by raw volume
    of participation, also showing attendance % for context.
    """
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

    result = per_mp.join(data.mp_info, on="id_poslanec", how="left")

    if party_filter:
        result = result.filter(pl.col("party").str.to_uppercase() == party_filter.upper())

    result = result.sort("active", descending=True).head(top)

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
        "total",
        "attendance_pct",
    ).to_dicts()

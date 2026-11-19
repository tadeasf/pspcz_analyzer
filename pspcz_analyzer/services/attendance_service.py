"""Attendance / participation rate analysis."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.vote_codes import category_count_exprs


def compute_attendance(
    data: PeriodData,
    top: int = 30,
    sort: str = "worst",
    party_filter: str | None = None,
) -> list[dict]:
    """Compute attendance rates for MPs.

    Categories come from ``vote_codes``:
    - Active: logged in, i.e. YES, NO or abstained / did not vote (K)
    - Absent: not logged in (@, or W before the oath)
    - Excused: '@' covered by an omluvy interval (M)

    Attendance % = active / (total - excused - before oath) x 100
    """
    # Exclude void votes
    void_ids = data.void_votes.get_column("id_hlasovani")
    mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    per_mp = (
        mp_votes.group_by("id_poslanec")
        .agg(
            *category_count_exprs(),
            (pl.col("vysledek") == VoteResult.BEFORE_OATH).sum().alias("before_oath"),
            pl.len().alias("total"),
        )
        .rename({"yes": "yes_votes", "no": "no_votes"})
        .with_columns(
            (pl.col("yes_votes") + pl.col("no_votes") + pl.col("abstained")).alias("active")
        )
    )

    # Fully-excused MPs have a zero denominator — active / 0 = inf used to
    # rank them as having PERFECT attendance. Leave the rate undefined (null)
    # instead; sorting uses nulls_last so they never top either ranking.
    denom = (pl.col("total") - pl.col("excused") - pl.col("before_oath")).cast(pl.Float64)
    per_mp = per_mp.with_columns(
        pl.when(denom > 0)
        .then(pl.col("active") / denom * 100)
        .otherwise(None)
        .alias("attendance_pct")
    )

    # Join with MP info
    result = per_mp.join(data.mp_info, on="id_poslanec", how="left")

    if party_filter:
        result = result.filter(pl.col("party").str.to_uppercase() == party_filter.upper())

    # Sort by the requested metric
    sort_config: dict[str, tuple[str, bool]] = {
        "worst": ("attendance_pct", False),
        "best": ("attendance_pct", True),
        "most_active": ("active", True),
        "least_active": ("active", False),
        "most_abstained": ("abstained", True),
        "most_excused": ("excused", True),
        "most_absent": ("absent", True),
        "most_yes": ("yes_votes", True),
        "most_no": ("no_votes", True),
    }
    col, desc = sort_config.get(sort, ("attendance_pct", False))
    result = result.sort(col, descending=desc, nulls_last=True).head(top)

    return result.select(
        "jmeno",
        "prijmeni",
        "party",
        "active",
        "yes_votes",
        "no_votes",
        "abstained",
        "absent",
        "excused",
        "attendance_pct",
    ).to_dicts()

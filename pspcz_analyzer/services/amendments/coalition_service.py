"""Cross-party coalition analysis for amendment votes.

Identifies unusual voting alliances, cross-party amendments, and
party cohesion differences between amendment and regular votes.
"""

import polars as pl
from loguru import logger

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.models.tisk_models import PeriodData


def _compute_party_agreement_on_amendments(
    data: PeriodData,
) -> dict[tuple[str, str], float]:
    """Compute pairwise party agreement rates on amendment votes.

    For each pair of parties, compute the fraction of amendment votes
    where both parties' majority voted the same way.

    Args:
        data: Period data with amendment_data populated.

    Returns:
        Dict mapping (party_a, party_b) → agreement rate (0.0–1.0).
    """
    # Collect all amendment vote IDs
    amend_vote_ids: list[int] = []
    for bill in data.amendment_data.values():
        for amend in bill.amendments:
            if amend.id_hlasovani is not None:
                amend_vote_ids.append(amend.id_hlasovani)

    if not amend_vote_ids:
        return {}

    # Get MP votes for amendment votes only
    mp_votes = data.mp_votes.filter(pl.col("id_hlasovani").is_in(amend_vote_ids))

    # Only active votes (YES or NO)
    active = mp_votes.filter(pl.col("vysledek").is_in({VoteResult.YES, VoteResult.NO}))

    # Join with MP info for party
    active = active.join(
        data.mp_info.select("id_poslanec", "party"),
        on="id_poslanec",
        how="inner",
    )

    # Compute party majority direction per vote
    party_majority = (
        active.group_by(["id_hlasovani", "party"])
        .agg(
            (pl.col("vysledek") == VoteResult.YES).sum().alias("yes_count"),
            (pl.col("vysledek") == VoteResult.NO).sum().alias("no_count"),
        )
        .with_columns(
            pl.when(pl.col("yes_count") > pl.col("no_count"))
            .then(pl.lit("YES"))
            .when(pl.col("no_count") > pl.col("yes_count"))
            .then(pl.lit("NO"))
            .otherwise(pl.lit(None))
            .alias("direction")
        )
        .filter(pl.col("direction").is_not_null())
    )

    # Self-join to get party pairs per vote
    pairs = party_majority.join(
        party_majority,
        on="id_hlasovani",
        suffix="_b",
    ).filter(pl.col("party") < pl.col("party_b"))

    if pairs.height == 0:
        return {}

    # Compute agreement
    agreement = (
        pairs.group_by(["party", "party_b"])
        .agg(
            (pl.col("direction") == pl.col("direction_b")).sum().alias("agree"),
            pl.len().alias("total"),
        )
        .with_columns((pl.col("agree") / pl.col("total")).alias("agreement_rate"))
    )

    result: dict[tuple[str, str], float] = {}
    for row in agreement.iter_rows(named=True):
        result[(row["party"], row["party_b"])] = row["agreement_rate"]

    return result


def _find_amendment_rebels(
    data: PeriodData,
    top_n: int = 20,
) -> list[dict]:
    """Find MPs who rebel more on amendment votes than on regular votes.

    Args:
        data: Period data with amendment_data populated.
        top_n: Number of top rebels to return.

    Returns:
        List of dicts with MP info and amendment/overall rebellion rates.
    """
    # Collect amendment vote IDs
    amend_vote_ids: set[int] = set()
    for bill in data.amendment_data.values():
        for amend in bill.amendments:
            if amend.id_hlasovani is not None:
                amend_vote_ids.add(amend.id_hlasovani)

    if not amend_vote_ids:
        return []

    void_ids = data.void_votes.get_column("id_hlasovani")
    all_mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    # Only active votes
    active = all_mp_votes.filter(pl.col("vysledek").is_in({VoteResult.YES, VoteResult.NO}))
    active = active.join(
        data.mp_info.select("id_poslanec", "party", "jmeno", "prijmeni"),
        on="id_poslanec",
        how="inner",
    )

    # Compute party majority direction for ALL votes
    party_maj = (
        active.group_by(["id_hlasovani", "party"])
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

    with_dir = active.join(
        party_maj.select("id_hlasovani", "party", "party_direction"),
        on=["id_hlasovani", "party"],
        how="inner",
    )

    with_dir = with_dir.with_columns(
        (pl.col("vysledek") != pl.col("party_direction")).alias("is_rebellion"),
        pl.col("id_hlasovani").is_in(amend_vote_ids).alias("is_amendment"),
    )

    # Aggregate per MP: amendment rebellions vs overall
    per_mp = with_dir.group_by("id_poslanec").agg(
        # Overall
        pl.col("is_rebellion").sum().alias("total_rebellions"),
        pl.len().alias("total_votes"),
        # Amendment-only
        (pl.col("is_rebellion") & pl.col("is_amendment")).sum().alias("amend_rebellions"),
        pl.col("is_amendment").sum().alias("amend_votes"),
    )

    per_mp = per_mp.with_columns(
        (pl.col("total_rebellions") / pl.col("total_votes") * 100).alias("overall_rebellion_pct"),
        pl.when(pl.col("amend_votes") > 0)
        .then(pl.col("amend_rebellions") / pl.col("amend_votes") * 100)
        .otherwise(0.0)
        .alias("amend_rebellion_pct"),
    ).with_columns(
        (pl.col("amend_rebellion_pct") - pl.col("overall_rebellion_pct")).alias("rebellion_diff"),
    )

    # Filter: only MPs with at least 5 amendment votes
    per_mp = per_mp.filter(pl.col("amend_votes") >= 5)

    # Sort by rebellion difference (higher = more rebel on amendments)
    per_mp = per_mp.sort("rebellion_diff", descending=True).head(top_n)

    # Join with MP info
    result = per_mp.join(
        data.mp_info.select("id_poslanec", "jmeno", "prijmeni", "party"),
        on="id_poslanec",
        how="left",
    )

    return result.select(
        "jmeno",
        "prijmeni",
        "party",
        "total_votes",
        "total_rebellions",
        "overall_rebellion_pct",
        "amend_votes",
        "amend_rebellions",
        "amend_rebellion_pct",
        "rebellion_diff",
    ).to_dicts()


def _compute_party_cohesion(
    data: PeriodData,
) -> list[dict]:
    """Compute per-party cohesion on amendment votes vs all votes.

    Cohesion = fraction of party members who voted with the majority direction.

    Args:
        data: Period data with amendment_data populated.

    Returns:
        List of dicts with party, amendment_cohesion, overall_cohesion.
    """
    # Collect amendment vote IDs
    amend_vote_ids: set[int] = set()
    for bill in data.amendment_data.values():
        for amend in bill.amendments:
            if amend.id_hlasovani is not None:
                amend_vote_ids.add(amend.id_hlasovani)

    if not amend_vote_ids:
        return []

    void_ids = data.void_votes.get_column("id_hlasovani")
    all_mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    active = all_mp_votes.filter(pl.col("vysledek").is_in({VoteResult.YES, VoteResult.NO}))
    active = active.join(
        data.mp_info.select("id_poslanec", "party"),
        on="id_poslanec",
        how="inner",
    )

    # Party majority direction
    party_maj = (
        active.group_by(["id_hlasovani", "party"])
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

    with_dir = active.join(
        party_maj.select("id_hlasovani", "party", "party_direction"),
        on=["id_hlasovani", "party"],
        how="inner",
    )

    with_dir = with_dir.with_columns(
        (pl.col("vysledek") == pl.col("party_direction")).alias("with_majority"),
        pl.col("id_hlasovani").is_in(amend_vote_ids).alias("is_amendment"),
    )

    # Aggregate per party
    per_party = with_dir.group_by("party").agg(
        # Overall cohesion
        pl.col("with_majority").mean().alias("overall_cohesion"),
        # Amendment cohesion
        pl.when(pl.col("is_amendment"))
        .then(pl.col("with_majority"))
        .otherwise(None)
        .mean()
        .alias("amend_cohesion"),
        # Counts
        pl.len().alias("total_votes"),
        pl.col("is_amendment").sum().alias("amend_votes"),
    )

    per_party = per_party.sort("amend_cohesion", descending=False)

    return per_party.select(
        pl.col("party").alias("party"),
        "overall_cohesion",
        "amend_cohesion",
        "total_votes",
        "amend_votes",
    ).to_dicts()


def compute_amendment_coalitions(
    data: PeriodData,
    top_rebels: int = 20,
) -> dict:
    """Compute all coalition analysis metrics.

    Args:
        data: Period data with amendment_data populated.
        top_rebels: Number of top amendment rebels to return.

    Returns:
        Dict with party_agreement, rebels, party_cohesion, and summary stats.
    """
    if not data.amendment_data:
        return {
            "party_agreement": [],
            "rebels": [],
            "party_cohesion": [],
            "total_bills": 0,
            "total_amendments": 0,
        }

    total_bills = len(data.amendment_data)
    total_amendments = sum(b.amendment_count for b in data.amendment_data.values())

    # Party agreement
    raw_agreement = _compute_party_agreement_on_amendments(data)
    party_agreement = [
        {"party_a": k[0], "party_b": k[1], "agreement_rate": v}
        for k, v in sorted(raw_agreement.items(), key=lambda x: x[1], reverse=True)
    ]

    # Amendment rebels
    rebels = _find_amendment_rebels(data, top_rebels)

    # Party cohesion
    party_cohesion = _compute_party_cohesion(data)

    logger.debug(
        "Coalition analysis: {} bills, {} amendments, {} party pairs, {} rebels",
        total_bills,
        total_amendments,
        len(party_agreement),
        len(rebels),
    )

    return {
        "party_agreement": party_agreement,
        "rebels": rebels,
        "party_cohesion": party_cohesion,
        "total_bills": total_bills,
        "total_amendments": total_amendments,
    }

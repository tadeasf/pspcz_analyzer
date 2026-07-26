"""Shared polars helpers for party-level voting analysis."""

from collections.abc import Sequence

import polars as pl

from pspcz_analyzer.models.enums import VoteResult

_DEFAULT_GROUP_KEYS: tuple[str, str] = ("id_hlasovani", "party")


def party_majority_direction(
    votes: pl.DataFrame,
    group_keys: Sequence[str] = _DEFAULT_GROUP_KEYS,
    direction_alias: str = "party_direction",
) -> pl.DataFrame:
    """Compute each group's majority direction (YES vs NO).

    A group's direction is YES when its YES votes outnumber its NO votes
    and vice versa; exact ties carry no direction and are dropped.
    Shared by loyalty (rebellion detection) and the amendment coalition
    analysis so the two can never drift apart.

    Args:
        votes: Frame with a "vysledek" column holding VoteResult values
            plus one column per group key.
        group_keys: Columns identifying a group (default vote + party).
        direction_alias: Name of the resulting direction column; its
            values are VoteResult.YES / VoteResult.NO.

    Returns:
        Grouped frame with yes_count, no_count, and the direction
        column; tied (direction-less) groups are filtered out.
    """
    return (
        votes.group_by(list(group_keys))
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
            .alias(direction_alias)
        )
        .filter(pl.col(direction_alias).is_not_null())
    )

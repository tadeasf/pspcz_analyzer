"""Automatic government-coalition detection from the investiture confidence vote.

The sitting government is read from the chamber's confidence vote ("Vyslovení
důvěry vládě"): the clubs whose MPs voted YES form the governing coalition, the
rest are opposition. This is exact for a fresh period (the investiture vote is
recent and club memberships have not yet drifted).

A polarization guard keeps the result trustworthy: a coalition is only emitted
when a *passed* confidence vote splits the chamber into a clear majority bloc vs
an opposing bloc. Near-unanimous procedural votes (which share the agenda-item
name) and label-scrambled historical votes fail the guard, so the detector
returns an empty set ("unknown") rather than a wrong answer.
"""

from __future__ import annotations

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    CONFIDENCE_MAX_YES_SHARE,
    CONFIDENCE_NO_BLOC,
    CONFIDENCE_YES_BLOC,
    INDEPENDENT_CLUB_MARKERS,
)


def _confidence_vote_candidates(votes: pl.DataFrame) -> pl.DataFrame:
    """Return passed confidence votes (excludes no-confidence motions).

    Matches the agenda-item name "vyslovení důvěry" but not "nedůvěr" (a
    no-confidence motion inverts the meaning of a YES vote).
    """
    name_col = "nazev_dlouhy" if "nazev_dlouhy" in votes.columns else "nazev_kratky"
    name = pl.col(name_col).fill_null("").str.to_lowercase()
    return votes.filter(
        name.str.contains("vyslovení důvěry")
        & ~name.str.contains("nedůvěr")
        & (pl.col("vysledek") == "A")
    )


def _club_yes_fractions(
    vote_id: int,
    mp_votes: pl.DataFrame,
    mp_info: pl.DataFrame,
) -> pl.DataFrame:
    """Per-club (party, seats n, yes count, yes_frac) for a single vote."""
    joined = mp_votes.filter(pl.col("id_hlasovani") == vote_id).join(
        mp_info.select(["id_poslanec", "party"]), on="id_poslanec", how="left"
    )
    return (
        joined.filter(pl.col("party").is_not_null())
        .group_by("party")
        .agg(
            pl.len().alias("n"),
            (pl.col("vysledek") == "A").sum().alias("yes"),
        )
        .with_columns((pl.col("yes") / pl.col("n")).alias("yes_frac"))
    )


def _is_independent(party: str) -> bool:
    """Return True if *party* denotes a non-affiliated (independent) club."""
    return any(marker in party for marker in INDEPENDENT_CLUB_MARKERS)


def _coalition_from_split(split: pl.DataFrame) -> frozenset[str]:
    """Apply the polarization guard and return the coalition club set.

    Returns an empty set when the vote is not a clean, contested, majority
    confidence split.
    """
    if split.is_empty():
        return frozenset()

    total_seats = int(split.get_column("n").sum())
    total_yes = int(split.get_column("yes").sum())

    # Genuinely contested: reject near-unanimous votes (procedural votes that
    # share the confidence agenda-item name, or uncontested caretaker votes).
    if total_seats == 0 or total_yes / total_seats > CONFIDENCE_MAX_YES_SHARE:
        return frozenset()

    yes_bloc = split.filter(pl.col("yes_frac") >= CONFIDENCE_YES_BLOC)
    no_bloc = split.filter(pl.col("yes_frac") <= CONFIDENCE_NO_BLOC)

    # Contested: a clear supporting bloc AND a clear opposing bloc must exist.
    if yes_bloc.is_empty() or no_bloc.is_empty():
        return frozenset()

    coalition_rows = split.filter(pl.col("yes_frac") > 0.5)
    coalition_seats = int(coalition_rows.get_column("n").sum())
    # The coalition must hold a seat majority (a real government).
    if coalition_seats * 2 <= total_seats:
        return frozenset()

    return frozenset(
        party
        for party in coalition_rows.get_column("party").to_list()
        if party and not _is_independent(party)
    )


def detect_coalition_parties(
    votes: pl.DataFrame,
    mp_votes: pl.DataFrame,
    mp_info: pl.DataFrame,
) -> frozenset[str]:
    """Detect the governing coalition's clubs from the confidence vote.

    Args:
        votes: Vote records (needs `id_hlasovani`, `vysledek`, a name column, `datum`).
        mp_votes: Per-MP vote results (`id_hlasovani`, `id_poslanec`, `vysledek`).
        mp_info: MP info with `id_poslanec` and normalized `party`.

    Returns:
        Frozenset of government-coalition club abbreviations, or an empty
        frozenset when no confidence vote could be confidently identified.
    """
    candidates = _confidence_vote_candidates(votes)
    if candidates.is_empty():
        return frozenset()

    # id_hlasovani is assigned sequentially over time, so the highest id is the
    # most recent confidence vote = the current government.
    candidates = candidates.sort("id_hlasovani", descending=True)

    for vote_id in candidates.get_column("id_hlasovani").to_list():
        split = _club_yes_fractions(vote_id, mp_votes, mp_info)
        coalition = _coalition_from_split(split)
        if coalition:
            logger.info(
                "[coalition] Detected coalition from confidence vote {}: {}",
                vote_id,
                ", ".join(sorted(coalition)),
            )
            return coalition

    logger.info("[coalition] No confident coalition detected (left unknown)")
    return frozenset()

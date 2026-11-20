"""Build MP info (id_poslanec -> name, current club) and per-vote clubs for a period."""

import polars as pl

from pspcz_analyzer.config import PERIOD_ORGAN_IDS
from pspcz_analyzer.models.enums import PARLIAMENTARY_CLUB_TYPE

# psp.cz abbreviations -> commonly used names. "ANO2011" is the official
# registration name; "Nezařaz" is the truncated abbreviation for independents.
_PARTY_ALIASES = {
    "ANO2011": "ANO",
    "Nezařaz": "Nezařazení",
}


def _club_memberships(period: int, organs: pl.DataFrame, memberships: pl.DataFrame) -> pl.DataFrame:
    """Club memberships of this period: id_osoba, party, od (date), do (date or null).

    Only clubs of this period's chamber and only membership rows
    (cl_funkce 0); function rows carry a function id in id_of that can
    collide with an organ id.
    """
    clubs = organs.filter(
        (pl.col("id_typ_organu") == PARLIAMENTARY_CLUB_TYPE)
        & (pl.col("organ_id_organ") == PERIOD_ORGAN_IDS[period])
    ).select("id_organ", pl.col("zkratka").replace(_PARTY_ALIASES).alias("party"))
    return (
        memberships.filter(pl.col("cl_funkce").cast(pl.String) == "0")
        .join(clubs, left_on="id_of", right_on="id_organ", how="inner")
        .select(
            "id_osoba",
            "party",
            pl.col("od_o").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("od"),
            pl.col("do_o").str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).alias("do"),
        )
        .drop_nulls("od")
    )


def build_mp_info(
    period: int,
    mps: pl.DataFrame,
    persons: pl.DataFrame,
    organs: pl.DataFrame,
    memberships: pl.DataFrame,
) -> pl.DataFrame:
    """Build MP lookup table: id_poslanec -> name and latest club within the period.

    Args:
        period: Electoral period number.
        mps: poslanec table.
        persons: osoby table.
        organs: organy table.
        memberships: zarazeni table.

    Returns:
        DataFrame with id_poslanec, id_osoba, jmeno, prijmeni, party (and pohlavi
        when the persons table has it).
    """
    organ_id = PERIOD_ORGAN_IDS[period]
    period_mps = mps.filter(pl.col("id_obdobi") == organ_id)

    gender = [c for c in ("pohlavi",) if c in persons.columns]  # "M", else a woman
    mp_persons = period_mps.join(
        persons.select("id_osoba", "jmeno", "prijmeni", *gender),
        on="id_osoba",
        how="left",
    )
    latest_club = (
        _club_memberships(period, organs, memberships)
        .sort("od", descending=True)
        .unique(subset=["id_osoba"], keep="first")
    )
    return mp_persons.join(
        latest_club.select("id_osoba", "party"), on="id_osoba", how="left"
    ).select("id_poslanec", "id_osoba", "jmeno", "prijmeni", "party", *gender)


def add_vote_parties(
    period: int,
    mp_votes: pl.DataFrame,
    votes: pl.DataFrame,
    mp_info: pl.DataFrame,
    organs: pl.DataFrame,
    memberships: pl.DataFrame,
) -> pl.DataFrame:
    """Add ``party``: the MP's club on the day of each vote (#101).

    The club is the latest membership that started on or before the vote
    date and had not ended before it. MPs with no such membership fall back
    to their latest club in the period (mp_info.party).

    Args:
        period: Electoral period number.
        mp_votes: hl_poslanec rows (id_poslanec, id_hlasovani, vysledek).
        votes: hl_hlasovani rows (id_hlasovani, datum).
        mp_info: Output of build_mp_info.
        organs: organy table.
        memberships: zarazeni table.

    Returns:
        mp_votes with the same rows and order plus a ``party`` column.
    """
    vote_dates = votes.select(
        "id_hlasovani", pl.col("datum").str.to_date("%d.%m.%Y", strict=False).alias("d")
    )
    keys = (
        mp_votes.select("id_poslanec", "id_hlasovani")
        .join(vote_dates, on="id_hlasovani", how="left")
        .join(
            mp_info.select("id_poslanec", "id_osoba", pl.col("party").alias("fallback")),
            on="id_poslanec",
            how="left",
        )
        .select("id_poslanec", "id_osoba", "d", "fallback")
        .unique()
        .drop_nulls(["id_osoba", "d"])
        .sort("d")
    )
    clubs = _club_memberships(period, organs, memberships).sort("od")
    # both sides are globally sorted by date, hence also within each id_osoba group
    at_date = keys.join_asof(
        clubs,
        left_on="d",
        right_on="od",
        by="id_osoba",
        strategy="backward",
        check_sortedness=False,
    )
    at_date = at_date.select(
        "id_poslanec",
        "d",
        pl.when(pl.col("do").is_null() | (pl.col("do") >= pl.col("d")))
        .then(pl.col("party"))
        .otherwise(None)
        .fill_null(pl.col("fallback"))
        .alias("party"),
    )
    return (
        mp_votes.join(vote_dates, on="id_hlasovani", how="left", maintain_order="left")
        .join(at_date, on=["id_poslanec", "d"], how="left", maintain_order="left")
        .drop("d")
    )

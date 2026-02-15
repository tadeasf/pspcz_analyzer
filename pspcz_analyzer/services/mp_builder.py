"""Build MP info table: id_poslanec -> name, party for a given period."""

import polars as pl

from pspcz_analyzer.config import PERIOD_ORGAN_IDS


def build_mp_info(
    period: int,
    mps: pl.DataFrame,
    persons: pl.DataFrame,
    organs: pl.DataFrame,
    memberships: pl.DataFrame,
) -> pl.DataFrame:
    """Build MP lookup table: id_poslanec -> name, party for a given period."""
    organ_id = PERIOD_ORGAN_IDS[period]
    period_mps = mps.filter(pl.col("id_obdobi") == organ_id)

    mp_persons = period_mps.join(
        persons.select("id_osoba", "jmeno", "prijmeni"),
        on="id_osoba",
        how="left",
    )

    clubs = organs.filter(pl.col("id_typ_organu") == 1).select("id_organ", "zkratka")

    club_memberships = memberships.join(
        clubs, left_on="id_of", right_on="id_organ", how="inner"
    ).select("id_osoba", "zkratka", "od_o", "do_o")

    club_memberships = club_memberships.sort("od_o", descending=True).unique(
        subset=["id_osoba"], keep="first"
    )

    mp_info = mp_persons.join(
        club_memberships.select("id_osoba", pl.col("zkratka").alias("party")),
        on="id_osoba",
        how="left",
    ).select("id_poslanec", "id_osoba", "jmeno", "prijmeni", "party")

    # Normalize party abbreviations from psp.cz to commonly used names.
    # "ANO2011" is the official registration name but everyone calls it "ANO".
    # "Nezařaz" is the truncated abbreviation for independent MPs ("Nezařazení").
    party_aliases = {
        "ANO2011": "ANO",
        "Nezařaz": "Nezařazení",
    }
    return mp_info.with_columns(pl.col("party").replace(party_aliases).alias("party"))

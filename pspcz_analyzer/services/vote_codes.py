"""One place for per-MP vote codes: display categories, excuses, sanity check.

psp.cz codes (https://public.psp.cz/sqw/hp.sqw?k=1302): A yes, B/N no,
C abstained, F did not vote, K abstained-or-did-not-vote (the only form
since 1995), @ not logged in, W not logged in before the oath, M excused.
Excuses are not in hl_poslanec: an '@' whose vote time falls inside an
omluvy interval becomes 'M'.
"""

import polars as pl
from loguru import logger

from pspcz_analyzer.models.enums import VoteResult

CATEGORY_KEYS: tuple[str, ...] = ("yes", "no", "abstained", "absent", "excused", "other")

VOTE_CATEGORIES: dict[str, str] = {
    VoteResult.YES: "yes",
    VoteResult.NO: "no",
    VoteResult.NO_ALT: "no",
    VoteResult.ABSTAINED: "abstained",
    VoteResult.DID_NOT_VOTE: "abstained",
    VoteResult.ABSTAIN_ALT: "abstained",
    VoteResult.ABSENT: "absent",
    VoteResult.BEFORE_OATH: "absent",
    VoteResult.EXCUSED: "excused",
}


def vote_category(code: str) -> str:
    """Map one vote code to its display category ("other" if unknown).

    Args:
        code: psp.cz per-MP vote code.

    Returns:
        One of CATEGORY_KEYS.
    """
    return VOTE_CATEGORIES.get(code, "other")


def category_count_exprs(col: str = "vysledek") -> list[pl.Expr]:
    """Aggregation expressions counting each category, for use in ``agg``.

    Args:
        col: Column holding the vote code.

    Returns:
        One summed boolean expression per CATEGORY_KEYS entry, aliased to it.
    """
    cat = pl.col(col).replace_strict(VOTE_CATEGORIES, default="other", return_dtype=pl.String)
    return [(cat == key).sum().alias(key) for key in CATEGORY_KEYS]


def _excused_keys(
    mp_votes: pl.DataFrame, votes: pl.DataFrame, excuses: pl.DataFrame
) -> pl.DataFrame:
    """(id_hlasovani, id_poslanec) of '@' records covered by an excuse."""
    absent = mp_votes.filter(pl.col("vysledek") == VoteResult.ABSENT).join(
        votes.select("id_hlasovani", "datum", "cas"), on="id_hlasovani"
    )
    matched = absent.join(
        excuses.select("id_poslanec", "den", "od", "do"),
        left_on=["id_poslanec", "datum"],
        right_on=["id_poslanec", "den"],
    )
    # "HH:MM" strings compare correctly; null od/do = whole sitting day.
    in_window = pl.col("od").is_null() | (
        (pl.col("cas") >= pl.col("od")) & (pl.col("cas") <= pl.col("do"))
    )
    return matched.filter(in_window).select("id_hlasovani", "id_poslanec").unique()


def normalize_vote_codes(
    mp_votes: pl.DataFrame, votes: pl.DataFrame, excuses: pl.DataFrame
) -> pl.DataFrame:
    """Fold 'N' into 'B' and turn excused '@' records into 'M'.

    Args:
        mp_votes: hl_poslanec rows (id_poslanec, id_hlasovani, vysledek).
        votes: hl_hlasovani rows (needs id_hlasovani, datum, cas).
        excuses: omluvy rows for the period (id_poslanec, den, od, do).

    Returns:
        mp_votes with the same rows and order, codes normalised.
    """
    keys = _excused_keys(mp_votes, votes, excuses).with_columns(pl.lit(True).alias("_excused"))
    return (
        mp_votes.join(keys, on=["id_hlasovani", "id_poslanec"], how="left", maintain_order="left")
        .with_columns(
            pl.when(pl.col("_excused"))
            .then(pl.lit(VoteResult.EXCUSED.value))
            .when(pl.col("vysledek") == VoteResult.NO_ALT)
            .then(pl.lit(VoteResult.NO.value))
            .otherwise(pl.col("vysledek"))
            .alias("vysledek")
        )
        .drop("_excused")
    )


def check_vote_totals(period: int, votes: pl.DataFrame, mp_votes: pl.DataFrame) -> int:
    """Log when per-MP records disagree with the hl_hlasovani summary.

    Checks pro = yes, proti = no, zdrzel + nehlasoval = abstained and
    prihlaseno = yes + no + abstained, and reports unknown codes.

    Args:
        period: Period number (for the log line).
        votes: hl_hlasovani rows.
        mp_votes: Normalised hl_poslanec rows.

    Returns:
        Number of votes whose totals disagree.
    """
    unknown = mp_votes.filter(~pl.col("vysledek").is_in(list(VOTE_CATEGORIES))).get_column(
        "vysledek"
    )
    if unknown.len():
        logger.error(
            "Period {}: {} vote records with unknown codes {}",
            period,
            unknown.len(),
            sorted(unknown.unique().to_list()),
        )
    counts = mp_votes.group_by("id_hlasovani").agg(category_count_exprs())
    joined = votes.join(counts, on="id_hlasovani")
    bad = joined.filter(
        (pl.col("pro") != pl.col("yes"))
        | (pl.col("proti") != pl.col("no"))
        | (pl.col("zdrzel") + pl.col("nehlasoval") != pl.col("abstained"))
        | (pl.col("prihlaseno") != pl.col("yes") + pl.col("no") + pl.col("abstained"))
    )
    if bad.height:  # a handful per period are inconsistencies in the psp.cz source itself
        logger.warning(
            "Period {}: {} votes where MP records disagree with the summary, e.g. {}",
            period,
            bad.height,
            bad.get_column("id_hlasovani").head(5).to_list(),
        )
    return bad.height

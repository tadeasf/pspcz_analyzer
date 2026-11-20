"""Attendance / participation rate analysis (#91, #117)."""

import polars as pl

from pspcz_analyzer.config import ATTENDANCE_MIN_VOTES
from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.vote_codes import category_count_exprs, vote_category
from pspcz_analyzer.utils.text import matches_tokens, search_tokens

_SORTS: dict[str, tuple[str, bool]] = {
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


def _mandate_bounds(data: PeriodData, mp_votes: pl.DataFrame) -> pl.DataFrame:
    """Per MP: first/last vote date, and whether the mandate covers the whole period."""
    span = mp_votes.group_by("id_poslanec").agg(
        pl.col("id_hlasovani").min().alias("first_id"),
        pl.col("id_hlasovani").max().alias("last_id"),
    )
    dates = data.votes.select("id_hlasovani", "datum")
    first_all, last_all = (
        mp_votes.get_column("id_hlasovani").min(),
        mp_votes.get_column("id_hlasovani").max(),
    )
    return (
        span.join(
            dates.rename({"id_hlasovani": "first_id", "datum": "mandate_from"}),
            on="first_id",
            how="left",
        )
        .join(
            dates.rename({"id_hlasovani": "last_id", "datum": "mandate_to"}),
            on="last_id",
            how="left",
        )
        .with_columns(
            (pl.col("first_id") == first_all).alias("from_start"),
            (pl.col("last_id") == last_all).alias("to_end"),
        )
        .drop("first_id", "last_id")
    )


def compute_attendance(
    data: PeriodData,
    top: int | None = 30,
    sort: str = "worst",
    party_filter: str | None = None,
) -> list[dict]:
    """Compute attendance per MP.

    Attendance % = logged in (yes, no, abstained / did not vote) / all votes
    during the mandate, without void votes and votes before the oath (W).
    Excused absences are counted separately. MPs with fewer than
    ATTENDANCE_MIN_VOTES votes are flagged ``small_sample`` and sorted last.

    Args:
        data: Period data.
        top: Rows to return (None = all).
        sort: Key of _SORTS.
        party_filter: Club abbreviation (case-insensitive), or None.

    Returns:
        Rows with name, party, counts, attendance_pct, mandate bounds and small_sample.
    """
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
            (pl.col("yes_votes") + pl.col("no_votes") + pl.col("abstained")).alias("active"),
            (pl.col("total") - pl.col("before_oath")).alias("mandate_votes"),
        )
        .with_columns(
            pl.when(pl.col("mandate_votes") > 0)
            .then(pl.col("active") / pl.col("mandate_votes") * 100)
            .otherwise(None)
            .alias("attendance_pct"),
            (pl.col("mandate_votes") < ATTENDANCE_MIN_VOTES).alias("small_sample"),
        )
        .join(_mandate_bounds(data, mp_votes), on="id_poslanec", how="left")
    )

    result = per_mp.join(data.mp_info, on="id_poslanec", how="left")
    if party_filter:
        result = result.filter(pl.col("party").str.to_uppercase() == party_filter.upper())

    col, desc = _SORTS.get(sort, _SORTS["worst"])
    result = result.sort(["small_sample", col], descending=[False, desc], nulls_last=True)
    if top:
        result = result.head(top)

    return result.select(
        "id_poslanec",
        "jmeno",
        "prijmeni",
        "party",
        "active",
        "yes_votes",
        "no_votes",
        "abstained",
        "absent",
        "excused",
        "mandate_votes",
        "attendance_pct",
        "small_sample",
        "mandate_from",
        "mandate_to",
        "from_start",
        "to_end",
    ).to_dicts()


def attendance_view(rows: list[dict], search: str, page: int, per_page: int) -> dict:
    """Name search and paging over already sorted/filtered attendance rows.

    Args:
        rows: Output of compute_attendance(top=None).
        search: Name search (diacritics-insensitive, word prefixes).
        page: 1-based page.
        per_page: Rows per page (0 = all).

    Returns:
        Dict with rows, total, page, total_pages, first, last.
    """
    tokens = search_tokens(search)
    kept = [r for r in rows if not tokens or matches_tokens(tokens, r["jmeno"], r["prijmeni"])]
    total = len(kept)
    size = per_page or max(total, 1)
    total_pages = max(1, (total + size - 1) // size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * size
    return {
        "rows": kept[start : start + size],
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "first": start + 1 if total else 0,
        "last": min(start + size, total),
    }


def club_strips(rows: list[dict]) -> dict:
    """Per-club dots and medians for the 'attendance by club' chart (#117).

    Args:
        rows: Output of compute_attendance(top=None).

    Returns:
        {"clubs": [{party, points: [{name, pct, id}], median}], "axis_min": int}.
    """
    ranked = [r for r in rows if not r["small_sample"] and r["attendance_pct"] is not None]
    clubs: dict[str, list[dict]] = {}
    for r in ranked:
        clubs.setdefault(r["party"] or "?", []).append(
            {
                "name": f"{r['jmeno']} {r['prijmeni']}",
                "pct": r["attendance_pct"],
                "id": r["id_poslanec"],
            }
        )
    out = []
    for party, pts in sorted(clubs.items(), key=lambda kv: -len(kv[1])):
        vals = sorted(p["pct"] for p in pts)
        mid = len(vals) // 2
        median = vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2
        out.append({"party": party, "points": pts, "median": median})
    lowest = min((p["pct"] for c in out for p in c["points"]), default=0.0)
    axis_min = max(0, int(lowest // 10) * 10)
    return {"clubs": out, "axis_min": axis_min}


MP_VOTE_CATEGORIES = ("present", "yes", "no", "abstained", "absent", "excused")
_PRESENT = ("yes", "no", "abstained")


def mp_votes_list(
    data: PeriodData, id_poslanec: int, category: str, page: int = 1, per_page: int = 50
) -> dict | None:
    """Votes of one MP in one attendance category, for "každé číslo vede na seznam" (#117, #91).

    Args:
        data: Period data.
        id_poslanec: The MP (id_poslanec of the period).
        category: One of MP_VOTE_CATEGORIES ("present" = yes + no + abstained).
        page: 1-based page.
        per_page: Votes per page.

    Returns:
        Dict with ``mp``, ``counts`` per category, ``rows`` of the page and paging,
        or None when the MP is not in the period.
    """
    info = data.mp_info.filter(pl.col("id_poslanec") == id_poslanec)
    if info.height == 0:
        return None
    void_ids = data.void_votes.get_column("id_hlasovani")
    mine = data.mp_votes.filter(
        (pl.col("id_poslanec") == id_poslanec)
        & ~pl.col("id_hlasovani").is_in(void_ids)
        & (pl.col("vysledek") != VoteResult.BEFORE_OATH)
    ).with_columns(
        pl.col("vysledek").map_elements(vote_category, return_dtype=pl.String).alias("cat")
    )
    counts = {k: int((mine["cat"] == k).sum()) for k in MP_VOTE_CATEGORIES[1:]}
    counts["present"] = sum(counts[k] for k in _PRESENT)
    cats = _PRESENT if category == "present" else (category,)
    picked = (
        mine.filter(pl.col("cat").is_in(list(cats)))
        .join(data.votes, on="id_hlasovani", how="inner")
        .sort("id_hlasovani", descending=True)
    )
    total_pages = max(1, (picked.height + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    rows = picked.slice((page - 1) * per_page, per_page).to_dicts()
    for r in rows:
        rec = data.vote_steno.get(r["id_hlasovani"]) or {}
        r["question"] = rec.get("question", "")
    return {
        "mp": info.row(0, named=True),
        "counts": counts,
        "category": category,
        "rows": rows,
        "total": picked.height,
        "page": page,
        "total_pages": total_pages,
    }

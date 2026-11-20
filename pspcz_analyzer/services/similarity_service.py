"""Voting similarity analysis with PCA clustering."""

from collections.abc import Callable

import numpy as np
import polars as pl

from pspcz_analyzer.config import CLUB_COLOR_OTHER, CLUB_COLORS, SIMILARITY_MIN_SHARED_VOTES
from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.affiliation import GOVERNMENT, OPPOSITION, classify_party
from pspcz_analyzer.services.coalition_table import parties_at
from pspcz_analyzer.services.vote_codes import vote_category

_VOTED = ("yes", "no", "abstained")


def _build_vote_matrix(data: PeriodData) -> tuple[np.ndarray, pl.DataFrame]:
    """Build a vote matrix (MPs x votes).

    Values: +1 = YES, -1 = NO, 0 = anything else.
    Returns the matrix and the MP info DataFrame (aligned by row index).
    """
    # Exclude void votes
    void_ids = data.void_votes.get_column("id_hlasovani")
    mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    # Encode votes as numeric
    mp_votes = mp_votes.with_columns(
        pl.when(pl.col("vysledek") == VoteResult.YES)
        .then(1)
        .when(pl.col("vysledek") == VoteResult.NO)
        .then(-1)
        .otherwise(0)
        .alias("vote_num")
    )

    # Pivot: rows=MPs, cols=votes
    pivot = mp_votes.pivot(
        on="id_hlasovani",
        index="id_poslanec",
        values="vote_num",
        aggregate_function="first",
    ).fill_null(0)

    mp_ids = pivot.select("id_poslanec")
    matrix = pivot.drop("id_poslanec").to_numpy().astype(np.float32)

    # Align MP info
    mp_info = mp_ids.join(data.mp_info, on="id_poslanec", how="left")

    return matrix, mp_info


def compute_pca_coords(data: PeriodData) -> list[dict]:
    """Compute 2D PCA coordinates for each MP based on voting patterns.

    Returns list of dicts with: mp_name, party, x, y
    """
    matrix, mp_info = _build_vote_matrix(data)

    # Center the data
    matrix_centered = matrix - matrix.mean(axis=0)

    # SVD-based PCA (no sklearn needed)
    U, S, Vt = np.linalg.svd(matrix_centered, full_matrices=False)
    coords_2d = U[:, :2] * S[:2]

    names = (mp_info.get_column("jmeno") + " " + mp_info.get_column("prijmeni")).to_list()
    parties = mp_info.get_column("party").to_list()

    return [
        {
            "mp_name": names[i],
            "party": parties[i] or "N/A",
            "x": float(coords_2d[i, 0]),
            "y": float(coords_2d[i, 1]),
        }
        for i in range(len(names))
    ]


def _indicator_matrices(
    mp_votes: pl.DataFrame, mp_ids: list[int], vote_ids: list[int]
) -> dict[str, np.ndarray]:
    """0/1 matrices (MPs x votes) for yes, no and abstained-or-did-not-vote."""
    row = {m: k for k, m in enumerate(mp_ids)}
    col = {v: k for k, v in enumerate(vote_ids)}
    mats = {c: np.zeros((len(mp_ids), len(vote_ids)), dtype=np.float32) for c in _VOTED}
    for mp, vote, cat in mp_votes.select("id_poslanec", "id_hlasovani", "cat").iter_rows():
        if cat in mats:
            mats[cat][row[mp], col[vote]] = 1.0
    return mats


def _agreement_counts(
    left: dict[str, np.ndarray], right: dict[str, np.ndarray]
) -> tuple[np.ndarray, np.ndarray]:
    """(same, both): votes where the pair voted alike / where both voted."""
    same = left["yes"] @ right["yes"].T + left["no"] @ right["no"].T
    same += left["abstained"] @ right["abstained"].T
    p_left = left["yes"] + left["no"] + left["abstained"]
    p_right = right["yes"] + right["no"] + right["abstained"]
    return same, p_left @ p_right.T


def _cross_club_counts(
    mp_votes: pl.DataFrame, mp_ids: list[int], vote_ids: list[int]
) -> tuple[np.ndarray, np.ndarray]:
    """(same, both) over the votes where the pair sat in different clubs that day (#101).

    All votes minus, for each club, the votes where both were in that club.
    """
    same, both = _agreement_counts(*[_indicator_matrices(mp_votes, mp_ids, vote_ids)] * 2)
    for club in mp_votes.get_column("party").unique().to_list():
        rows = mp_votes.filter(
            pl.col("party").is_null() if club is None else pl.col("party") == club
        )
        m = _indicator_matrices(rows, mp_ids, vote_ids)
        s_c, b_c = _agreement_counts(m, m)
        same, both = same - s_c, both - b_c
    return same, both


def _side_per_record(data: PeriodData, mp_votes: pl.DataFrame) -> pl.Series:
    """government / opposition / unknown for each record, by club and government on the vote date."""
    vote_day = data.votes.select(
        "id_hlasovani", pl.col("datum").str.to_date("%d.%m.%Y", strict=False).alias("day")
    )
    days = mp_votes.join(vote_day, on="id_hlasovani", how="left", maintain_order="left")
    cache: dict[tuple[str | None, object], str] = {}
    sides = []
    for party, day in days.select("party", "day").iter_rows():
        key = (party, day)
        if key not in cache:
            coalition = (
                parties_at(data.coalition_spans, day)
                if data.coalition_spans and day
                else data.coalition_parties
            )
            cache[key] = classify_party(party or "", coalition)
        sides.append(cache[key])
    return pl.Series(sides)


def _pairs(
    same: np.ndarray,
    both: np.ndarray,
    names: list[str],
    parties: list[str | None],
    keep: Callable[[int, int], bool],
    top: int | None,
) -> list[dict]:
    """Top pairs by agreement among (i, j) with keep(i, j) and enough shared votes."""
    pairs = []
    n = len(names)
    for i in range(n):
        for j in range(i + 1, n):
            shared = int(both[i, j])
            if shared >= SIMILARITY_MIN_SHARED_VOTES and keep(i, j):
                pairs.append(
                    {
                        "mp1_name": names[i],
                        "mp1_party": parties[i],
                        "mp2_name": names[j],
                        "mp2_party": parties[j],
                        "same": int(same[i, j]),
                        "shared": shared,
                        "agreement": float(same[i, j]) / shared,
                    }
                )
    pairs.sort(key=lambda p: (p["agreement"], p["shared"]), reverse=True)
    return pairs[:top] if top else pairs


def compute_cross_party_similarity(data: PeriodData, top: int | None = 20) -> dict[str, list[dict]]:
    """Most-agreeing MP pairs from different clubs and across government/opposition (#104).

    Agreement = votes where both voted the same way / votes where both voted
    (yes, no, or abstained-or-did-not-vote; not logged in and excused do not
    count). The cross-club table counts only votes where the two MPs were in
    different clubs on that day. Pairs need SIMILARITY_MIN_SHARED_VOTES shared votes. The
    government x opposition table counts only votes where one MP's club was
    in government and the other's in opposition on that day.

    Args:
        data: Period data.
        top: Pairs per table (None = every eligible pair).

    Returns:
        {"cross_party": [...], "gov_opp": [...]}; each pair has names, parties,
        same, shared and agreement (0-1).
    """
    void_ids = data.void_votes.get_column("id_hlasovani")
    mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids)).with_columns(
        pl.col("vysledek").map_elements(vote_category, return_dtype=pl.String).alias("cat")
    )
    mp_ids = sorted(mp_votes.get_column("id_poslanec").unique().to_list())
    vote_ids = sorted(mp_votes.get_column("id_hlasovani").unique().to_list())
    info = pl.DataFrame({"id_poslanec": mp_ids}).join(data.mp_info, on="id_poslanec", how="left")
    names = (info.get_column("jmeno") + " " + info.get_column("prijmeni")).to_list()
    parties = info.get_column("party").to_list()

    same, both = _cross_club_counts(mp_votes, mp_ids, vote_ids)
    cross = _pairs(same, both, names, parties, lambda i, j: True, top)

    sides = _side_per_record(data, mp_votes)
    gov = _indicator_matrices(mp_votes.filter(sides == GOVERNMENT), mp_ids, vote_ids)
    opp = _indicator_matrices(mp_votes.filter(sides == OPPOSITION), mp_ids, vote_ids)
    s_go, b_go = _agreement_counts(gov, opp)
    gov_opp = _pairs(s_go + s_go.T, b_go + b_go.T, names, parties, lambda i, j: True, top)
    return {"cross_party": cross, "gov_opp": gov_opp}


_MAP_W, _MAP_H, _MAP_PAD = 640.0, 400.0, 14.0


def similarity_map(data: PeriodData) -> dict:
    """PCA map points scaled to an SVG viewBox, coloured by club, plus a legend (#116).

    Args:
        data: Period data.

    Returns:
        {"points": [{name, party, cx, cy, color}], "legend": [{party, color, side}],
        "width", "height"}.
    """
    coords = compute_pca_coords(data)
    if not coords:
        return {"points": [], "legend": [], "width": _MAP_W, "height": _MAP_H}
    xs = [c["x"] for c in coords]
    ys = [c["y"] for c in coords]
    span_x = (max(xs) - min(xs)) or 1.0
    span_y = (max(ys) - min(ys)) or 1.0
    points = [
        {
            "name": c["mp_name"],
            "party": c["party"],
            "cx": round(_MAP_PAD + (c["x"] - min(xs)) / span_x * (_MAP_W - 2 * _MAP_PAD), 1),
            "cy": round(_MAP_PAD + (c["y"] - min(ys)) / span_y * (_MAP_H - 2 * _MAP_PAD), 1),
            "color": CLUB_COLORS.get(c["party"], CLUB_COLOR_OTHER),
        }
        for c in coords
    ]
    counts: dict[str, int] = {}
    for c in coords:
        counts[c["party"]] = counts.get(c["party"], 0) + 1
    legend = [
        {
            "party": party,
            "color": CLUB_COLORS.get(party, CLUB_COLOR_OTHER),
            "side": classify_party(party, data.coalition_parties),
            "count": n,
        }
        for party, n in sorted(counts.items(), key=lambda kv: -kv[1])
    ]
    return {
        "points": points,
        "legend": legend,
        "groups": _side_labels(points, data.coalition_parties),
        "width": _MAP_W,
        "height": _MAP_H,
    }


def _side_labels(points: list[dict], coalition: frozenset[str]) -> list[dict]:
    """Where to write "vládní kluby" / "opoziční kluby" on the map: each side's centre."""
    out = []
    for side in ("government", "opposition"):
        pts = [p for p in points if classify_party(p["party"], coalition) == side]
        if pts:
            x = sum(p["cx"] for p in pts) / len(pts)
            y = min(p["cy"] for p in pts) - 12
            out.append({"side": side, "x": round(x, 1), "y": round(max(y, 14), 1)})
    return out

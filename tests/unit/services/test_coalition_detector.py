"""Tests for automatic government-coalition detection from confidence votes."""

import polars as pl

from pspcz_analyzer.services.coalition_detector import detect_coalition_parties

# 10 MPs: 6 in club GOV, 4 in club OPP (GOV holds a seat majority).
MP_INFO = pl.DataFrame(
    {
        "id_poslanec": list(range(1, 11)),
        "party": ["GOV"] * 6 + ["OPP"] * 4,
    }
)


def _votes(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"id_hlasovani": pl.Int64, "vysledek": pl.Utf8, "nazev_dlouhy": pl.Utf8},
    )


def _mp_votes(per_vote: dict[int, dict[int, str]]) -> pl.DataFrame:
    """per_vote: {vote_id: {id_poslanec: vysledek}}."""
    ids, mps, res = [], [], []
    for vid, votes in per_vote.items():
        for mp, r in votes.items():
            ids.append(vid)
            mps.append(mp)
            res.append(r)
    return pl.DataFrame(
        {"id_hlasovani": ids, "id_poslanec": mps, "vysledek": res},
        schema={"id_hlasovani": pl.Int64, "id_poslanec": pl.Int64, "vysledek": pl.Utf8},
    )


def _all_yes(mp_ids: range) -> dict[int, str]:
    return dict.fromkeys(mp_ids, "A")


def _gov_yes_opp_no() -> dict[int, str]:
    return {**dict.fromkeys(range(1, 7), "A"), **dict.fromkeys(range(7, 11), "B")}


def test_detects_clean_coalition():
    votes = _votes([{"id_hlasovani": 1, "vysledek": "A", "nazev_dlouhy": "Vyslovení důvěry vládě"}])
    mp_votes = _mp_votes({1: _gov_yes_opp_no()})
    assert detect_coalition_parties(votes, mp_votes, MP_INFO) == frozenset({"GOV"})


def test_near_unanimous_returns_empty():
    # Procedural vote sharing the confidence name: everyone YES → no opposing bloc
    votes = _votes([{"id_hlasovani": 2, "vysledek": "A", "nazev_dlouhy": "Vyslovení důvěry vládě"}])
    mp_votes = _mp_votes({2: _all_yes(range(1, 11))})
    assert detect_coalition_parties(votes, mp_votes, MP_INFO) == frozenset()


def test_no_confidence_motion_excluded():
    # "nedůvěr" inverts meaning → must be ignored even if polarized + passed
    votes = _votes(
        [{"id_hlasovani": 3, "vysledek": "A", "nazev_dlouhy": "Návrh na vyslovení nedůvěry vládě"}]
    )
    mp_votes = _mp_votes({3: _gov_yes_opp_no()})
    assert detect_coalition_parties(votes, mp_votes, MP_INFO) == frozenset()


def test_no_matching_vote_returns_empty():
    votes = _votes([{"id_hlasovani": 4, "vysledek": "A", "nazev_dlouhy": "Novela zákona o daních"}])
    mp_votes = _mp_votes({4: _gov_yes_opp_no()})
    assert detect_coalition_parties(votes, mp_votes, MP_INFO) == frozenset()


def test_failed_confidence_ignored():
    # vysledek != "A" (government did not win confidence)
    votes = _votes([{"id_hlasovani": 5, "vysledek": "R", "nazev_dlouhy": "Vyslovení důvěry vládě"}])
    mp_votes = _mp_votes({5: _gov_yes_opp_no()})
    assert detect_coalition_parties(votes, mp_votes, MP_INFO) == frozenset()


def test_picks_most_recent_by_id():
    # 3 clubs (A=5, B=4, C=2). Two valid confidence votes with different winning
    # coalitions — the higher id_hlasovani (later government) must win.
    mp_info = pl.DataFrame(
        {
            "id_poslanec": list(range(1, 12)),
            "party": ["A"] * 5 + ["B"] * 4 + ["C"] * 2,
        }
    )
    votes = _votes(
        [
            {"id_hlasovani": 10, "vysledek": "A", "nazev_dlouhy": "Vyslovení důvěry vládě"},
            {"id_hlasovani": 99, "vysledek": "A", "nazev_dlouhy": "Vyslovení důvěry vládě"},
        ]
    )
    # Both votes are contested majorities (<=75% YES) with different coalitions.
    # Older (10): B+C yes (6 seats) vs A no → {B, C}
    older = {**dict.fromkeys(range(1, 6), "B"), **dict.fromkeys(range(6, 12), "A")}
    # Newer (99): A+C yes (7 seats) vs B no → {A, C}
    newer = {
        **dict.fromkeys(range(1, 6), "A"),
        **dict.fromkeys(range(6, 10), "B"),
        10: "A",
        11: "A",
    }
    mp_votes = _mp_votes({10: older, 99: newer})
    assert detect_coalition_parties(votes, mp_votes, mp_info) == frozenset({"A", "C"})

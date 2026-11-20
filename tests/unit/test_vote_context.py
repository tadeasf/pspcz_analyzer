"""Vote kinds, filters, item results and the vote detail story (#113, #114)."""

import polars as pl

from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.vote_context import (
    filter_kind,
    group_result,
    proposer_view,
    vote_story,
)
from tests.fixtures.sample_data import make_mp_info, make_mp_votes, make_void_votes, make_votes


def _data() -> PeriodData:
    data = PeriodData(
        period=10,
        votes=make_votes(),
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
    )
    data.vote_steno = {
        1: {"kind": "committee", "question": "Přikázat výboru?"},
        2: {"kind": "reject"},
        3: {"kind": "procedural", "subkind": "challenge", "proposer": "Matěj Gregor"},
        4: {"kind": "procedural", "invalid_by": 3, "repeated_as": 5, "pro": "zrušit usnesení"},
        5: {"kind": "procedural", "repeats": 4},
    }
    return data


def test_filter_kind_groups_reject_and_return() -> None:
    data = _data()
    assert filter_kind(data.votes, data, "reject_return")["id_hlasovani"].to_list() == [2]
    assert filter_kind(data.votes, data, "committee")["id_hlasovani"].to_list() == [1]
    assert filter_kind(data.votes, data, "procedural")["id_hlasovani"].to_list() == [3, 4, 5]


def test_group_result_prefers_the_final_vote() -> None:
    rows = [
        {"kind": "amendment", "vysledek": "A", "is_void": False},
        {"kind": "final", "vysledek": "R", "is_void": False},
    ]
    assert group_result(rows) == "vl.res_final_fail"
    assert group_result([{"kind": "committee", "vysledek": "A", "is_void": False}]) == (
        "vl.res_committee_pass"
    )
    assert group_result([{"kind": "procedural", "vysledek": "A", "is_void": False}]) == ""


def test_story_of_a_challenged_vote() -> None:
    data = _data()
    info = {"kind": "procedural", "vysledek": "A", "schuze": 1, "tisk_ct": None}
    info["steno"] = {k: data.vote_steno[4].get(k) for k in ("invalid_by", "repeated_as", "pro")}
    story = vote_story(data, info)
    assert story["invalid"] and story["consequence"] == ""
    assert story["challenger"] == "Matěj Gregor"
    assert story["repeat"] and story["repeat"]["cislo"] == 5
    assert story["pro"] == "zrušit usnesení"


def test_story_consequence_of_a_final_vote() -> None:
    info = {"kind": "final", "vysledek": "A", "schuze": 1, "tisk_ct": None, "steno": None}
    story = vote_story(_data(), info)
    assert story["consequence"] == "vd.cons_final_pass"
    assert story["final_meaning"]


def test_proposer_with_club_on_the_vote_day_and_verb() -> None:
    data = _data()
    data.mp_info = pl.DataFrame(
        {
            "id_poslanec": [1, 2],
            "jmeno": ["Karel", "Lucie"],
            "prijmeni": ["Haas", "Sedmihradská"],
            "party": ["ODS", "ANO"],
            "pohlavi": ["M", "Ž"],
        }
    )
    data.mp_votes = pl.DataFrame({"id_hlasovani": [5], "id_poslanec": [1], "party": ["ODS"]})
    assert proposer_view(data, "Haas", 5) == {
        "verb": "vd.proposed_by_m",
        "text": "Karel Haas (ODS)",
    }
    assert (proposer_view(data, "Lucie Sedmihradská", 5) or {})["verb"] == "vd.proposed_by_f"
    assert (proposer_view(data, "Haas, Novák", 5) or {})["verb"] == "vd.proposed_by_pl"
    assert (proposer_view(data, "Novák", 5) or {})["verb"] == "vd.proposed_by"

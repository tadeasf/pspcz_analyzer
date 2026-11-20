"""Tests for amendment query and listing service."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendment_listing import _sd_numbers, law_amendments, list_amendments
from pspcz_analyzer.services.amendment_service import amendment_mp_votes
from tests.fixtures.sample_data import (
    make_mp_info,
    make_mp_votes,
    make_void_votes,
    make_votes,
)


def _make_bill(schuze: int, bod: int, ct: int, n_amendments: int = 3) -> BillAmendmentData:
    """Create a test BillAmendmentData."""
    amendments = [
        AmendmentVote(
            letter=chr(65 + i),  # A, B, C, ...
            vote_number=100 + i,
            id_hlasovani=i + 1,
            result="accepted" if i % 2 == 0 else "rejected",
            committee_stance="doporucujici" if i % 2 == 0 else "nedoporucujici",
        )
        for i in range(n_amendments)
    ]
    final = AmendmentVote(
        letter="",
        vote_number=100 + n_amendments,
        id_hlasovani=n_amendments + 1,
        result="accepted",
        is_final_vote=True,
    )
    return BillAmendmentData(
        period=10,
        schuze=schuze,
        bod=bod,
        ct=ct,
        tisk_nazev=f"Tisk {ct} - zákon o testování",
        steno_url="https://psp.cz/steno/test",
        amendments=amendments,
        final_vote=final,
        parse_confidence=0.95,
        parse_warnings=[],
    )


def _make_data_with_amendments() -> PeriodData:
    """Create a PeriodData with amendment_data populated."""
    votes = make_votes()
    return PeriodData(
        period=10,
        votes=votes,
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
        amendment_data={
            (78, 1): _make_bill(78, 1, 489, 3),
            (78, 2): _make_bill(78, 2, 500, 2),
            (79, 1): _make_bill(79, 1, 510, 4),
        },
    )


class TestListAmendments:
    def test_groups_by_law_and_counts_letters(self):
        result = list_amendments(_make_data_with_amendments())
        assert result["total"] == 3
        assert result["law_count"] == 3
        assert result["period_tally"]["total"] == 9
        assert sum(result["period_tally"]["by_fate"].values()) == 9

    def test_search_by_tisk_number(self):
        result = list_amendments(_make_data_with_amendments(), search="489")
        assert [g["ct"] for g in result["rows"]] == [489]

    def test_search_folds_diacritics_and_case(self):
        data = _make_data_with_amendments()
        assert list_amendments(data, search="ZAKON testovani")["total"] == 3

    def test_fate_filter_keeps_only_matching_letters(self):
        result = list_amendments(_make_data_with_amendments(), fate="rejected")
        assert all(x["fate"] == "rejected" for g in result["rows"] for x in g["shown"])
        assert sum(len(g["shown"]) for g in result["rows"]) == 4

    def test_letters_view_and_pagination(self):
        data = _make_data_with_amendments()
        result = list_amendments(data, view="letters", per_page=4, page=3)
        assert result["total"] == 9
        assert len(result["rows"]) == 1
        assert result["rows"][0]["law"]["ct"]

    def test_sort_most(self):
        result = list_amendments(_make_data_with_amendments(), sort="most")
        assert [g["ct"] for g in result["rows"]] == [510, 489, 500]

    def test_same_law_in_two_sittings_is_one_group(self):
        data = _make_data_with_amendments()
        data.amendment_data[(80, 3)] = _make_bill(80, 3, 489, 2)
        result = list_amendments(data)
        assert result["law_count"] == 3
        group = next(g for g in result["rows"] if g["ct"] == 489)
        assert len(group["letters"]) == 5
        assert (group["schuze"], group["bod"]) == (80, 3)

    def test_empty_amendment_data(self):
        data = PeriodData(
            period=10,
            votes=make_votes(),
            mp_votes=make_mp_votes(),
            void_votes=make_void_votes(),
            mp_info=make_mp_info(),
        )
        result = list_amendments(data)
        assert result["total"] == 0
        assert result["law_count"] == 0


class TestLawAmendments:
    def test_not_found_returns_none(self):
        assert law_amendments(_make_data_with_amendments(), schuze=99, bod=99) is None

    def test_letters_and_tally(self):
        result = law_amendments(_make_data_with_amendments(), schuze=78, bod=1)
        assert result is not None
        assert result["ct"] == 489
        assert [x["letter"] for x in result["letters"]] == ["A", "B", "C"]
        assert result["tally"]["accepted"] == 2
        assert result["final_result"] == "accepted"
        assert result["letters"][0]["committee_stance"] == "doporucujici"


class TestAmendmentMpVotes:
    def test_returns_dict_for_existing_vote(self):
        data = _make_data_with_amendments()
        result = amendment_mp_votes(data, id_hlasovani=1)
        assert result is not None
        assert isinstance(result, dict)

    def test_nonexistent_vote_returns_none(self):
        data = _make_data_with_amendments()
        result = amendment_mp_votes(data, id_hlasovani=99999)
        assert result is None

    def test_contains_party_breakdown(self):
        data = _make_data_with_amendments()
        result = amendment_mp_votes(data, id_hlasovani=1)
        assert result is not None
        assert "party_breakdown" in result
        parties = {p["party"] for p in result["party_breakdown"]}
        assert "ANO" in parties
        assert "ODS" in parties

    def test_contains_mp_votes(self):
        data = _make_data_with_amendments()
        result = amendment_mp_votes(data, id_hlasovani=1)
        assert result is not None
        assert "mp_votes" in result
        assert len(result["mp_votes"]) > 0
        for mp in result["mp_votes"]:
            assert "vote_label" in mp
            assert "party" in mp


def test_sd_numbers_per_sub_letter() -> None:
    text = "G. \nPoslanec Patrik Pařil \nG.1. SD 1077 \ntext\nG.2. SD 1078\n"
    assert _sd_numbers("G2", text) == ["1078"]
    assert _sd_numbers("B1", "B1. SD 1731\n") == ["1731"]
    assert _sd_numbers("G", text) == ["1077", "1078"]

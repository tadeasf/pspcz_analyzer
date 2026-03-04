"""Tests for amendment query and listing service."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendment_service import (
    amendment_detail,
    amendment_mp_votes,
    list_amendment_bills,
)
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


class TestListAmendmentBills:
    def test_returns_pagination_dict(self):
        data = _make_data_with_amendments()
        result = list_amendment_bills(data)
        assert isinstance(result, dict)
        assert "rows" in result
        assert "total" in result
        assert "page" in result
        assert "per_page" in result
        assert "total_pages" in result

    def test_total_matches_bills(self):
        data = _make_data_with_amendments()
        result = list_amendment_bills(data)
        assert result["total"] == 3

    def test_search_filters_by_name(self):
        data = _make_data_with_amendments()
        result = list_amendment_bills(data, search="489")
        assert result["total"] >= 1
        for row in result["rows"]:
            assert "489" in row["tisk_nazev"]

    def test_search_case_insensitive(self):
        data = _make_data_with_amendments()
        result = list_amendment_bills(data, search="ZÁKON")
        # All bills contain "zákon" in their name
        assert result["total"] == 3

    def test_pagination(self):
        data = _make_data_with_amendments()
        result = list_amendment_bills(data, per_page=2, page=1)
        assert len(result["rows"]) == 2
        assert result["total_pages"] == 2

    def test_page_2(self):
        data = _make_data_with_amendments()
        result = list_amendment_bills(data, per_page=2, page=2)
        assert len(result["rows"]) == 1

    def test_row_has_expected_fields(self):
        data = _make_data_with_amendments()
        result = list_amendment_bills(data)
        row = result["rows"][0]
        assert "schuze" in row
        assert "bod" in row
        assert "ct" in row
        assert "tisk_nazev" in row
        assert "amendment_count" in row
        assert "parse_confidence" in row

    def test_empty_amendment_data(self):
        data = PeriodData(
            period=10,
            votes=make_votes(),
            mp_votes=make_mp_votes(),
            void_votes=make_void_votes(),
            mp_info=make_mp_info(),
        )
        result = list_amendment_bills(data)
        assert result["total"] == 0
        assert result["rows"] == []


class TestAmendmentDetail:
    def test_returns_dict(self):
        data = _make_data_with_amendments()
        result = amendment_detail(data, schuze=78, bod=1)
        assert isinstance(result, dict)

    def test_not_found_returns_none(self):
        data = _make_data_with_amendments()
        result = amendment_detail(data, schuze=99, bod=99)
        assert result is None

    def test_contains_amendments_list(self):
        data = _make_data_with_amendments()
        result = amendment_detail(data, schuze=78, bod=1)
        assert result is not None
        assert "amendments" in result
        assert len(result["amendments"]) == 3

    def test_contains_final_vote(self):
        data = _make_data_with_amendments()
        result = amendment_detail(data, schuze=78, bod=1)
        assert result is not None
        assert "final_vote" in result
        assert result["final_vote"] is not None
        assert result["final_vote"]["is_final_vote"] is True

    def test_amendment_has_fields(self):
        data = _make_data_with_amendments()
        result = amendment_detail(data, schuze=78, bod=1)
        assert result is not None
        amend = result["amendments"][0]
        assert "letter" in amend
        assert "vote_number" in amend
        assert "result" in amend
        assert "committee_stance" in amend

    def test_bill_metadata(self):
        data = _make_data_with_amendments()
        result = amendment_detail(data, schuze=78, bod=1)
        assert result is not None
        assert result["schuze"] == 78
        assert result["bod"] == 1
        assert result["ct"] == 489


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

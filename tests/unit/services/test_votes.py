"""Tests for vote search and detail service."""

from pspcz_analyzer.services.votes_service import list_votes, vote_detail
from tests.fixtures.sample_data import make_period_data


class TestListVotes:
    def test_returns_dict_with_pagination(self):
        data = make_period_data()
        result = list_votes(data)
        assert isinstance(result, dict)
        assert "rows" in result
        assert "total" in result
        assert "page" in result
        assert "per_page" in result
        assert "total_pages" in result

    def test_all_votes_returned(self):
        """With no filters, should return all 5 votes."""
        data = make_period_data()
        result = list_votes(data)
        assert result["total"] == 5

    def test_search_filters_by_name(self):
        """Search should filter votes by description text."""
        data = make_period_data()
        result = list_votes(data, search="Test vote 1")
        assert result["total"] >= 1
        for row in result["rows"]:
            assert "Test vote 1" in row["nazev_dlouhy"]

    def test_pagination_works(self):
        """Per-page limit should reduce rows returned."""
        data = make_period_data()
        result = list_votes(data, per_page=2, page=1)
        assert len(result["rows"]) == 2
        assert result["total_pages"] == 3  # ceil(5/2) = 3

    def test_page_2(self):
        """Page 2 should return different rows."""
        data = make_period_data()
        page1 = list_votes(data, per_page=2, page=1)
        page2 = list_votes(data, per_page=2, page=2)
        ids1 = {r["id_hlasovani"] for r in page1["rows"]}
        ids2 = {r["id_hlasovani"] for r in page2["rows"]}
        assert ids1.isdisjoint(ids2)

    def test_outcome_label_present(self):
        """Each row should have an outcome_label."""
        data = make_period_data()
        result = list_votes(data)
        for row in result["rows"]:
            assert "outcome_label" in row


class TestVoteDetail:
    def test_returns_dict(self):
        data = make_period_data()
        result = vote_detail(data, vote_id=1)
        assert isinstance(result, dict)

    def test_info_section(self):
        """Detail should include vote info."""
        data = make_period_data()
        result = vote_detail(data, vote_id=1)
        assert result is not None
        assert "info" in result
        assert result["info"]["id_hlasovani"] == 1

    def test_party_breakdown(self):
        """Detail should include per-party vote breakdown."""
        data = make_period_data()
        result = vote_detail(data, vote_id=1)
        assert result is not None
        assert "party_breakdown" in result
        parties = {r["party"] for r in result["party_breakdown"]}
        assert "ANO" in parties
        assert "ODS" in parties

    def test_mp_votes_list(self):
        """Detail should include per-MP vote list."""
        data = make_period_data()
        result = vote_detail(data, vote_id=1)
        assert result is not None
        assert "mp_votes" in result
        assert len(result["mp_votes"]) > 0
        for m in result["mp_votes"]:
            assert "vote_label" in m

    def test_nonexistent_vote(self):
        """Non-existent vote ID should return None."""
        data = make_period_data()
        result = vote_detail(data, vote_id=99999)
        assert result is None

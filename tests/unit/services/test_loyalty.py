"""Tests for loyalty (rebellion rate) computation."""

from pspcz_analyzer.services.loyalty_service import compute_loyalty
from tests.fixtures.sample_data import make_period_data


class TestComputeLoyalty:
    def test_returns_list_of_dicts(self):
        data = make_period_data()
        result = compute_loyalty(data)
        assert isinstance(result, list)
        assert all(isinstance(r, dict) for r in result)

    def test_rebellion_pct_range(self):
        """Rebellion percentages should be between 0 and 100."""
        data = make_period_data()
        result = compute_loyalty(data)
        for row in result:
            assert 0 <= row["rebellion_pct"] <= 100

    def test_rebel_mp_detected(self):
        """MP 3 (Karel Dvořák, ODS) votes NO on 3/5 votes against ODS majority YES."""
        data = make_period_data()
        result = compute_loyalty(data, top=50)
        rebels = [r for r in result if r["prijmeni"] == "Dvořák"]
        assert len(rebels) == 1
        # 3 rebellions out of 5 active votes = 60%
        assert rebels[0]["rebellion_pct"] == 60.0

    def test_loyal_mp_zero_rebellion(self):
        """MPs 1 and 2 (ANO) always vote YES with party — 0% rebellion."""
        data = make_period_data()
        result = compute_loyalty(data, top=50)
        loyal = [r for r in result if r["party"] == "ANO"]
        for mp in loyal:
            assert mp["rebellion_pct"] == 0.0

    def test_party_filter(self):
        """Filtering by party should only return MPs from that party."""
        data = make_period_data()
        result = compute_loyalty(data, party_filter="ODS")
        assert all(r["party"] == "ODS" for r in result)

    def test_party_filter_case_insensitive(self):
        """Party filter should be case-insensitive."""
        data = make_period_data()
        result = compute_loyalty(data, party_filter="ods")
        assert all(r["party"] == "ODS" for r in result)

    def test_top_limits_results(self):
        """Top parameter should limit the number of results."""
        data = make_period_data()
        result = compute_loyalty(data, top=2)
        assert len(result) <= 2

    def test_empty_data(self):
        """Empty data should return empty list."""
        import polars as pl

        from pspcz_analyzer.services.data_service import PeriodData

        data = PeriodData(
            period=1,
            votes=pl.DataFrame(
                schema={
                    "id_hlasovani": pl.Int64,
                    "datum": pl.Utf8,
                    "nazev_dlouhy": pl.Utf8,
                    "schuze": pl.Int32,
                    "bod": pl.Int32,
                }
            ),
            mp_votes=pl.DataFrame(
                schema={"id_poslanec": pl.Int64, "id_hlasovani": pl.Int64, "vysledek": pl.Utf8}
            ),
            void_votes=pl.DataFrame({"id_hlasovani": pl.Series([], dtype=pl.Int64)}),
            mp_info=pl.DataFrame(
                schema={
                    "id_poslanec": pl.Int64,
                    "id_osoba": pl.Int64,
                    "jmeno": pl.Utf8,
                    "prijmeni": pl.Utf8,
                    "party": pl.Utf8,
                }
            ),
        )
        result = compute_loyalty(data)
        assert result == []

    def test_rebellion_votes_attached(self):
        """Each result row should have a rebellion_votes list."""
        data = make_period_data()
        result = compute_loyalty(data, top=50)
        for row in result:
            assert "rebellion_votes" in row
            assert isinstance(row["rebellion_votes"], list)

    def test_sorted_by_rebellion_descending(self):
        """Results should be sorted by rebellion_pct descending."""
        data = make_period_data()
        result = compute_loyalty(data, top=50)
        pcts = [r["rebellion_pct"] for r in result]
        assert pcts == sorted(pcts, reverse=True)

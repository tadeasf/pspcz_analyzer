"""Tests for cross-party coalition analysis service."""

import polars as pl

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.coalition_service import (
    compute_amendment_coalitions,
)
from tests.fixtures.sample_data import make_mp_info, make_void_votes


def _make_amendment_period_data() -> PeriodData:
    """Create PeriodData with amendment data suitable for coalition analysis.

    Setup:
    - 3 parties: ANO (MPs 1,2), ODS (MPs 3,4,6), STAN (MP 5)
    - 5 votes total, votes 1-3 are amendment votes
    - MP 1 (ANO): all YES
    - MP 2 (ANO): all YES
    - MP 3 (ODS): votes NO on amendment votes 1-3 (rebel)
    - MP 4 (ODS): all YES
    - MP 5 (STAN): YES on 1, absent on 2, excused on 3, DID_NOT_VOTE on 4, abstained on 5
    - MP 6 (ODS): all YES
    """
    votes = pl.DataFrame(
        {
            "id_hlasovani": [1, 2, 3, 4, 5],
            "id_organ": [165] * 5,
            "schuze": [78] * 5,
            "cislo": [1, 2, 3, 4, 5],
            "bod": [1] * 5,
            "datum": ["2024-01-01"] * 5,
            "cas": ["10:00:00"] * 5,
            "pro": [100] * 5,
            "proti": [50] * 5,
            "zdrzel": [10] * 5,
            "nehlasoval": [20] * 5,
            "prihlaseno": [180] * 5,
            "kvorum": [90] * 5,
            "druh_hlasovani": ["N"] * 5,
            "vysledek": ["A"] * 5,
            "nazev_dlouhy": [f"Test vote {i}" for i in range(1, 6)],
            "nazev_kratky": [f"TV{i}" for i in range(1, 6)],
        },
        schema={
            "id_hlasovani": pl.Int64,
            "id_organ": pl.Int32,
            "schuze": pl.Int32,
            "cislo": pl.Int32,
            "bod": pl.Int32,
            "datum": pl.Utf8,
            "cas": pl.Utf8,
            "pro": pl.Int32,
            "proti": pl.Int32,
            "zdrzel": pl.Int32,
            "nehlasoval": pl.Int32,
            "prihlaseno": pl.Int32,
            "kvorum": pl.Int32,
            "druh_hlasovani": pl.Utf8,
            "vysledek": pl.Utf8,
            "nazev_dlouhy": pl.Utf8,
            "nazev_kratky": pl.Utf8,
        },
    )

    records = []
    # MP 1 (ANO) - all YES
    for vid in range(1, 6):
        records.append({"id_poslanec": 1, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 2 (ANO) - all YES
    for vid in range(1, 6):
        records.append({"id_poslanec": 2, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 3 (ODS) - rebels on amendment votes 1-3 (NO), loyal on 4-5 (YES)
    for vid in range(1, 4):
        records.append({"id_poslanec": 3, "id_hlasovani": vid, "vysledek": VoteResult.NO})
    for vid in range(4, 6):
        records.append({"id_poslanec": 3, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 4 (ODS) - all YES
    for vid in range(1, 6):
        records.append({"id_poslanec": 4, "id_hlasovani": vid, "vysledek": VoteResult.YES})
    # MP 5 (STAN) - varied
    records.append({"id_poslanec": 5, "id_hlasovani": 1, "vysledek": VoteResult.YES})
    records.append({"id_poslanec": 5, "id_hlasovani": 2, "vysledek": VoteResult.ABSENT})
    records.append({"id_poslanec": 5, "id_hlasovani": 3, "vysledek": VoteResult.EXCUSED})
    records.append({"id_poslanec": 5, "id_hlasovani": 4, "vysledek": VoteResult.DID_NOT_VOTE})
    records.append({"id_poslanec": 5, "id_hlasovani": 5, "vysledek": VoteResult.ABSTAINED})
    # MP 6 (ODS) - all YES
    for vid in range(1, 6):
        records.append({"id_poslanec": 6, "id_hlasovani": vid, "vysledek": VoteResult.YES})

    mp_votes = pl.DataFrame(
        records,
        schema={"id_poslanec": pl.Int64, "id_hlasovani": pl.Int64, "vysledek": pl.Utf8},
    )

    # Amendment data: votes 1-3 are amendment votes for bill (78, 1)
    bill = BillAmendmentData(
        period=10,
        schuze=78,
        bod=1,
        ct=489,
        tisk_nazev="Test bill",
        amendments=[
            AmendmentVote(letter="A", vote_number=1, id_hlasovani=1, result="accepted"),
            AmendmentVote(letter="B", vote_number=2, id_hlasovani=2, result="rejected"),
            AmendmentVote(letter="C", vote_number=3, id_hlasovani=3, result="accepted"),
        ],
    )

    return PeriodData(
        period=10,
        votes=votes,
        mp_votes=mp_votes,
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
        amendment_data={(78, 1): bill},
    )


class TestComputeAmendmentCoalitions:
    def test_returns_dict(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        assert isinstance(result, dict)

    def test_has_expected_keys(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        assert "party_agreement" in result
        assert "rebels" in result
        assert "party_cohesion" in result
        assert "total_bills" in result
        assert "total_amendments" in result

    def test_total_counts(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        assert result["total_bills"] == 1
        assert result["total_amendments"] == 3

    def test_party_agreement_is_list(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        assert isinstance(result["party_agreement"], list)

    def test_party_agreement_has_rates(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        for pair in result["party_agreement"]:
            assert "party_a" in pair
            assert "party_b" in pair
            assert "agreement_rate" in pair
            assert 0.0 <= pair["agreement_rate"] <= 1.0

    def test_party_cohesion_is_list(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        assert isinstance(result["party_cohesion"], list)

    def test_party_cohesion_has_fields(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        for pc in result["party_cohesion"]:
            assert "party" in pc
            assert "overall_cohesion" in pc

    def test_rebels_is_list(self):
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        assert isinstance(result["rebels"], list)

    def test_empty_amendment_data(self):
        data = PeriodData(
            period=10,
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
            mp_info=make_mp_info(),
        )
        result = compute_amendment_coalitions(data)
        assert result["party_agreement"] == []
        assert result["rebels"] == []
        assert result["party_cohesion"] == []
        assert result["total_bills"] == 0
        assert result["total_amendments"] == 0

    def test_party_agreement_sorted_descending(self):
        """Party agreement pairs should be sorted by rate descending."""
        data = _make_amendment_period_data()
        result = compute_amendment_coalitions(data)
        rates = [p["agreement_rate"] for p in result["party_agreement"]]
        assert rates == sorted(rates, reverse=True)

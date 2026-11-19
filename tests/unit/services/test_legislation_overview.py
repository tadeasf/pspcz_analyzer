"""Tests for the homepage legislation blocks (#108)."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.legislation_overview import homepage_blocks
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory, TiskHistoryStage
from tests.fixtures.sample_data import (
    make_mp_info,
    make_mp_votes,
    make_void_votes,
    make_votes,
)


def _make_tisk(ct: int, status: str, latest_date: str | None) -> TiskInfo:
    stages = []
    if latest_date:
        stages.append(TiskHistoryStage(stage_type="3_cteni", label="3. čtení", date=latest_date))
    history = TiskHistory(ct=ct, period=10, current_status=status, stages=stages)
    return TiskInfo(
        id_tisk=ct * 10,
        ct=ct,
        nazev=f"Zákon {ct}",
        period=10,
        id_druh=1,
        topics=["Ekonomika"],
        history=history,
    )


def _make_bill(schuze: int, bod: int, ct: int, parties: list[str]) -> BillAmendmentData:
    amendments = [
        AmendmentVote(letter="A", vote_number=1, result="accepted", submitter_parties=parties),
    ]
    return BillAmendmentData(
        period=10, schuze=schuze, bod=bod, ct=ct, tisk_nazev=f"Zákon {ct}", amendments=amendments
    )


def _make_data() -> PeriodData:
    return PeriodData(
        period=10,
        votes=make_votes(),
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
        tisk_lookup={
            (78, 1): _make_tisk(200, "vyhlášeno", "5. 11. 2025"),
            (78, 2): _make_tisk(300, "projednáváno", "20. 11. 2025"),
            (79, 1): _make_tisk(400, "projednáváno", None),
        },
        amendment_data={
            (78, 2): _make_bill(78, 2, 300, ["ANO", "SPD"]),
        },
        coalition_parties=frozenset({"ANO", "SPD", "MS"}),
    )


class TestHomepageBlocks:
    def test_in_progress_only_chamber_bills_by_last_step(self):
        blocks = homepage_blocks(_make_data())
        assert [r["ct"] for r in blocks["in_progress"]] == [300, 400]
        assert blocks["in_progress_total"] == 2

    def test_recently_passed_contains_published_law(self):
        blocks = homepage_blocks(_make_data())
        assert [r["ct"] for r in blocks["recently_passed"]] == [200]
        assert blocks["recently_passed"][0]["status_key"] == "laws.status.published"

    def test_newest_by_tisk_number(self):
        assert [r["ct"] for r in homepage_blocks(_make_data())["newest"]] == [400, 300, 200]

    def test_non_bills_are_excluded(self):
        data = _make_data()
        data.tisk_lookup[(80, 1)] = TiskInfo(
            id_tisk=1, ct=999, nazev="Zpráva", period=10, id_druh=5
        )
        assert 999 not in [r["ct"] for r in homepage_blocks(data)["newest"]]

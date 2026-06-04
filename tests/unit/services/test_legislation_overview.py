"""Tests for the homepage legislation overview (recent activity + active laws)."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.legislation_overview import active_laws, recent_activity
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


class TestRecentActivity:
    def test_orders_by_latest_date_desc(self):
        rows = recent_activity(_make_data())
        # ct=300 (20.11) before ct=200 (5.11); ct=400 has no date so excluded
        assert [r["ct"] for r in rows] == [300, 200]

    def test_attaches_driver_and_amendment_link(self):
        rows = recent_activity(_make_data())
        ct300 = next(r for r in rows if r["ct"] == 300)
        assert ct300["has_amendments"] is True
        assert ct300["driver"] == "government"
        assert ct300["amendment_link"] == {"schuze": 78, "bod": 2}

    def test_bill_without_amendments(self):
        rows = recent_activity(_make_data())
        ct200 = next(r for r in rows if r["ct"] == 200)
        assert ct200["has_amendments"] is False
        assert ct200["driver"] == "unknown"

    def test_limit_respected(self):
        rows = recent_activity(_make_data(), limit=1)
        assert len(rows) == 1
        assert rows[0]["ct"] == 300


class TestActiveLaws:
    def test_excludes_terminal_bills(self):
        rows = active_laws(_make_data())
        cts = {r["ct"] for r in rows}
        # 200 is "vyhlášeno" (terminal) → excluded; 300 + 400 active
        assert cts == {300, 400}

    def test_active_with_date_first(self):
        rows = active_laws(_make_data())
        assert rows[0]["ct"] == 300  # dated active bill ranks above undated

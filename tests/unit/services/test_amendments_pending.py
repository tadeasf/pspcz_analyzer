"""Tests for bills whose amendments are not voted yet (#112)."""

from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData, TiskInfo
from pspcz_analyzer.services.amendment_letters import bill_letters, tally
from pspcz_analyzer.services.amendments.pdf_parser import PdfAmendment
from pspcz_analyzer.services.amendments.pending import build_pending_bills, pending_cts
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory, TiskHistoryStage
from tests.fixtures.sample_data import make_mp_info, make_mp_votes, make_void_votes, make_votes


def _tisk(ct: int, status: str = "projednáváno", ct1: int | None = 3, third: bool = False):
    stages = [TiskHistoryStage(stage_type="2_cteni", label="2. čtení", date="1. 11. 2026")]
    if third:
        stages.append(TiskHistoryStage(stage_type="3_cteni", label="3. čtení", date="5. 11. 2026"))
    history = TiskHistory(ct=ct, period=10, current_status=status, stages=stages)
    history.amendment_tisk_ct1 = ct1
    return TiskInfo(id_tisk=ct, ct=ct, nazev=f"Zákon {ct}", period=10, id_druh=2, history=history)


def _data(*tisky: TiskInfo) -> PeriodData:
    return PeriodData(
        period=10,
        votes=make_votes(),
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
        tisk_lookup={(1, i): t for i, t in enumerate(tisky)},
    )


def test_pending_needs_document_and_no_third_reading() -> None:
    data = _data(
        _tisk(1),
        _tisk(2, ct1=None),
        _tisk(3, third=True),
        _tisk(4, status="vyhlášeno"),
        _tisk(5),
    )
    assert pending_cts(data, voted_cts={5}) == {1: "Zákon 1"}


def test_pending_bill_letters_have_pending_fate() -> None:
    data = _data(_tisk(1))
    pdf = {1: [PdfAmendment(letter="a", submitter_names=["Jan Novák"], raw_text="text")]}
    bills = build_pending_bills(10, {1: "Zákon 1"}, pdf, data)
    assert [(b.schuze, b.bod, b.is_pending) for b in bills] == [(0, 1, True)]
    letters = bill_letters(bills[0], frozenset())
    assert [(r["letter"], r["fate"]) for r in letters] == [("A", "pending")]
    assert tally(letters)["by_fate"]["pending"] == 1


def test_bill_without_parsed_letters_is_skipped() -> None:
    assert build_pending_bills(10, {1: "Zákon 1"}, {1: []}, _data(_tisk(1))) == []


def test_voted_bill_is_not_pending() -> None:
    assert not BillAmendmentData(period=10, schuze=31, bod=3, ct=1).is_pending

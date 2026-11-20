"""Tests for applying admin corrections (services/overrides)."""

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.services.overrides import (
    apply_amendment_summaries,
    apply_briefs,
    apply_vote_questions,
    homepage_pick,
    load_overrides,
    set_override,
)


def _ov(**kinds: dict) -> dict:
    base = {"vote_question": {}, "brief": {}, "amendment_summary": {}, "homepage_pick": {}}
    return base | kinds


def test_set_and_load_roundtrip(tmp_path) -> None:
    set_override(tmp_path, 10, "homepage_pick", "talked_about", {"ct": 285}, "admin")
    ov = load_overrides(tmp_path, 10)
    assert homepage_pick(ov) == 285
    assert ov["homepage_pick"]["talked_about"]["by"] == "admin"
    set_override(tmp_path, 10, "homepage_pick", "talked_about", None, "admin")
    assert homepage_pick(load_overrides(tmp_path, 10)) is None


def test_vote_question_replaces_or_creates_record() -> None:
    steno: dict[int, dict] = {1: {"question": "", "status": "unknown", "quote": "", "url": "u"}}
    ov = _ov(vote_question={"1": {"question": "A?", "by": "a", "at": "t"}, "2": {"question": "B?"}})
    apply_vote_questions(steno, ov)
    assert steno[1]["question"] == "A?" and steno[1]["status"] == "ok" and steno[1]["url"] == "u"
    assert steno[2]["question"] == "B?" and steno[2]["edited"]["by"] == ""


def test_brief_review_hide_and_edit() -> None:
    briefs = {
        1: {"status": "warning", "cs": {"nazev": "x"}, "en": {"nazev": "y"}},
        2: {"status": "ok", "cs": {}},
        3: {"status": "warning", "cs": {"nazev": "old", "koho": "k"}, "en": {"nazev": "old en"}},
    }
    ov = _ov(
        brief={
            "1": {"review": "ok"},
            "2": {"review": "hidden"},
            "3": {"review": "edited", "nazev": "new", "shrnuti": "s"},
        }
    )
    apply_briefs(briefs, ov)
    assert briefs[1]["status"] == "ok" and "edited" in briefs[1]
    assert 2 not in briefs
    assert (
        briefs[3]["cs"] == {"nazev": "new", "koho": "k", "shrnuti": "s"} and briefs[3]["en"] == {}
    )


def test_amendment_summary_by_ct_and_letter() -> None:
    amend = AmendmentVote(letter="b", vote_number=1, summary="ai", summary_warning=True)
    data = {(31, 3): BillAmendmentData(period=10, schuze=31, bod=3, ct=246, amendments=[amend])}
    apply_amendment_summaries(data, _ov(amendment_summary={"246:B": {"summary": "fixed"}}))
    assert amend.summary == "fixed" and not amend.summary_warning

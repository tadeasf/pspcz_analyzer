"""Steno authorization tracking on saved psp.cz pages (no network).

Fixtures in tests/fixtures/steno: live pages of 20 Nov 2026 in each psp.cz state, a
period-9 page, and two pages saved by the Wayback Machine before authorization next to
their current version (s001053: only typography changed; s007044: a speaker line changed
inside the window of vote 86660).
"""

import html
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from pspcz_analyzer.admin.steno_review import steno_report
from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services import steno_page_state as ps
from pspcz_analyzer.services import vote_steno
from pspcz_analyzer.services.amendments import pipeline as amendment_pipeline
from pspcz_analyzer.services.amendments import steno_scraper
from pspcz_analyzer.services.amendments.steno_changes import load_changes, mark_read
from pspcz_analyzer.services.amendments.summary_carry import carry_summaries
from pspcz_analyzer.services.overrides import load_overrides, set_override
from pspcz_analyzer.services.vote_steno_changes import quote_status
from tests.fixtures.sample_data import make_mp_info, make_mp_votes, make_void_votes, make_votes

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "steno"


def _page(name: str) -> str:
    return steno_scraper._detect_decode((FIX / name).read_bytes())


@pytest.mark.parametrize(
    ("name", "status"),
    [
        ("p10_s036010_unauth_uncorrected.htm", "unauthorized"),
        ("p10_s034169_unauth.htm", "unauthorized"),
        ("p10_s024240_unauth_143d.htm", "unauthorized"),
        ("p10_s024055_auth_uncorrected.htm", "authorized"),
        ("p10_s035010_final.htm", "final"),
        ("p9_s010010.htm", "final"),
        ("p10_036_index.htm", "unknown"),
    ],
)
def test_status_of_each_psp_state(name: str, status: str) -> None:
    assert ps.page_status(_page(name))[0] == status


def test_note_menus_and_typography_are_no_text_change() -> None:
    unauth = _page("p10_s034169_unauth.htm")
    authorized = re.sub(r'<p class="status \w+">[^<]*</p>', "", unauth)
    assert ps.page_status(authorized)[0] == "final"
    assert ps.body_sha(authorized) == ps.body_sha(unauth)
    old, new = _page("p10_s001053_wayback_20251116.htm"), _page("p10_s001053_now.htm")
    assert old != new and ps.body_sha(old) == ps.body_sha(new)  # „“ and – only
    assert ps.body_sha(_page("p10_s007044_wayback_20260212.htm")) != ps.body_sha(
        _page("p10_s007044_now.htm")
    )


def test_first_sight_is_a_baseline_then_a_hash_change_counts(tmp_path: Path) -> None:
    now = datetime(2026, 11, 20, 3, tzinfo=UTC)
    old, new = _page("p10_s007044_wayback_20260212.htm"), _page("p10_s007044_now.htm")
    assert not ps.record_check(tmp_path, 7, "s007044.htm", old, now)  # baseline
    assert not ps.is_due(tmp_path, 7, "s007044.htm", now + timedelta(hours=1))
    assert ps.is_due(tmp_path, 7, "s007044.htm", now + timedelta(days=1))  # unauthorized: daily
    assert ps.record_check(tmp_path, 7, "s007044.htm", new, now + timedelta(days=1))
    entry = ps.load_state(tmp_path, 7)["s007044.htm"]
    assert entry["status"] == "final" and entry["amend_stale"] and entry["prev_sha"]
    assert not ps.is_due(tmp_path, 7, "s007044.htm", now + timedelta(days=60))  # final: 90 days
    assert ps.stale_pages(tmp_path, 7) == {"s007044.htm"}
    ps.clear_stale(tmp_path, 7, {"s007044.htm"})
    assert ps.stale_pages(tmp_path, 7) == set()


def test_tracked_download_checks_a_stateless_page_once(tmp_path: Path, monkeypatch) -> None:
    old, new = _page("p10_s001053_wayback_20251116.htm"), _page("p10_s001053_now.htm")
    sdir = steno_scraper._steno_cache_dir(tmp_path, 10)
    (sdir / "1_sub_s001053.html").write_text(old, encoding="utf-8")  # cached before tracking
    fetched: list[str] = []
    monkeypatch.setattr(steno_scraper, "_fetch_steno_page", lambda url: fetched.append(url) or new)
    html, previous = steno_scraper._download_subpage_tracked("b/", "s001053.htm", 10, 1, tmp_path)
    assert (html, previous) == (new, old) and len(fetched) == 1
    assert not ps.load_state(sdir, 1)["s001053.htm"].get("amend_stale")  # baseline
    html, previous = steno_scraper._download_subpage_tracked("b/", "s001053.htm", 10, 1, tmp_path)
    assert (html, previous) == (new, None) and len(fetched) == 1  # final: not due again


def test_quote_status_is_the_status_of_the_page_with_the_quote() -> None:
    first, second = _page("p10_s034169_unauth.htm"), _page("p10_s035010_final.htm")
    quote = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", ps.body_html(first))).split()[20:32])
    pages = [("a.htm", first), ("b.htm", second)]
    states = {"a.htm": "unauthorized", "b.htm": "final"}
    assert quote_status(pages, "b.htm", quote, states) == "unauthorized"
    assert quote_status(pages, "b.htm", "", states) == "final"  # no quote: the vote's page


class _LLM:
    model = "fake"


def _votes(rows: list[tuple[int, int, int]]) -> pl.DataFrame:
    """``(id_hlasovani, schuze, cislo)`` rows of an old sitting."""
    return pl.DataFrame(
        {
            "id_hlasovani": [r[0] for r in rows],
            "schuze": [r[1] for r in rows],
            "cislo": [r[2] for r in rows],
            "datum": ["1.1.2026"] * len(rows),
            "nazev_dlouhy": ["Bod"] * len(rows),
            "vysledek": ["A"] * len(rows),
        }
    )


def _run(
    monkeypatch, tmp_path: Path, schuze: int, votes: pl.DataFrame, pages: list, previous: dict
) -> tuple[dict, list[int]]:
    """One ``_sitting_records`` run on given pages; returns records and the asked votes."""
    asked: list[int] = []

    def fake_ask(llm, text, span, cislo, bod, *rest) -> dict:
        asked.append(cislo)
        return {"question": f"Otázka {cislo}", "quote": "", "status": "ok", "pro": "ano"}

    monkeypatch.setattr(vote_steno, "_ask", fake_ask)
    monkeypatch.setattr(vote_steno, "sitting_pages", lambda *a, **k: (pages, previous))
    out = tmp_path / "vote_steno" / "10" / f"{schuze}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    records, _ = vote_steno._sitting_records(_LLM(), 10, schuze, votes, tmp_path, out)  # type: ignore[arg-type]
    out.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
    return records, asked


@pytest.mark.parametrize("legacy", [False, True])
def test_authorization_without_text_change_reuses_the_record(
    monkeypatch, tmp_path: Path, legacy: bool
) -> None:
    old, new = _page("p10_s001053_wayback_20251116.htm"), _page("p10_s001053_now.htm")
    votes = _votes([(86335, 1, 9), (86336, 1, 10)])
    first, asked = _run(monkeypatch, tmp_path, 1, votes, [("s001053.htm", old)], {})
    assert sorted(asked) == [9, 10]
    if legacy:  # a record written before body_sha existed
        out = tmp_path / "vote_steno" / "10" / "1.json"
        recs = json.loads(out.read_text())
        out.write_text(
            json.dumps(
                {k: {f: v for f, v in r.items() if f != "body_sha"} for k, r in recs.items()}
            )
        )
    pages = [("s001053.htm", new)]
    second, asked = _run(monkeypatch, tmp_path, 1, votes, pages, {"s001053.htm": old})
    assert asked == []  # no LLM call
    assert second["86335"]["question"] == "Otázka 9" and "previous" not in second["86335"]
    assert second["86335"]["sha"] != first["86335"]["sha"] and second["86335"]["body_sha"]


def test_text_change_in_the_window_reextracts_and_keeps_the_old_record(
    monkeypatch, tmp_path: Path
) -> None:
    old, new = _page("p10_s007044_wayback_20260212.htm"), _page("p10_s007044_now.htm")
    votes = _votes([(86660, 7, 44)])
    _run(monkeypatch, tmp_path, 7, votes, [("s007044.htm", old)], {})
    stored = json.loads((tmp_path / "vote_steno" / "10" / "7.json").read_text())
    stored["86660"]["question"] = "Stará otázka"  # what the old extraction said
    (tmp_path / "vote_steno" / "10" / "7.json").write_text(json.dumps(stored))
    pages = [("s007044.htm", new)]
    records, asked = _run(monkeypatch, tmp_path, 7, votes, pages, {"s007044.htm": old})
    assert asked == [44]
    prev = records["86660"]["previous"]
    assert prev["question"] == "Stará otázka" and prev["window"] and prev["replaced_at"]
    assert records["86660"]["question"] == "Otázka 44"


def _period(tmp_path: Path) -> PeriodData:
    data = PeriodData(
        period=10,
        votes=make_votes(),
        mp_votes=make_mp_votes(),
        void_votes=make_void_votes(),
        mp_info=make_mp_info(),
    )
    data.overrides = load_overrides(tmp_path, 10)
    return data


def _correct(tmp_path: Path, kind: str, key: str, fields: dict, at: str) -> None:
    """An admin correction made at a given time (set_override stamps seconds only)."""
    set_override(tmp_path, 10, kind, key, fields, "admin")
    path = tmp_path / "overrides" / "10.json"
    data = json.loads(path.read_text())
    data[kind][key]["at"] = at
    path.write_text(json.dumps(data))


def test_corrected_vote_goes_back_to_review_when_its_steno_changes(
    monkeypatch, tmp_path: Path
) -> None:
    old, new = _page("p10_s007044_wayback_20260212.htm"), _page("p10_s007044_now.htm")
    votes = _votes([(86660, 7, 44)])
    _run(monkeypatch, tmp_path, 7, votes, [("s007044.htm", old)], {})
    _correct(tmp_path, "vote_question", "86660", {"question": "Opraveno"}, "2026-01-01T00:00:00")
    assert steno_report(_period(tmp_path), tmp_path)["votes"] == []
    _run(monkeypatch, tmp_path, 7, votes, [("s007044.htm", new)], {"s007044.htm": old})
    rows = steno_report(_period(tmp_path), tmp_path)["votes"]
    assert [(r["id"], r["corrected"], r["new_question"]) for r in rows] == [
        (86660, "Opraveno", "Otázka 44")
    ]
    _correct(tmp_path, "vote_question", "86660", {"question": "Znovu"}, "2099-01-01T00:00:00")
    assert steno_report(_period(tmp_path), tmp_path)["votes"] == []  # saved again: reviewed


def _bill(schuze: int, bod: int, ct: int, summary: str) -> BillAmendmentData:
    amend = AmendmentVote(letter="A", vote_number=1, amendment_text="text A", summary=summary)
    return BillAmendmentData(period=10, schuze=schuze, bod=bod, ct=ct, amendments=[amend])


def test_changed_steno_invalidates_the_bill_summaries_and_reviews(tmp_path: Path) -> None:
    sdir = steno_scraper._steno_cache_dir(tmp_path, 10)
    old, new = _page("p10_s007044_wayback_20260212.htm"), _page("p10_s007044_now.htm")
    ps.record_check(sdir, 7, "s007044.htm", old)
    ps.record_check(sdir, 7, "s007044.htm", new)  # changed after the last amendment run
    ps.record_check(sdir, 7, "s007045.htm", old)  # baseline only
    _correct(tmp_path, "amendment_summary", "246:A", {"summary": "Ručně"}, "2026-01-01T00:00:00")
    previous = {(7, 3): _bill(7, 3, 246, "staré A"), (7, 4): _bill(7, 4, 247, "B souhrn")}
    used = {(7, 3): ["s007044.htm"], (7, 4): ["s007045.htm"]}
    kept = amendment_pipeline._drop_changed(tmp_path, 10, used, previous)
    assert set(kept) == {(7, 4)}
    bills = [_bill(7, 3, 246, ""), _bill(7, 4, 247, "")]
    carry_summaries(bills, kept)
    assert [b.amendments[0].summary for b in bills] == ["", "B souhrn"]
    assert load_changes(tmp_path, 10)["246:A"]["old_summary"] == "staré A"
    letters = steno_report(_period(tmp_path), tmp_path)["letters"]
    assert [(r["ref"], r["corrected"], r["old_summary"]) for r in letters] == [
        ("246:A", "Ručně", "staré A")
    ]
    mark_read(tmp_path, 10, used)
    assert ps.stale_pages(sdir, 7) == set()
    assert amendment_pipeline._drop_changed(tmp_path, 10, used, previous) == previous

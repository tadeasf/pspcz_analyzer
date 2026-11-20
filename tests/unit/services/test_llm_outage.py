"""LLM outage handling (20 Nov 2026): fail fast, never store a failed call as a result."""

import json
from pathlib import Path
from unittest.mock import patch

import httpx
import polars as pl
import pytest

from pspcz_analyzer.services import vote_steno
from pspcz_analyzer.services.amendments import summarizer
from pspcz_analyzer.services.llm import LLMClient, LLMUnavailable
from pspcz_analyzer.services.llm.health import FailureCounter, failures, is_outage

_REQ = httpx.Request("POST", "https://llm.example/v1/chat/completions")


def _status(code: int) -> httpx.HTTPStatusError:
    return httpx.HTTPStatusError("x", request=_REQ, response=httpx.Response(code, request=_REQ))


def test_outage_errors_and_rejected_requests() -> None:
    assert is_outage(httpx.ReadTimeout("t")) and is_outage(httpx.ConnectError("c"))
    assert is_outage(_status(503)) and is_outage(_status(429))
    assert not is_outage(_status(400))  # a prompt the backend rejects is no outage


def test_counter_stops_at_the_limit_and_a_success_resets_it() -> None:
    counter = FailureCounter(3)
    counter.failed("[t]", "a")
    counter.failed("[t]", "b")
    counter.ok()
    counter.failed("[t]", "c")
    counter.failed("[t]", "d")
    with pytest.raises(LLMUnavailable, match="3 LLM requests in a row failed"):
        counter.failed("[t]", "e")


def _client() -> LLMClient:
    return LLMClient(provider="openai", base_url="https://llm.example/v1", model="m", api_key="k")


def test_timeouts_stop_the_run_but_a_rejected_request_does_not_count() -> None:
    client = _client()
    with patch("httpx.post", side_effect=httpx.ReadTimeout("t")):
        assert client._generate("p", "s") is None
        assert client._generate("p", "s") is None
    with patch("httpx.post", side_effect=_status(400)):
        assert client._generate("p", "s") is None  # backend answered: count back to 0
    with patch("httpx.post", side_effect=httpx.ReadTimeout("t")):
        client._generate("p", "s")
        client._generate("p", "s")
        with pytest.raises(LLMUnavailable):
            client._generate("p", "s")
    failures.reset()


def test_summarizer_does_not_swallow_an_outage() -> None:
    class _Down:
        def summarize_bilingual(self, text: str, title: str) -> dict:
            raise LLMUnavailable("down")

    with pytest.raises(LLMUnavailable):
        summarizer._summarize_bill_text(_Down(), "text", "title", bill_index=1, total_bills=1)  # type: ignore[arg-type]


# ── vote steno: a failed call is not stored, an unknown answer is ────────

_PAGE = (
    "<p>Předseda: Budeme hlasovat o návrhu poslance Nováka, aby se bod přerušil do pátku."
    ' Zahajuji hlasování.</p><a href="/sqw/hlasy.sqw?G={vid}" id="h1">hlasování číslo 1</a>'
    ", přihlášeno 150 poslanců, pro 90, proti 40. Návrh byl přijat.</p>"
)


class _LLM:
    """Scripted ``_generate_json`` replies: None = failed call, dict = an answer."""

    model = "fake"

    def __init__(self, replies: list[dict | None]) -> None:
        self.replies = replies
        self.calls = 0

    def _generate_json(self, prompt: str, system: str, schema: dict) -> dict | None:
        self.calls += 1
        return self.replies.pop(0) if self.replies else None


# the model answered, but its quote is not in the steno: a valid "unknown"
_UNKNOWN_ANSWER = {
    "druh": "procedural",
    "otazka": "Přerušit bod?",
    "citace": "nic takového tam není",
}


def _run(monkeypatch, tmp_path: Path, llm: _LLM, page: str) -> dict:
    vid = 101
    votes = pl.DataFrame(
        {
            "id_hlasovani": [vid],
            "schuze": [5],
            "cislo": [1],
            "datum": ["1.1.2026"],
            "nazev_dlouhy": ["Bod"],
            "vysledek": ["A"],
        }
    )
    monkeypatch.setattr(vote_steno, "sitting_pages", lambda *a, **k: [("s005001.htm", page)])
    out = tmp_path / "5.json"
    records, _ = vote_steno._sitting_records(llm, 10, 5, votes, tmp_path, out)  # type: ignore[arg-type]
    out.write_text(json.dumps(records, ensure_ascii=False))
    return records


def test_failed_call_is_not_stored_and_is_asked_next_run(monkeypatch, tmp_path: Path) -> None:
    page = _PAGE.format(vid=101)
    assert _run(monkeypatch, tmp_path, _LLM([None, None]), page) == {}
    llm = _LLM([_UNKNOWN_ANSWER, _UNKNOWN_ANSWER])
    records = _run(monkeypatch, tmp_path, llm, page)
    assert llm.calls >= 1 and records["101"]["status"] == "unknown"


def test_unknown_answer_stays_cached(monkeypatch, tmp_path: Path) -> None:
    page = _PAGE.format(vid=101)
    _run(monkeypatch, tmp_path, _LLM([_UNKNOWN_ANSWER, _UNKNOWN_ANSWER]), page)
    again = _LLM([])
    records = _run(monkeypatch, tmp_path, again, page)
    assert again.calls == 0 and records["101"]["status"] == "unknown"


def test_failed_call_after_a_text_change_keeps_the_old_record(monkeypatch, tmp_path: Path) -> None:
    page = _PAGE.format(vid=101)
    first = _run(monkeypatch, tmp_path, _LLM([_UNKNOWN_ANSWER, _UNKNOWN_ANSWER]), page)
    changed = page.replace("do pátku", "do čtvrtka")
    kept = _run(monkeypatch, tmp_path, _LLM([None, None]), changed)
    assert kept["101"]["sha"] == first["101"]["sha"]  # old record, so the next run asks again
    llm = _LLM([_UNKNOWN_ANSWER, _UNKNOWN_ANSWER])
    assert _run(monkeypatch, tmp_path, llm, changed)["101"]["sha"] != first["101"]["sha"]
    assert llm.calls >= 1

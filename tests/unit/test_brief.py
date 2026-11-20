"""Tests for the AI brief check and view (#109, #97)."""

from typing import Any

import pytest

from pspcz_analyzer.services.tisk import brief as brief_module
from pspcz_analyzer.services.tisk.brief import (
    FIELDS,
    brief_issues,
    brief_view,
    generate_brief,
    text_issues,
)

SOURCE = "Cena roční známky činí 2 570 Kč. Účinnost od 1. ledna 2027."
CS = {
    "nazev": "Zmrazení ceny dálniční známky",
    "shrnuti": "Roční známka bude stát 2 570 Kč.",
    "koho": "řidičů",
    "kolik": "roční 2570 Kč",
    "od_kdy": "od 1. 1. 2027",
}
EN = {**CS, "nazev": "Motorway vignette price freeze"}


def test_clean_brief_passes() -> None:
    assert brief_issues(CS, EN, SOURCE) == []


def test_cyrillic_drift_and_invented_number_flagged() -> None:
    cs = {**CS, "shrnuti": "Roční známka стрáží 3 000 Kč."}
    issues = brief_issues(cs, EN, SOURCE)
    assert "script:shrnuti_cs" in issues
    assert "number:3000" in issues


def test_english_in_czech_field_and_empty_flagged() -> None:
    cs = {**CS, "nazev": "", "koho": "drivers of the cars and trucks"}
    issues = brief_issues(cs, EN, SOURCE)
    assert "empty" in issues
    assert "english:koho" in issues


def test_issue_97_sample_is_flagged() -> None:
    """The corrupt text from #97: one English word, a stray "×", an unfinished sentence."""
    cs = {
        **CS,
        "shrnuti": "Změněn byl rozsah definition internalizátora. Vymezeny × se výjimek",
    }
    issues = brief_issues(cs, EN, SOURCE)
    assert {"english:shrnuti", "sign:shrnuti", "unfinished:shrnuti"} <= set(issues)
    assert brief_issues({**CS, "kolik": "3 × 2 570 Kč"}, EN, SOURCE + " 3") == []


def test_view_falls_back_to_czech() -> None:
    brief = {"status": "ok", "cs": CS, "en": {"shrnuti": ""}}
    assert (brief_view(brief, "en") or {})["nazev"] == CS["nazev"]
    assert brief_view(None, "cs") is None


def test_text_issues_for_amendment_summary() -> None:
    assert text_issues("Kraje by dostaly 6 % z daně.", "Regions get 6 %.", "6 % výnosu") == []
    assert text_issues("Kraje dostanou 9 %.", "", "6 % výnosu") == ["number:9"]
    assert text_issues("Kraje получат 6 %.", "", "6 %") == ["script"]


class FakeLLM:
    """Stands in for LLMClient: returns the queued replies of _generate_json in order."""

    model = "fake-model"

    def __init__(self, *replies: dict[str, str]) -> None:
        self.replies = list(replies)
        self.calls = 0

    def _generate_json(self, prompt: str, system: str, schema: dict[str, Any]) -> dict[str, str]:
        self.calls += 1
        return self.replies[min(self.calls, len(self.replies)) - 1]


def _reply(cs: dict[str, str]) -> dict[str, str]:
    return {**cs, **{f"{f}_en": EN[f] for f in FIELDS}}


_BAD = _reply({**CS, "shrnuti": "Roční známka стрáží 3 000 Kč."})


@pytest.fixture(autouse=True)
def _two_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(brief_module, "LLM_EMPTY_RETRIES", 2)


def test_generate_clean_brief_is_ok_first_try() -> None:
    llm = FakeLLM(_reply(CS))
    brief = generate_brief(llm, SOURCE, "Návrh zákona")  # type: ignore[arg-type]
    assert brief is not None
    assert (brief["status"], brief["issues"], llm.calls) == ("ok", [], 1)
    assert brief["cs"] == CS


def test_generate_corrupt_brief_is_retried_then_stored_as_warning() -> None:
    llm = FakeLLM(_BAD)
    brief = generate_brief(llm, SOURCE, "Návrh zákona")  # type: ignore[arg-type]
    assert brief is not None
    assert llm.calls == 3  # first try + 2 retries
    assert brief["status"] == "warning"
    assert {"script:shrnuti_cs", "number:3000"} <= set(brief["issues"])


def test_generate_retry_that_comes_back_clean_is_ok() -> None:
    llm = FakeLLM(_BAD, _reply(CS))
    brief = generate_brief(llm, SOURCE, "Návrh zákona")  # type: ignore[arg-type]
    assert brief is not None
    assert (brief["status"], llm.calls) == ("ok", 2)

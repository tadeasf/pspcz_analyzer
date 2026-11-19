"""Tests for the AI brief check and view (#109, #97)."""

from pspcz_analyzer.services.tisk.brief import brief_issues, brief_view, text_issues

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


def test_view_falls_back_to_czech() -> None:
    brief = {"status": "ok", "cs": CS, "en": {"shrnuti": ""}}
    assert (brief_view(brief, "en") or {})["nazev"] == CS["nazev"]
    assert brief_view(None, "cs") is None


def test_text_issues_for_amendment_summary() -> None:
    assert text_issues("Kraje by dostaly 6 % z daně.", "Regions get 6 %.", "6 % výnosu") == []
    assert text_issues("Kraje dostanou 9 %.", "", "6 % výnosu") == ["number:9"]
    assert text_issues("Kraje получат 6 %.", "", "6 %") == ["script"]

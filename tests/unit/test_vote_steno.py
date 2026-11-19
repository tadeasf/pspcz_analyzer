"""Tests for vote windows and kinds from steno (#102)."""

from pspcz_analyzer.services.vote_steno import vote_kind, vote_windows

PAGE_1 = (
    "<p>Nyní návrh B1 poslance Papajanovského, který se týká krajů.</p>"
    "<p>Zahajuji hlasování.</p>"
    '<p><a href="/sqw/hlasy.sqw?G=88539" id="h9">Hlasování číslo&nbsp;9</a>, návrh byl zamítnut.</p>'
    "<p>Přednesu návrh usnesení o celém návrhu zákona, sněmovní tisk 246.</p>"
)
PAGE_2 = '<p>Zahajuji hlasování.</p><a href="/sqw/hlasy.sqw?G=88540" id="h10">Hlasování 10</a>'


def test_window_spans_pages_and_ends_at_anchor() -> None:
    w = vote_windows([("s031088.htm", PAGE_1), ("s031089.htm", PAGE_2)])
    assert w[88539]["cislo"] == 9
    assert w[88539]["window"].endswith("Zahajuji hlasování.")
    assert "návrh byl zamítnut" in w[88540]["window"]
    assert w[88540]["page"] == "s031089.htm"
    assert w[88540]["window"].startswith("Hlasování číslo 9")  # previous result opens the window


def test_kind_closest_to_vote_wins() -> None:
    w = vote_windows([("a", PAGE_1), ("b", PAGE_2)])
    assert vote_kind(w[88539]["window"]) == "amendment"
    assert vote_kind(w[88540]["window"]) == "final"
    assert vote_kind("Budeme hlasovat o sloučení rozpravy k bodům 14 a 15.") == "procedural"
    assert vote_kind("Dobrý den.") == "other"

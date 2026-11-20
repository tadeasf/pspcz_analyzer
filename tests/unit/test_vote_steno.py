"""Tests for vote anchoring, kinds and links from steno (#102)."""

from typing import Any

import polars as pl

from pspcz_analyzer.services.speeches import speech_counts
from pspcz_analyzer.services.vote_steno import _ask, _ask_once
from pspcz_analyzer.services.vote_steno_text import (
    anchor_check,
    earlier_proposals,
    is_referential,
    link_repeats,
    rule_kind,
    sitting_text,
    vote_kind,
    vote_windows,
)

PAGE_1 = (
    "<p>Nyní návrh B1 poslance Papajanovského, který se týká krajů.</p>"
    "<p>Zahajuji hlasování.</p>"
    '<p><a href="/sqw/hlasy.sqw?G=88539" id="h9">Hlasování číslo&nbsp;9</a>, návrh byl zamítnut.</p>'
    "<p>Přednesu návrh usnesení o celém návrhu zákona, sněmovní tisk 246.</p>"
)
PAGE_2 = '<p>Zahajuji hlasování.</p><a href="/sqw/hlasy.sqw?G=88540" id="h10">Hlasování 10</a>'


def _a(vid: int, cislo: int, text: str) -> str:
    return f'<a href="/sqw/hlasy.sqw?G={vid}" id="h{cislo}">{text}</a>'


def test_window_spans_pages_and_ends_at_anchor() -> None:
    w = vote_windows([("s031088.htm", PAGE_1), ("s031089.htm", PAGE_2)])
    assert w[88539]["cislo"] == 9
    assert w[88539]["window"].endswith("Zahajuji hlasování.")
    assert "návrh byl zamítnut" in w[88540]["window"]
    assert w[88540]["page"] == "s031089.htm"
    assert w[88540]["window"].startswith("Hlasování číslo 9")  # previous result opens the window


def test_anchor_variants_and_references() -> None:
    """Announcements in all three forms anchor; a later reference does not move them."""
    page = (
        "<p>Benda navrhuje pevný čas. Kdo je pro? Tak zmatečné "
        + _a(1, 273, "hlasování (číslo&nbsp;273")
        + ", přihlášeno 150, pro 34.)</p><p>Hlasujeme znovu návrh Bendy.</p>"
        + _a(2, 274, "Hlasování číslo&nbsp;274")
        + ", přihlášeno 152, pro 46.</p><p>Okamura navrhuje 10 minut. Je to "
        + _a(3, 278, "hlasování&nbsp; 278")
        + " , přihlášeno 152 poslanců.</p>"
        + "<p>Haas: revokuje usnesení přijaté v "
        + _a(1, 273, "hlasování pořadové číslo 277")
        + ", kterým se stanovil čas. Zahajuji hlasování.</p>"
        + _a(4, 279, "Hlasování číslo 279")
        + ", přihlášeno 157.</p>"
    )
    w = vote_windows([("p", page)])
    assert [w[i]["cislo"] for i in (1, 2, 3, 4)] == [273, 274, 278, 279]
    assert w[1]["window"].endswith("Tak zmatečné")  # first announcement, not the reference
    assert "revokuje usnesení" in w[4]["window"]  # the reference did not cut vote 279's window
    assert "Okamura navrhuje" in w[3]["window"]


def test_kind_closest_to_vote_wins() -> None:
    w = vote_windows([("a", PAGE_1), ("b", PAGE_2)])
    assert vote_kind(w[88539]["window"]) == "amendment"
    assert vote_kind(w[88540]["window"]) == "final"
    assert vote_kind("Budeme hlasovat o sloučení rozpravy k bodům 14 a 15.") == "procedural"
    assert vote_kind("Dobrý den.") == "other"
    assert rule_kind("Hlasujeme o přikázání výboru pro zdravotnictví.") == ("committee", "")
    assert rule_kind("Budeme hlasovat o zpochybnění hlasování.") == ("procedural", "challenge")


def test_earlier_proposals_for_referential_lead_in() -> None:
    text = (
        "Předseda PSP Tomio Okamura : Navrhuji omezit řečnickou dobu na 10 minut. "
        + "x " * 400
        + "Poslanec Marek Benda : Navrhuji pevný čas hlasování. "
        + "y " * 400
    )
    tail = "Předseda PSP Tomio Okamura : A teď se bude hlasovat o mém návrhu. Zahajuji hlasování."
    full = text + tail
    assert is_referential(tail)
    snips = earlier_proposals(full, len(text), tail)
    assert snips and snips[0].startswith("Tomio Okamura: ")
    assert "10 minut" in snips[0]


def _votes(rows: list[tuple[int, int, int, str]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=["id_hlasovani", "cislo", "bod", "vysledek"], orient="row")


def test_link_repeats_challenge_and_chair_repeat() -> None:
    pages = [
        (
            "p",
            "<p>Zahajuji. Kdo je pro? Tak zmatečné "
            + _a(10, 273, "hlasování (číslo 273")
            + ", přihlášeno 1.</p><p>Znovu.</p>"
            + _a(11, 274, "Hlasování číslo 274")
            + ", přihlášeno 1.</p><p>Revokace.</p>"
            + _a(12, 280, "Hlasování číslo 280")
            + ", přihlášeno 1.</p><p>Zpochybňuji hlasování 280. Budeme hlasovat o zpochybnění.</p>"
            + _a(13, 281, "Hlasování číslo 281")
            + ", přihlášeno 1.</p><p>Opět o revokaci.</p>"
            + _a(14, 282, "Hlasování číslo 282")
            + ", přihlášeno 1.</p>",
        )
    ]
    text, spans = sitting_text(pages)
    votes = _votes(
        [
            (10, 273, 5, "R"),
            (11, 274, 5, "R"),
            (12, 280, 5, "A"),
            (13, 281, 5, "A"),
            (14, 282, 5, "R"),
        ]
    )
    records: dict[str, dict] = {str(v): {"cislo": spans[v]["cislo"]} for v in spans}
    link_repeats(records, votes, text, spans)
    assert records["12"]["invalid_by"] == 13
    assert records["12"]["repeated_as"] == 14
    assert records["14"]["repeats"] == 12
    assert records["10"]["chair_repeated_as"] == 11
    assert (
        "invalid_by" not in records["10"]
    )  # the chair's word "zmatečné" is not psp.cz's void flag


def test_anchor_check_reports_only_mismatches() -> None:
    votes = _votes([(1, 1, 5, "A"), (2, 2, 5, "A"), (3, 3, 6, "A")])
    assert anchor_check(votes, {1, 3}) == {"5": {"votes": 2, "anchored": 1}}


class _FakeLLM:
    def __init__(self, reply: dict[str, Any]) -> None:
        self.reply = reply

    def _generate_json(self, prompt: str, system: str, schema: dict) -> dict[str, Any]:
        return self.reply


_REPLY = {
    "druh": "procedural",
    "podtyp": "speaking_time",
    "otazka": "Omezit řečnickou dobu na 10 minut?",
    "otazka_en": "Limit speaking time to 10 minutes?",
    "pro_znamena": "omezit řečnickou dobu",
    "pro_znamena_en": "limit speaking time",
    "navrhovatel": "Tomio Okamura",
    "citace": "Navrhuji omezit řečnickou dobu na 10 minut.",
}


def test_ask_quote_from_earlier_context_is_uncertain() -> None:
    window = "Hlasování číslo 278, přihlášeno 152. A teď se bude hlasovat o mém návrhu."
    snips = ["Tomio Okamura: Navrhuji omezit řečnickou dobu na 10 minut."]
    out = _ask_once(_FakeLLM(_REPLY), window, snips, 279, "tisk 231")  # type: ignore[arg-type]
    assert out["status"] == "uncertain"
    assert (out["kind"], out["subkind"]) == ("procedural", "speaking_time")
    assert out["proposer"] == "Tomio Okamura"


def test_ask_rejects_result_formula_quote_and_invented_proposer() -> None:
    window = "Hlasování číslo 280, přihlášeno 157 poslanců, pro 80. Návrh byl přijat. Zahajuji."
    bad = {**_REPLY, "citace": "Hlasování číslo 280, přihlášeno 157 poslanců, pro 80."}
    assert _ask_once(_FakeLLM(bad), window, [], 281, "")["status"] == "unknown"  # type: ignore[arg-type]
    window2 = "Navrhuji omezit řečnickou dobu na 10 minut. Zahajuji hlasování."
    out = _ask_once(_FakeLLM(_REPLY), window2, [], 279, "")  # type: ignore[arg-type]
    assert out["status"] == "ok" and out["proposer"] == ""  # Okamura is not in the text


def test_ask_quote_not_in_steno_is_unknown() -> None:
    # #100: an invented quote (not in the window or any earlier context) must not
    # yield a guessed subject, even after the second pass with more context
    text = (
        "Předseda PSP Tomio Okamura: Budeme hlasovat o návrhu na zamítnutí v prvém čtení. "
        "Zahajuji hlasování. Kdo je pro?"
    )
    invented = {**_REPLY, "citace": "Navrhuji zrušit poplatky za Českou televizi úplně."}
    out = _ask(_FakeLLM(invented), text, {"start": 0, "end": len(text)}, 287, "tisk 231")  # type: ignore[arg-type]
    assert out["status"] == "unknown"
    assert not out.get("question")


INDEX = '<a name="b3" id="b3"></a><a href="31-2.html#q7">Projednávání</a><a name="b4" id="b4"></a>'
DAY = (
    "<h1>Stenografický zápis 31. schůze, 23.&nbsp;září&nbsp;2026</h1>"
    '<a name="q7" id="q7"></a><a href="s031050.htm#r1">Předseda PSP Tomio Okamura</a>'
    '<a href="s031050.htm#r2">Poslanec Jan Novák</a><a href="s031051.htm#r1">Ministryně Eva Malá</a>'
    '<a name="q8" id="q8"></a><a href="s031052.htm#r1">Poslanec Petr Král</a>'
)


def test_speech_counts_skip_chair_and_stop_at_next_section() -> None:
    assert speech_counts(INDEX, {"31-2.html": DAY}) == {3: {"2026-09-23": 2}}


def test_merged_debate_counts_for_every_item() -> None:
    index = (
        '<a name="b2" id="b2"></a><b>2. A</b><a href="31-2.html#q8">Projednávání</a>'
        '<a name="b3" id="b3"></a><b>3. B</b>'
        '<!--bz--><b>Sloučená rozprava k bodům 2 a 3</b><br><a href="31-2.html#q7">Projednávání</a></div>'
    )
    counts = speech_counts(index, {"31-2.html": DAY})
    assert counts[2] == {"2026-09-23": 3}  # 1 own (q8) + 2 merged (q7)
    assert counts[3] == {"2026-09-23": 2}

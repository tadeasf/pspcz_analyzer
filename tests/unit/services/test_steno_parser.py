"""Tests for stenographic record amendment parser."""

from pspcz_analyzer.services.amendments.steno_parser import (
    _clean_html,
    _extract_section,
    _normalize_result,
    _parse_block,
    _parse_letter_groups,
    _split_into_blocks,
    parse_steno_amendments,
)

# ── Fixtures: realistic steno HTML snippets ──────────────────────────────────

STENO_SIMPLE = """
<html><body>
<p>Předsedající řekl: Nyní přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Nejprve budeme hlasovat o pozměňovacím návrhu označeném písmenem A.
Stanovisko výboru je doporučující. Předkladatel? (Souhlas.)</p>
<p>Hlasování číslo 42. Kdo je pro? Kdo je proti?
Přijato. Pro 120, proti 30.</p>
<p>Dále pozměňovací návrh pod označením B1.
Stanovisko výboru je nedoporučující. Předkladatel? (Nesouhlas.)</p>
<p>Hlasování číslo 43. Kdo je pro? Kdo je proti?
Zamítnuto. Pro 50, proti 100.</p>
<p>Nyní budeme hlasovat o návrhu zákona jako celku.</p>
<p>Hlasování číslo 44. Kdo je pro? Kdo je proti?
Přijato. Pro 130, proti 20.</p>
</body></html>
"""

STENO_CHALLENGE = """
<html><body>
<p>Nyní přikročíme k hlasování o pozměňovacích návrzích k tomuto tisku.</p>
<p>Pozměňovací návrh písmenem C. Stanovisko výboru je doporučující.</p>
<p>Hlasování číslo 50. Přijato.</p>
<p>Poslanec Novák: zpochybňuji hlasování.</p>
<p>Hlasování číslo 51. Zamítnuto.</p>
</body></html>
"""

STENO_WITHDRAWAL = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem D.</p>
<p>Poslankyně Nová stahuji pozměňovací návrh.</p>
<p>Pozměňovací návrh pod označením E.</p>
<p>Hlasování číslo 60. Přijato.</p>
</body></html>
"""

STENO_GROUPED = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Budeme hlasovat o pozměňovacím návrhu označeným písmenem E1 a F2.
Stanovisko výboru je bez stanoviska.</p>
<p>Hlasování číslo 70. Návrh byl přijat.</p>
</body></html>
"""

STENO_LEG_TECH = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Nejprve budeme hlasovat o legislativně-technických úpravách podle §&nbsp;95.</p>
<p>Hlasování číslo 80. Přijato.</p>
</body></html>
"""

STENO_NO_SECTION = """
<html><body>
<p>Toto je pouze diskuse bez hlasování.</p>
<p>Pan poslanec Novák hovořil o novele zákona.</p>
</body></html>
"""


class TestCleanHtml:
    def test_strips_tags(self):
        assert _clean_html("<p>Hello <b>world</b></p>") == "Hello world"

    def test_decodes_entities(self):
        result = _clean_html("A&amp;B &lt;C&gt; &quot;D&quot;")
        assert "A&B" in result
        assert "<C>" in result

    def test_normalizes_whitespace(self):
        assert _clean_html("  foo   bar  \n  baz  ") == "foo bar baz"

    def test_replaces_nbsp(self):
        assert "§ 95" in _clean_html("§&nbsp;95")
        assert "§ 95" in _clean_html("§\xa095")


class TestExtractSection:
    def test_finds_section(self):
        text = _clean_html(STENO_SIMPLE)
        section = _extract_section(text)
        assert section != ""
        assert "pozměňovac" in section.lower()

    def test_no_section_returns_empty(self):
        text = _clean_html(STENO_NO_SECTION)
        section = _extract_section(text)
        assert section == ""


class TestNormalizeResult:
    def test_accepted(self):
        assert _normalize_result("Přijato") == "accepted"
        assert _normalize_result("Návrh byl přijat") == "accepted"

    def test_rejected(self):
        assert _normalize_result("Zamítnuto") == "rejected"

    def test_unknown(self):
        assert _normalize_result("Něco jiného") == "unknown"


class TestParseLetterGroups:
    def test_single_letter(self):
        primary, grouped = _parse_letter_groups("A")
        assert primary == "A"
        assert grouped == []

    def test_two_letters_with_a(self):
        primary, grouped = _parse_letter_groups("E1 a F2")
        assert primary == "E1"
        assert grouped == ["F2"]

    def test_comma_separated(self):
        primary, grouped = _parse_letter_groups("A, B")
        assert primary == "A"
        assert grouped == ["B"]


class TestSplitIntoBlocks:
    def test_splits_at_vote_numbers(self):
        section = "Návrh A. Hlasování číslo 1. Přijato. Návrh B. Hlasování číslo 2. Zamítnuto."
        blocks = _split_into_blocks(section)
        assert len(blocks) >= 2

    def test_single_block(self):
        section = "Hlasování číslo 5. Přijato."
        blocks = _split_into_blocks(section)
        assert len(blocks) >= 1


class TestParseBlock:
    def test_extracts_letter(self):
        text = "pozměňovacím návrhu označeným písmenem A Hlasování číslo 10. Přijato."
        block = _parse_block(text)
        assert block.letter == "A"

    def test_extracts_committee_stance(self):
        text = "Stanovisko výboru je doporučující. Hlasování číslo 10. Přijato."
        block = _parse_block(text)
        assert block.committee_stance == "doporučující"

    def test_extracts_proposer_stance(self):
        text = "Předkladatel? (Souhlas.) Hlasování číslo 10. Přijato."
        block = _parse_block(text)
        assert block.proposer_stance == "souhlas"

    def test_extracts_vote_number_and_result(self):
        text = "Hlasování číslo 42. Přijato."
        block = _parse_block(text)
        assert block.vote_number == 42
        assert block.result == "accepted"

    def test_rejected_result(self):
        text = "Hlasování číslo 43. Zamítnuto."
        block = _parse_block(text)
        assert block.result == "rejected"

    def test_detects_final_vote(self):
        text = "návrhu zákona jako celku Hlasování číslo 44. Přijato."
        block = _parse_block(text)
        assert block.is_final is True

    def test_detects_challenge(self):
        text = "zpochybňuji hlasování Hlasování číslo 51. Zamítnuto."
        block = _parse_block(text)
        assert block.is_challenge is True

    def test_detects_withdrawal(self):
        text = "stahuji pozměňovací návrh."
        block = _parse_block(text)
        assert block.is_withdrawal is True

    def test_detects_leg_tech(self):
        text = "legislativně-technických úprav § 95 Hlasování číslo 80. Přijato."
        block = _parse_block(text)
        assert block.is_leg_tech is True


class TestParsestenoAmendments:
    def test_simple_two_amendments_and_final(self):
        amendments, confidence, warnings = parse_steno_amendments(STENO_SIMPLE)
        # Should find at least 2 regular amendments + final vote
        letters = [a.letter for a in amendments]
        assert "A" in letters
        assert "B1" in letters
        # Final vote should be detected
        finals = [a for a in amendments if a.is_final_vote]
        assert len(finals) >= 1
        # Confidence should be relatively high
        assert confidence >= 0.5

    def test_amendment_a_accepted(self):
        amendments, _, _ = parse_steno_amendments(STENO_SIMPLE)
        a_amends = [a for a in amendments if a.letter == "A" and not a.is_final_vote]
        assert len(a_amends) >= 1
        assert a_amends[0].result == "accepted"
        assert a_amends[0].vote_number == 42

    def test_amendment_b1_rejected(self):
        amendments, _, _ = parse_steno_amendments(STENO_SIMPLE)
        b_amends = [a for a in amendments if a.letter == "B1"]
        assert len(b_amends) >= 1
        assert b_amends[0].result == "rejected"

    def test_stances_extracted(self):
        amendments, _, _ = parse_steno_amendments(STENO_SIMPLE)
        a_amend = next(a for a in amendments if a.letter == "A" and not a.is_final_vote)
        assert a_amend.committee_stance == "doporučující"
        assert a_amend.proposer_stance == "souhlas"

    def test_challenge_creates_revote(self):
        amendments, _, _ = parse_steno_amendments(STENO_CHALLENGE)
        revotes = [a for a in amendments if a.is_revote]
        assert len(revotes) >= 1
        assert revotes[0].vote_number == 51

    def test_withdrawal(self):
        amendments, _, _ = parse_steno_amendments(STENO_WITHDRAWAL)
        withdrawn = [a for a in amendments if a.is_withdrawn]
        assert len(withdrawn) >= 1
        assert withdrawn[0].letter == "D"

    def test_grouped_letters(self):
        amendments, _, _ = parse_steno_amendments(STENO_GROUPED)
        # Should find E1 as primary with F2 as grouped
        e1 = [a for a in amendments if a.letter == "E1"]
        assert len(e1) >= 1
        assert "F2" in e1[0].grouped_with

    def test_leg_tech_detected(self):
        amendments, _, _ = parse_steno_amendments(STENO_LEG_TECH)
        leg_tech = [a for a in amendments if a.is_leg_tech]
        assert len(leg_tech) >= 1

    def test_no_section_returns_empty(self):
        amendments, confidence, warns = parse_steno_amendments(STENO_NO_SECTION)
        assert amendments == []
        assert confidence < 1.0
        assert len(warns) > 0

    def test_empty_html(self):
        amendments, confidence, _warns = parse_steno_amendments("")
        assert amendments == []
        assert confidence < 1.0


# ── Submitter extraction tests ────────────────────────────────────────────────

# Pattern A (dominant): letter + genitive name
STENO_SUBMITTER = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh písmenem A pana poslance Bartoše.
Stanovisko výboru je doporučující. Předkladatel? (Souhlas.)</p>
<p>Hlasování číslo 90. Přijato.</p>
</body></html>
"""

# Pattern B: "předloženy" plural past tense + instrumental
STENO_SUBMITTER_FEMALE = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem B předloženy paní poslankyní Richterovou.
Stanovisko výboru je nedoporučující.</p>
<p>Hlasování číslo 91. Zamítnuto.</p>
</body></html>
"""

# Pattern A with academic title + genitive
STENO_SUBMITTER_TITLED = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh písmenem C pana poslance Mgr. Nováka.
Stanovisko výboru je doporučující.</p>
<p>Hlasování číslo 92. Přijato.</p>
</body></html>
"""


class TestSubmitterExtraction:
    def test_submitter_name_extracted(self):
        """Pattern A: letter + genitive name ('poslance Bartoše')."""
        amendments, _, _ = parse_steno_amendments(STENO_SUBMITTER)
        a_amend = next(a for a in amendments if a.letter == "A")
        assert a_amend.submitter_names == ["Bartoše"]

    def test_female_submitter(self):
        """Pattern B: 'předloženy paní poslankyní Richterovou'."""
        amendments, _, _ = parse_steno_amendments(STENO_SUBMITTER_FEMALE)
        b_amend = next(a for a in amendments if a.letter == "B")
        assert b_amend.submitter_names == ["Richterovou"]

    def test_titled_submitter(self):
        """Pattern A with title: 'poslance Mgr. Nováka'."""
        amendments, _, _ = parse_steno_amendments(STENO_SUBMITTER_TITLED)
        c_amend = next(a for a in amendments if a.letter == "C")
        assert "Nováka" in c_amend.submitter_names[0]

    def test_no_submitter_is_empty(self):
        amendments, _, _ = parse_steno_amendments(STENO_SIMPLE)
        # STENO_SIMPLE doesn't have submitter patterns
        a_amend = next(a for a in amendments if a.letter == "A")
        assert a_amend.submitter_names == []

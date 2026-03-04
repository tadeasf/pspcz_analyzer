"""Tests for stenographic record amendment parser."""

from pspcz_analyzer.services.amendments.steno_parser import (
    _blocks_to_amendments,
    _clean_html,
    _extract_section,
    _normalize_result,
    _parse_block,
    _parse_letter_groups,
    _ParseBlock,
    _split_into_blocks,
    cross_validate_amendments,
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
        assert block.committee_stance == "doporucujici"

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
        assert a_amend.committee_stance == "doporucujici"
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


# ── New pattern tests: parenthesized stances, Pattern C, paren vote ──────────

# Parenthesized proposer stance: "Stanovisko předkladatele? (Souhlasné.)"
STENO_PROPOSER_PAREN = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem A.
Stanovisko výboru je doporučující.
Stanovisko předkladatele? (Souhlasné.)</p>
<p>Hlasování číslo 10. Přijato.</p>
</body></html>
"""

# Proposer stance with role prefix: "(Ministr: Nesouhlas.)"
STENO_PROPOSER_MINISTER = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem B.
Stanovisko výboru je nedoporučující.
Stanovisko navrhovatele? (Ministr: Nesouhlas.)</p>
<p>Hlasování číslo 11. Zamítnuto.</p>
</body></html>
"""

# Parenthesized committee stance: "(Zpravodajka: Bez stanoviska.)"
STENO_COMMITTEE_PAREN = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem C.
Stanovisko výboru? (Zpravodajka: Bez stanoviska.)
Předkladatel? (Souhlas.)</p>
<p>Hlasování číslo 12. Přijato.</p>
</body></html>
"""

# Pattern C submitter: "návrh pana kolegy poslance Šafránkové"
STENO_SUBMITTER_PATTERN_C = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Budeme hlasovat o návrhu pana kolegy poslance Šafránkové
označeným písmenem D.</p>
<p>Hlasování číslo 13. Zamítnuto.</p>
</body></html>
"""

# First+last name: "návrh pana poslance Jana Kuchaře"
STENO_SUBMITTER_FULL_NAME = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Budeme hlasovat o pozměňovacím návrhu pana poslance Jana Kuchaře
označeným písmenem E.</p>
<p>Hlasování číslo 14. Přijato.</p>
</body></html>
"""

# Vote number with parenthesized format: "Hlasování (číslo 42)"
STENO_VOTE_PAREN = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem F.
Stanovisko výboru je doporučující.</p>
<p>Hlasování (číslo 42). Přijato.</p>
</body></html>
"""

# Standalone proposer stance without dialogue keyword: "(Souhlas.)"
STENO_PROPOSER_STANDALONE = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem G.
Stanovisko výboru je doporučující. (Souhlas.)</p>
<p>Hlasování číslo 15. Přijato.</p>
</body></html>
"""

# "Kladné" as proposer stance
STENO_PROPOSER_KLADNE = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem H.
Stanovisko výboru je doporučující.
Stanovisko předkladatele? (Kladné.)</p>
<p>Hlasování číslo 16. Přijato.</p>
</body></html>
"""


class TestParenthesizedProposerStance:
    def test_souhlasne_dialogue(self):
        """Stanovisko předkladatele? (Souhlasné.)"""
        amendments, _, _ = parse_steno_amendments(STENO_PROPOSER_PAREN)
        a = next(a for a in amendments if a.letter == "A")
        assert a.proposer_stance == "souhlas"

    def test_minister_nesouhlas(self):
        """(Ministr: Nesouhlas.)"""
        amendments, _, _ = parse_steno_amendments(STENO_PROPOSER_MINISTER)
        b = next(a for a in amendments if a.letter == "B")
        assert b.proposer_stance == "nesouhlas"

    def test_standalone_souhlas(self):
        """Standalone (Souhlas.) after committee stance."""
        amendments, _, _ = parse_steno_amendments(STENO_PROPOSER_STANDALONE)
        g = next(a for a in amendments if a.letter == "G")
        assert g.proposer_stance == "souhlas"

    def test_kladne_as_souhlas(self):
        """(Kladné.) should normalize to souhlas."""
        amendments, _, _ = parse_steno_amendments(STENO_PROPOSER_KLADNE)
        h = next(a for a in amendments if a.letter == "H")
        assert h.proposer_stance == "souhlas"


class TestParenthesizedCommitteeStance:
    def test_zpravodajka_bez_stanoviska(self):
        """(Zpravodajka: Bez stanoviska.)"""
        amendments, _, _ = parse_steno_amendments(STENO_COMMITTEE_PAREN)
        c = next(a for a in amendments if a.letter == "C")
        assert c.committee_stance == "bez_stanoviska"

    def test_committee_paren_with_proposer(self):
        """Both committee and proposer should be extracted from same block."""
        amendments, _, _ = parse_steno_amendments(STENO_COMMITTEE_PAREN)
        c = next(a for a in amendments if a.letter == "C")
        assert c.committee_stance == "bez_stanoviska"
        assert c.proposer_stance == "souhlas"


class TestSubmitterPatternC:
    def test_kolegy_poslance(self):
        """Pattern C: 'návrhu pana kolegy poslance Šafránkové'."""
        amendments, _, _ = parse_steno_amendments(STENO_SUBMITTER_PATTERN_C)
        d = next(a for a in amendments if a.letter == "D")
        assert "Šafránkové" in d.submitter_names[0]

    def test_first_last_name(self):
        """First+last name: 'poslance Jana Kuchaře'."""
        amendments, _, _ = parse_steno_amendments(STENO_SUBMITTER_FULL_NAME)
        e = next(a for a in amendments if a.letter == "E")
        assert "Kuchaře" in e.submitter_names[0]


class TestVoteParenFormat:
    def test_vote_number_in_parens(self):
        """Hlasování (číslo 42) should parse vote number 42."""
        amendments, _, _ = parse_steno_amendments(STENO_VOTE_PAREN)
        f = next(a for a in amendments if a.letter == "F")
        assert f.vote_number == 42
        assert f.result == "accepted"


# ── Letter fallback and bug fix tests ─────────────────────────────────────────

# Fallback letter extraction: "návrh A pan poslanec" (no "písmenem")
STENO_LETTER_FALLBACK = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Budeme hlasovat o pozměňovacím návrh A pan poslanec Nacher.
Stanovisko výboru je doporučující.</p>
<p>Hlasování číslo 10. Přijato.</p>
<p>Dalším je pozměňovací návrh B. Stanovisko výboru je nedoporučující.</p>
<p>Hlasování číslo 11. Zamítnuto.</p>
</body></html>
"""

# Scenario: blocks without letters should NOT inherit previous letter
STENO_NO_LETTER_INHERITANCE = """
<html><body>
<p>Přikročíme k hlasování o pozměňovacích návrzích.</p>
<p>Pozměňovací návrh označeným písmenem A.
Stanovisko výboru je doporučující.</p>
<p>Hlasování číslo 10. Přijato.</p>
<p>Nyní budeme hlasovat o dalším.
Stanovisko výboru je nedoporučující.</p>
<p>Hlasování číslo 11. Zamítnuto.</p>
<p>Pozměňovací návrh označeným písmenem C.
Stanovisko výboru je doporučující.</p>
<p>Hlasování číslo 12. Přijato.</p>
</body></html>
"""


class TestLetterFallbackRegex:
    def test_fallback_extracts_letter(self):
        """Fallback regex should catch 'návrh A pan poslanec'."""
        amendments, _, _ = parse_steno_amendments(STENO_LETTER_FALLBACK)
        letters = [a.letter for a in amendments if not a.is_final_vote]
        assert "A" in letters
        assert "B" in letters


class TestLetterInheritanceFix:
    def test_blocks_without_letter_get_empty(self):
        """Blocks without letters should get '' instead of inheriting."""
        amendments, _, _ = parse_steno_amendments(STENO_NO_LETTER_INHERITANCE)
        letters = [a.letter for a in amendments if not a.is_final_vote]
        assert letters[0] == "A"
        # Second vote has no letter — should be "" not "A"
        assert letters[1] == ""
        assert letters[2] == "C"

    def test_challenge_inherits_letter(self):
        """Challenge blocks should still inherit the previous letter."""
        amendments, _, _ = parse_steno_amendments(STENO_CHALLENGE)
        revotes = [a for a in amendments if a.is_revote]
        assert len(revotes) >= 1
        # Revote should inherit letter C from the challenged vote
        assert revotes[0].letter == "C"

    def test_blocks_to_amendments_empty_letter_on_non_challenge(self):
        """Direct unit test: _blocks_to_amendments with no letter and no challenge."""
        blocks = [
            _ParseBlock(text="first", letter="A", vote_number=1, result="accepted"),
            _ParseBlock(text="second", letter="", vote_number=2, result="rejected"),
        ]
        result = _blocks_to_amendments(blocks)
        assert result[0].letter == "A"
        assert result[1].letter == ""  # NOT "A"

    def test_blocks_to_amendments_challenge_inherits(self):
        """Direct unit test: challenge block inherits previous letter."""
        blocks = [
            _ParseBlock(text="first", letter="B", vote_number=1, result="accepted"),
            _ParseBlock(
                text="challenge", letter="", vote_number=2, result="rejected", is_challenge=True
            ),
        ]
        result = _blocks_to_amendments(blocks)
        assert result[0].letter == "B"
        assert result[1].letter == "B"
        assert result[1].is_revote is True


# ── Cross-validation tests ───────────────────────────────────────────────────


class TestCrossValidateAmendments:
    def test_fills_missing_letters_from_vote_titles(self):
        """Cross-validation should fill empty letters from vote nazev_dlouhy."""
        import polars as pl

        from pspcz_analyzer.models.amendment_models import AmendmentVote

        amendments = [
            AmendmentVote(letter="A", vote_number=10, result="accepted"),
            AmendmentVote(letter="", vote_number=11, result="rejected"),
        ]
        votes = pl.DataFrame(
            {
                "schuze": [5, 5],
                "cislo": [10, 11],
                "nazev_dlouhy": [
                    "pozm. navrh A posl. Nacher",
                    "pozm. navrh B posl. Vyborny",
                ],
            }
        )
        result, warnings = cross_validate_amendments(amendments, votes, 5, 1)
        letters = [a.letter for a in result]
        assert "A" in letters
        assert "B" in letters

    def test_detects_final_vote_from_title(self):
        """Cross-validation should detect final vote from 'jako celku'."""
        import polars as pl

        from pspcz_analyzer.models.amendment_models import AmendmentVote

        amendments = [
            AmendmentVote(letter="", vote_number=20, result="accepted"),
        ]
        votes = pl.DataFrame(
            {
                "schuze": [5],
                "cislo": [20],
                "nazev_dlouhy": ["navrhu zakona jako celku"],
            }
        )
        result, warnings = cross_validate_amendments(amendments, votes, 5, 1)
        assert result[0].is_final_vote is True

    def test_creates_missing_amendments(self):
        """Cross-validation should create amendments for unmatched vote titles."""
        import polars as pl

        from pspcz_analyzer.models.amendment_models import AmendmentVote

        amendments = [
            AmendmentVote(letter="A", vote_number=10, result="accepted"),
        ]
        votes = pl.DataFrame(
            {
                "schuze": [5, 5],
                "cislo": [10, 11],
                "nazev_dlouhy": [
                    "pozm. navrh A posl. Nacher",
                    "pozm. navrh C posl. Novak",
                ],
            }
        )
        result, warnings = cross_validate_amendments(amendments, votes, 5, 1)
        letters = [a.letter for a in result]
        assert "C" in letters
        assert any("C" in w for w in warnings)

"""Tests for amendment summarizer helpers and LLM parsers."""

from __future__ import annotations

from pathlib import Path

from pspcz_analyzer.models.amendment_models import AmendmentVote
from pspcz_analyzer.services.amendments.summarizer import _build_amendment_meta
from pspcz_analyzer.services.llm.parsers import (
    _format_amendments_list,
    _normalize_amendment_letter,
    _parse_amendment_summaries_json,
)
from pspcz_analyzer.services.tisk.io.extractor import extract_text_from_pdf


class TestBuildAmendmentMeta:
    """Tests for _build_amendment_meta helper."""

    def test_basic_fields(self) -> None:
        amend = AmendmentVote(
            letter="A",
            vote_number=1,
            pdf_submitter_names=["Novák Jan"],
            description="legislativně-technická oprava",
            amendment_text="Změna § 5 odst. 1",
        )
        meta = _build_amendment_meta(amend)
        assert meta["letter"] == "A"
        assert meta["submitter"] == "Novák Jan"
        assert meta["description"] == "legislativně-technická oprava"
        assert meta["amendment_text"] == "Změna § 5 odst. 1"
        assert "grouped_with" not in meta

    def test_grouped_with(self) -> None:
        amend = AmendmentVote(
            letter="D",
            vote_number=2,
            grouped_with=["C", "E"],
            amendment_text="some text",
        )
        meta = _build_amendment_meta(amend)
        assert meta["grouped_with"] == "C, E"

    def test_fallback_to_steno_submitter(self) -> None:
        amend = AmendmentVote(
            letter="B",
            vote_number=1,
            submitter_names=["Svoboda"],
        )
        meta = _build_amendment_meta(amend)
        assert meta["submitter"] == "Svoboda"

    def test_no_submitter(self) -> None:
        amend = AmendmentVote(letter="C", vote_number=1)
        meta = _build_amendment_meta(amend)
        assert meta["submitter"] == ""

    def test_empty_amendment_text(self) -> None:
        amend = AmendmentVote(letter="F", vote_number=1)
        meta = _build_amendment_meta(amend)
        assert meta["amendment_text"] == ""


class TestFormatAmendmentsList:
    """Tests for _format_amendments_list with per-amendment text."""

    def test_with_per_amendment_text(self) -> None:
        amendments = [
            {
                "letter": "A",
                "submitter": "Novák",
                "description": "oprava",
                "amendment_text": "Mění § 5 odst. 1 takto...",
            }
        ]
        result = _format_amendments_list(amendments)
        assert "[Amendment A]" in result
        assert "Submitter: Novák" in result
        assert "Description: oprava" in result
        assert "Text: Mění § 5 odst. 1 takto..." in result

    def test_with_grouped_with(self) -> None:
        amendments = [
            {
                "letter": "D",
                "submitter": "Ochodnicá",
                "description": "",
                "amendment_text": "some text",
                "grouped_with": "C, E",
            }
        ]
        result = _format_amendments_list(amendments)
        assert "Voted together with: C, E" in result

    def test_backward_compat_no_amendment_text(self) -> None:
        """Old-style dicts without amendment_text still work."""
        amendments = [
            {
                "letter": "B",
                "submitter": "Svoboda",
                "description": "technická",
            }
        ]
        result = _format_amendments_list(amendments)
        assert "[Amendment B]" in result
        assert "Submitter: Svoboda" in result
        assert "Text: (not available)" in result

    def test_text_truncation(self) -> None:
        long_text = "x" * 2000
        amendments = [
            {
                "letter": "A",
                "submitter": "",
                "description": "",
                "amendment_text": long_text,
            }
        ]
        result = _format_amendments_list(amendments)
        assert "..." in result
        # Should not contain the full 2000 chars
        assert len(result) < 2000

    def test_empty_list(self) -> None:
        assert _format_amendments_list([]) == ""

    def test_multiple_amendments(self) -> None:
        amendments = [
            {"letter": "A", "submitter": "X", "description": "", "amendment_text": "text A"},
            {"letter": "B", "submitter": "Y", "description": "", "amendment_text": "text B"},
        ]
        result = _format_amendments_list(amendments)
        assert "[Amendment A]" in result
        assert "[Amendment B]" in result
        assert "text A" in result
        assert "text B" in result


class TestNormalizeAmendmentLetter:
    """Tests for _normalize_amendment_letter."""

    def test_plain_letter(self) -> None:
        assert _normalize_amendment_letter("A") == "A"
        assert _normalize_amendment_letter("B1") == "B1"

    def test_markdown_header(self) -> None:
        assert _normalize_amendment_letter("### A") == "A"
        assert _normalize_amendment_letter("## B1") == "B1"

    def test_amendment_prefix(self) -> None:
        assert _normalize_amendment_letter("Amendment A") == "A"
        assert _normalize_amendment_letter("amendment B1") == "B1"

    def test_parenthesized_suffix(self) -> None:
        assert _normalize_amendment_letter("A (Novák)") == "A"
        assert _normalize_amendment_letter("B1 (Svoboda, Novák)") == "B1"

    def test_combined_noise(self) -> None:
        assert _normalize_amendment_letter("### Amendment A (Novák)") == "A"

    def test_lowercase(self) -> None:
        assert _normalize_amendment_letter("a") == "A"

    def test_empty(self) -> None:
        assert _normalize_amendment_letter("") == ""


class TestParseAmendmentSummariesJsonNormalization:
    """Tests that JSON parsing normalizes messy LLM letter output."""

    def test_clean_letters(self) -> None:
        data = {"amendments": [{"letter": "A", "summary": "changes X"}]}
        assert _parse_amendment_summaries_json(data) == {"A": "changes X"}

    def test_markdown_header_letters(self) -> None:
        data = {"amendments": [{"letter": "### A", "summary": "changes X"}]}
        assert _parse_amendment_summaries_json(data) == {"A": "changes X"}

    def test_amendment_prefix_letters(self) -> None:
        data = {"amendments": [{"letter": "Amendment B1", "summary": "changes Y"}]}
        assert _parse_amendment_summaries_json(data) == {"B1": "changes Y"}

    def test_parenthesized_letters(self) -> None:
        data = {"amendments": [{"letter": "A (Novák)", "summary": "changes Z"}]}
        assert _parse_amendment_summaries_json(data) == {"A": "changes Z"}


class TestExtractTextFromPdfHtmlFallback:
    """Tests for HTML fallback in extract_text_from_pdf."""

    def test_real_html_content(self, tmp_path: Path) -> None:
        html_content = b"""<!DOCTYPE html>
<html><head><title>Test</title></head>
<body>
<p>Amendment text paragraph one.</p>
<p>Amendment text paragraph two.</p>
</body></html>"""
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(html_content)
        result = extract_text_from_pdf(pdf_path)
        assert "Amendment text paragraph one" in result
        assert "Amendment text paragraph two" in result

    def test_html_with_tables(self, tmp_path: Path) -> None:
        html_content = b"""<html><body>
<table><tr><td>Cell 1</td><td>Cell 2</td></tr></table>
</body></html>"""
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(html_content)
        result = extract_text_from_pdf(pdf_path)
        assert "Cell 1" in result
        assert "Cell 2" in result

    def test_non_pdf_non_html_returns_empty(self, tmp_path: Path) -> None:
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"this is just plain text, not pdf or html")
        result = extract_text_from_pdf(pdf_path)
        assert result == ""

    def test_nonexistent_file_returns_empty(self, tmp_path: Path) -> None:
        pdf_path = tmp_path / "nonexistent.pdf"
        result = extract_text_from_pdf(pdf_path)
        assert result == ""

    def test_windows_1250_html(self, tmp_path: Path) -> None:
        """HTML encoded in Windows-1250 (common for psp.cz)."""
        html_content = b"<html><body><p>\xc8esk\xe1 republika</p></body></html>"
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(html_content)
        result = extract_text_from_pdf(pdf_path)
        # Should extract something (even with encoding fallback)
        assert "republika" in result

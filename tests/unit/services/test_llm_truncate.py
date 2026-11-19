"""Tests for truncate_legislative_text."""

from unittest.mock import patch

from pspcz_analyzer.services.llm import helpers


def test_shortener_off_still_caps_at_context_limit() -> None:
    """Full-text mode must not send a text longer than max_chars (HTTP 400 on overflow)."""
    with patch.object(helpers, "TISK_SHORTENER", False):
        assert helpers.truncate_legislative_text("x" * 50, max_chars=20) == "x" * 20
        assert helpers.truncate_legislative_text("short", max_chars=20) == "short"

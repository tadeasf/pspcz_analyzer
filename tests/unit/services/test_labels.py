"""Tests for the shared vote label tokens."""

from pspcz_analyzer.services.labels import mp_vote_label, vote_outcome_label


class TestMpVoteLabel:
    def test_known_codes(self) -> None:
        assert mp_vote_label("A") == "YES"
        assert mp_vote_label("B") == "NO"
        assert mp_vote_label("C") == "ABSTAINED"
        assert mp_vote_label("F") == "DID_NOT_VOTE"
        assert mp_vote_label("@") == "Absent"
        assert mp_vote_label("M") == "Excused"

    def test_unknown_code_returns_raw(self) -> None:
        assert mp_vote_label("X") == "X"

    def test_empty_code_returns_placeholder(self) -> None:
        assert mp_vote_label("") == "?"

    def test_tokens_match_vote_detail_template(self) -> None:
        """vote_detail.html compares exactly these tokens for CSS classes."""
        expected = {
            "A": "YES",
            "B": "NO",
            "C": "ABSTAINED",
            "@": "Absent",
            "M": "Excused",
        }
        for code, token in expected.items():
            assert mp_vote_label(code) == token


class TestVoteOutcomeLabel:
    def test_known_outcomes(self) -> None:
        assert vote_outcome_label("A") == "passed"
        assert vote_outcome_label("R") == "rejected"
        assert vote_outcome_label("Z") == "void"

    def test_unknown_outcome_returns_raw(self) -> None:
        assert vote_outcome_label("P") == "P"
        assert vote_outcome_label("") == ""

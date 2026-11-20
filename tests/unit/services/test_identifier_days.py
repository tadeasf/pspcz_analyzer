"""Tests for 3rd-reading day matching in the amendment identifier (#93)."""

from datetime import date

from pspcz_analyzer.services.amendments.identifier import _parse_day, _third_reading_match


def test_parse_both_date_formats() -> None:
    assert _parse_day("23. 9. 2026") == date(2026, 9, 23)
    assert _parse_day("23.09.2026") == date(2026, 9, 23)
    assert _parse_day("") is None


def test_bod_without_vote_on_third_reading_day_is_not_a_candidate() -> None:
    readings: list[tuple[int | None, date | None]] = [(31, date(2026, 9, 23))]
    assert _third_reading_match(readings, 13, ["25.03.2026"]) is None
    assert _third_reading_match(readings, 13, ["25.03.2026", "23.09.2026"]) == "day"


def test_sitting_number_wins_over_a_wrong_history_date() -> None:
    """psp.cz says tisk 56 had its 3rd reading on 23. 8. at sitting 31 (it was 23. 9.)."""
    assert _third_reading_match([(31, date(2026, 8, 23))], 31, ["23.09.2026"]) == "sitting"


def test_undated_third_reading_keeps_old_behaviour() -> None:
    assert _third_reading_match([(None, None)], 13, ["14.07.2026"]) == "undated"

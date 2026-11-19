"""Tests for the hand-kept government table and per-vote coalition (#101)."""

from datetime import date

import polars as pl

from pspcz_analyzer.services.coalition_table import coalition_spans, parties_at
from tests.fixtures.sample_data import make_period_data


def test_period_9_pirates_leave_government() -> None:
    spans = coalition_spans(9)
    assert "Piráti" in parties_at(spans, date(2023, 5, 1))
    assert "Piráti" not in parties_at(spans, date(2024, 10, 1))
    assert parties_at(spans, date(2021, 11, 1)) == frozenset({"ANO"})
    assert all(s.source for s in spans)


def test_spans_are_contiguous() -> None:
    spans = coalition_spans(9)
    for prev, nxt in zip(spans, spans[1:], strict=False):
        assert prev.end is not None and (nxt.start - prev.end).days == 1


def test_coalition_for_vote_uses_vote_date() -> None:
    data = make_period_data()
    data.votes = data.votes.with_columns(pl.lit("01.05.2023").alias("datum"))
    data.coalition_spans = coalition_spans(9)
    data.coalition_parties = coalition_spans(9)[-1].parties
    vote_id = data.votes.get_column("id_hlasovani")[0]
    assert "Piráti" in data.coalition_for_vote(vote_id)
    assert data.coalition_for_vote(None) == data.coalition_parties

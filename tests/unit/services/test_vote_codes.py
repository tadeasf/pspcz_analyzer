"""Tests for vote-code categories, excuses and the summary check (#91)."""

import polars as pl
import pytest

from pspcz_analyzer.services.vote_codes import (
    CATEGORY_KEYS,
    category_count_exprs,
    check_vote_totals,
    normalize_vote_codes,
    vote_category,
)


@pytest.mark.parametrize(
    ("code", "category"),
    [
        ("A", "yes"),
        ("B", "no"),
        ("N", "no"),
        ("C", "abstained"),
        ("F", "abstained"),
        ("K", "abstained"),
        ("@", "absent"),
        ("W", "absent"),
        ("M", "excused"),
        ("X", "other"),
        ("", "other"),
    ],
)
def test_vote_category(code: str, category: str) -> None:
    assert vote_category(code) == category


def test_category_counts_sum_to_total() -> None:
    df = pl.DataFrame({"vysledek": ["A", "B", "K", "K", "@", "M", "W", "X"]})
    row = df.select(*category_count_exprs()).row(0, named=True)
    assert row == {"yes": 1, "no": 1, "abstained": 2, "absent": 2, "excused": 1, "other": 1}
    assert sum(row[k] for k in CATEGORY_KEYS) == df.height


def _votes() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_hlasovani": [1, 2],
            "datum": ["23.10.2026", "24.10.2026"],
            "cas": ["15:00", "04:39"],
            "pro": [1, 0],
            "proti": [0, 1],
            "zdrzel": [0, 0],
            "nehlasoval": [1, 0],
            "prihlaseno": [2, 1],
        },
        schema_overrides=dict.fromkeys(("pro", "proti", "zdrzel", "nehlasoval", "prihlaseno"), pl.Int32),
    )


def test_normalize_excuses_only_absent_in_window() -> None:
    mp_votes = pl.DataFrame(
        {
            "id_poslanec": [10, 11, 12, 10, 11, 12],
            "id_hlasovani": [1, 1, 1, 2, 2, 2],
            "vysledek": ["@", "A", "K", "@", "N", "@"],
        }
    )
    excuses = pl.DataFrame(
        {
            # MP 10: excused afternoon of the 23rd and the night continuation of the 24th.
            # MP 11: excused but logged in, so the vote wins.
            # MP 12: excused on the 24th only after 09:00, so the 04:39 vote stays absent.
            "id_poslanec": [10, 10, 11, 12],
            "den": ["23.10.2026", "24.10.2026", "23.10.2026", "24.10.2026"],
            "od": ["14:00", "00:00", None, "09:00"],
            "do": ["23:59", "09:00", None, "23:59"],
        }
    )
    out = normalize_vote_codes(mp_votes, _votes(), excuses)
    assert out.get_column("vysledek").to_list() == ["M", "A", "K", "M", "B", "@"]


def test_check_vote_totals_flags_mismatch() -> None:
    mp_votes = pl.DataFrame(
        {
            "id_poslanec": [10, 11, 10, 11],
            "id_hlasovani": [1, 1, 2, 2],
            "vysledek": ["A", "K", "A", "@"],  # vote 2: summary says one NO
        }
    )
    assert check_vote_totals(10, _votes(), mp_votes) == 1

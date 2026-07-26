"""Tests for the shared party majority direction helper."""

import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.services.party_voting import party_majority_direction


def _votes(rows: list[tuple[int, str, str]]) -> pl.DataFrame:
    """Build a (id_hlasovani, party, vysledek) frame from tuples."""
    return pl.DataFrame(
        {
            "id_hlasovani": [r[0] for r in rows],
            "party": [r[1] for r in rows],
            "vysledek": [r[2] for r in rows],
        }
    )


class TestPartyMajorityDirection:
    def test_yes_majority(self) -> None:
        df = _votes([(1, "ANO", "A"), (1, "ANO", "A"), (1, "ANO", "B")])
        out = party_majority_direction(df)
        assert out.height == 1
        assert out.row(0, named=True)["party_direction"] == VoteResult.YES

    def test_no_majority(self) -> None:
        df = _votes([(1, "ODS", "B"), (1, "ODS", "B"), (1, "ODS", "A")])
        out = party_majority_direction(df)
        assert out.row(0, named=True)["party_direction"] == VoteResult.NO

    def test_tie_is_dropped(self) -> None:
        df = _votes([(1, "ANO", "A"), (1, "ANO", "B"), (2, "ODS", "A")])
        out = party_majority_direction(df)
        assert out.get_column("party").to_list() == ["ODS"]

    def test_counts_preserved(self) -> None:
        df = _votes([(1, "ANO", "A"), (1, "ANO", "A"), (1, "ANO", "B")])
        row = party_majority_direction(df).row(0, named=True)
        assert row["yes_count"] == 2
        assert row["no_count"] == 1

    def test_custom_alias_and_keys(self) -> None:
        df = pl.DataFrame({"schuze": [5, 5, 5], "vysledek": ["B", "B", "A"]})
        out = party_majority_direction(df, group_keys=["schuze"], direction_alias="direction")
        assert "direction" in out.columns
        assert out.row(0, named=True)["direction"] == VoteResult.NO

    def test_direction_values_are_vote_result_codes(self) -> None:
        """loyalty_table.html compares party_direction against 'A'/'B'."""
        df = _votes([(1, "ANO", "A"), (1, "ODS", "B")])
        out = party_majority_direction(df)
        directions = dict(
            zip(
                out.get_column("party").to_list(),
                out.get_column("party_direction").to_list(),
                strict=True,
            )
        )
        assert directions["ANO"] == "A"
        assert directions["ODS"] == "B"

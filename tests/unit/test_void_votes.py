"""Void votes come from zmatecne.unl, never from hl{year}z.unl (#92)."""

from pathlib import Path

import polars as pl

from pspcz_analyzer.services.data_reader import DataReader

_VOTES = pl.DataFrame({"id_hlasovani": pl.Series([10, 11, 12], dtype=pl.Int64)})


def test_loads_zmatecne_restricted_to_period(tmp_path: Path) -> None:
    voting_dir = tmp_path / "hl-2017ps"
    voting_dir.mkdir()
    (voting_dir / "zmatecne.unl").write_bytes(b"5|\n11|\n99|\n")  # all periods
    (voting_dir / "hl2017z.unl").write_bytes(b"10|\n12|\n")  # different table: valid votes

    void = DataReader(cache_dir=tmp_path)._load_void_votes(8, voting_dir, _VOTES)

    assert void.get_column("id_hlasovani").to_list() == [11]


def test_missing_file_gives_empty_frame(tmp_path: Path) -> None:
    void = DataReader(cache_dir=tmp_path)._load_void_votes(8, tmp_path, _VOTES)

    assert void.height == 0
    assert void.schema["id_hlasovani"] == pl.Int64


def test_watcher_reloads_period_when_excuses_change(tmp_path) -> None:
    """A new omluvy_{period}.parquet (daily refresh) reloads that period in the frontend."""
    reader = DataReader(cache_dir=tmp_path)
    reader._periods[10] = object()  # type: ignore[assignment]
    assert reader._identify_changed_periods({"omluvy_10.parquet"}) == [10]

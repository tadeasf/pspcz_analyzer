"""#85: every period-10 vote links to the tisk of its approved agenda item.

Reads the locally cached open-data parquet (no network) and recomputes the
expected tisk per vote independently: vote (schuze, bod) -> the approved
(pozvanka null) schuze row -> its approved bod_schuze row -> tisky.ct.
"""

from pathlib import Path

import polars as pl
import pytest

from pspcz_analyzer.config import DEFAULT_CACHE_DIR, PERIOD_ORGAN_IDS
from pspcz_analyzer.services.tisk.lookup_builder import UNSCHEDULED_SCHUZE, build_tisk_lookup
from pspcz_analyzer.services.tisk.text_service import TiskTextService

PERIOD = 10
_PARQUET = DEFAULT_CACHE_DIR / "parquet"
_TABLES = ("schuze", "bod_schuze", "tisky_v2", f"hl_hlasovani_{PERIOD}")


def _read(name: str) -> pl.DataFrame:
    """Read one cached parquet table, skipping the test when it is missing."""
    path: Path = _PARQUET / f"{name}.parquet"
    if not path.exists():
        pytest.skip(f"cached open data missing: {path}")
    return pl.read_parquet(path)


def _expected_ct(
    schuze: pl.DataFrame, bod_schuze: pl.DataFrame, tisky: pl.DataFrame
) -> dict[tuple[int, int], int]:
    """Map (schuze, bod) -> ct from the approved agenda, row by row in plain Python."""
    organ_id = PERIOD_ORGAN_IDS[PERIOD]
    session_num = {
        r["id_schuze"]: r["schuze"]
        for r in schuze.iter_rows(named=True)
        if r["id_org"] == organ_id and r["pozvanka"] is None
    }
    ct_by_tisk = {r["id_tisk"]: r["ct"] for r in tisky.iter_rows(named=True) if r["ct"]}
    expected: dict[tuple[int, int], int] = {}
    for r in bod_schuze.iter_rows(named=True):
        num = session_num.get(r["id_schuze"])
        if num is None or r["pozvanka"] is not None or r["bod"] is None:
            continue
        ct = ct_by_tisk.get(r["id_tisk"])
        if ct is not None:
            expected[(num, r["bod"])] = ct
    return expected


@pytest.mark.integration
def test_every_vote_links_to_the_approved_agenda_tisk() -> None:
    schuze, bod_schuze, tisky, votes = (_read(name) for name in _TABLES)
    lookup = build_tisk_lookup(
        PERIOD, votes, schuze, bod_schuze, tisky, TiskTextService(DEFAULT_CACHE_DIR), {}, {}
    )
    expected = _expected_ct(schuze, bod_schuze, tisky)

    diffs = []
    for vote in votes.iter_rows(named=True):
        key = (vote["schuze"], vote["bod"])
        assert key[0] != UNSCHEDULED_SCHUZE
        got = lookup.get(key)
        got_ct = got.ct if got else None
        if got_ct != expected.get(key):
            diffs.append((vote["id_hlasovani"], key, got_ct, expected.get(key)))

    assert votes.height > 0
    # The issue's example: vote 88876 (34th sitting) is about tisk 231, not 269
    example = votes.filter(pl.col("id_hlasovani") == 88876)
    if example.height:
        info = lookup[(example["schuze"][0], example["bod"][0])]
        assert info.ct == 231
    assert diffs == [], f"{len(diffs)} votes linked to the wrong tisk, first: {diffs[:10]}"

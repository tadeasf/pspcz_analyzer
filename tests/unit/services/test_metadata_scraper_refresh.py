"""Tests for the daily-refresh re-scrape gate in the tisk metadata scraper."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pspcz_analyzer.config import TISKY_HISTORIE_DIR, TISKY_META_DIR
from pspcz_analyzer.services.tisk import metadata_scraper
from pspcz_analyzer.services.tisk.io.history_scraper import (
    HISTORY_PARSER_VERSION,
    TiskHistory,
    TiskHistoryStage,
    history_from_dict,
    load_history_json,
    save_history_json,
)

PERIOD = 10


def _hist_path(cache_dir: Path, ct: int) -> Path:
    return cache_dir / TISKY_META_DIR / str(PERIOD) / TISKY_HISTORIE_DIR / f"{ct}.json"


def _cache_history(cache_dir: Path, ct: int, status: str) -> None:
    """Write a cached history with amendment data present (no migration re-scrape)."""
    hist = TiskHistory(
        ct=ct,
        period=PERIOD,
        current_status=status,
        stages=[TiskHistoryStage(stage_type="3_cteni", label="3. čtení")],
        amendment_tisk_ct1=5,  # non-None: migration path will not trigger
    )
    save_history_json(hist, _hist_path(cache_dir, ct))


@pytest.fixture
def cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Cache dir with one active (ct=1) and one terminal (ct=2) cached bill."""
    monkeypatch.setattr(metadata_scraper.time, "sleep", lambda _s: None)
    _cache_history(tmp_path, 1, "projednáváno")
    _cache_history(tmp_path, 2, "vyhlášeno")
    return tmp_path


def _patch_scraper(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Patch scrape_tisk_history to record re-scrape calls; returns the call log."""
    calls: list[int] = []

    def _fake(period: int, ct: int) -> TiskHistory:
        calls.append(ct)
        return TiskHistory(
            ct=ct,
            period=period,
            current_status="schváleno sněmovnou",
            scraped_at=datetime.now(UTC).isoformat(),
            amendment_tisk_ct1=5,
        )

    monkeypatch.setattr(metadata_scraper, "scrape_tisk_history", _fake)
    return calls


def test_force_active_rescrapes_only_active(cache_dir: Path, monkeypatch: pytest.MonkeyPatch):
    calls = _patch_scraper(monkeypatch)
    result = metadata_scraper.scrape_histories_sync(PERIOD, [1, 2], cache_dir, force_active=True)
    # Active bill re-scraped, terminal bill served from cache
    assert calls == [1]
    assert set(result.keys()) == {1, 2}
    assert result[1].current_status == "schváleno sněmovnou"
    assert result[2].current_status == "vyhlášeno"


def test_no_force_serves_from_cache(cache_dir: Path, monkeypatch: pytest.MonkeyPatch):
    calls = _patch_scraper(monkeypatch)
    result = metadata_scraper.scrape_histories_sync(PERIOD, [1, 2], cache_dir, force_active=False)
    # Nothing re-scraped — both come from cache unchanged
    assert calls == []
    assert result[1].current_status == "projednáváno"
    assert result[2].current_status == "vyhlášeno"


def test_old_parser_version_rescrapes_even_terminal(
    cache_dir: Path, monkeypatch: pytest.MonkeyPatch
):
    # A history written before versioning (no parser_version key) is stale,
    # even with a terminal status and without force_active.
    path = _hist_path(cache_dir, 2)
    data = json.loads(path.read_text(encoding="utf-8"))
    del data["parser_version"]
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    assert history_from_dict(data).parser_version == 0

    calls = _patch_scraper(monkeypatch)
    result = metadata_scraper.scrape_histories_sync(PERIOD, [1, 2], cache_dir, force_active=False)
    assert calls == [2]
    assert result[2].current_status == "schváleno sněmovnou"
    reloaded = load_history_json(path)
    assert reloaded is not None
    assert reloaded.parser_version == HISTORY_PARSER_VERSION

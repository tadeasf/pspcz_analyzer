"""Scrape legislative history and law changes from psp.cz."""

import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import (
    PSP_REQUEST_DELAY,
    TERMINAL_TISK_STATUSES,
    TISKY_HISTORIE_DIR,
    TISKY_LAW_CHANGES_DIR,
    TISKY_META_DIR,
)
from pspcz_analyzer.services.tisk.io import (
    TiskHistory,
    load_history_json,
    load_law_changes_json,
    save_history_json,
    save_law_changes_json,
    scrape_proposed_law_changes,
    scrape_tisk_history,
)


def scrape_histories_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
    cancel_check: Callable[[], None] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    force_active: bool = False,
) -> dict:
    """Scrape legislative history pages for all tisky in a period.

    Caches results as JSON files. Skips already-cached tisky.

    Args:
        period: Electoral period number.
        ct_numbers: Tisk numbers to scrape.
        cache_dir: Base cache directory.
        cancel_check: Optional cancellation hook raised between tisky.
        progress_callback: Optional (done, total) progress hook.
        force_active: When True, re-scrape any cached bill whose status is not
            terminal (see TERMINAL_TISK_STATUSES) so daily refresh picks up
            legislative step changes. Terminal bills are still served from cache.

    Returns:
        {ct: TiskHistory} dict.
    """
    hist_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
    hist_dir.mkdir(parents=True, exist_ok=True)

    histories: dict[int, TiskHistory] = {}
    total = len(ct_numbers)
    scraped = 0

    for i, ct in enumerate(ct_numbers, 1):
        if cancel_check:
            cancel_check()
        json_path = hist_dir / f"{ct}.json"

        # Load from cache if available
        if json_path.exists():
            h = load_history_json(json_path)
            if h:
                # Refresh active (non-terminal) bills so status changes show up
                needs_refresh = force_active and h.current_status not in TERMINAL_TISK_STATUSES
                # One-time migration: history predates amendment sub-tisk scraping
                needs_migration = h.amendment_tisk_ct1 is None and bool(h.stages)
                if needs_refresh or needs_migration:
                    h_fresh = scrape_tisk_history(period, ct)
                    if h_fresh and (needs_refresh or h_fresh.amendment_tisk_ct1 is not None):
                        save_history_json(h_fresh, json_path)
                        h = h_fresh
                        scraped += 1
                        time.sleep(PSP_REQUEST_DELAY)
                histories[ct] = h
            if progress_callback:
                progress_callback(i, total)
            continue

        # Scrape from psp.cz
        if i % 50 == 0 or i == 1:
            logger.info(
                "[tisk pipeline] Scraping history for period {}: {}/{}",
                period,
                i,
                total,
            )

        h = scrape_tisk_history(period, ct)
        if h:
            save_history_json(h, json_path)
            histories[ct] = h
            scraped += 1

        time.sleep(PSP_REQUEST_DELAY)
        if progress_callback:
            progress_callback(i, total)

    logger.info(
        "[tisk pipeline] History scraping for period {}: {} cached, {} new, {} total",
        period,
        len(histories) - scraped,
        scraped,
        len(histories),
    )
    return histories


def scrape_law_changes_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
    cancel_check: Callable[[], None] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    force_active: bool = False,
    active_cts: set[int] | None = None,
) -> dict[int, list[dict]]:
    """Scrape law change pages (snzp=1) for all tisky in a period.

    Caches results as JSON.

    Args:
        period: Electoral period number.
        ct_numbers: Tisk numbers to scrape.
        cache_dir: Base cache directory.
        cancel_check: Optional cancellation hook raised between tisky.
        progress_callback: Optional (done, total) progress hook.
        force_active: When True, re-scrape cached law changes for cts in
            *active_cts* so daily refresh picks up newly enacted law changes.
        active_cts: Tisk numbers whose bills are still active (non-terminal).

    Returns:
        {ct: [law_change_dicts]} dict.
    """
    law_changes_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_LAW_CHANGES_DIR
    law_changes_dir.mkdir(parents=True, exist_ok=True)

    result: dict[int, list[dict]] = {}
    total = len(ct_numbers)
    scraped = 0

    for i, ct in enumerate(ct_numbers, 1):
        if cancel_check:
            cancel_check()
        # Load from cache (unless this is an active bill being force-refreshed)
        force = force_active and active_cts is not None and ct in active_cts
        cached = load_law_changes_json(period, ct, cache_dir)
        if cached is not None and not force:
            result[ct] = [asdict(c) for c in cached]
            if progress_callback:
                progress_callback(i, total)
            continue

        if i % 50 == 0 or i == 1:
            logger.info(
                "[tisk pipeline] Scraping law changes for period {}: {}/{}",
                period,
                i,
                total,
            )

        changes = scrape_proposed_law_changes(period, ct)
        save_law_changes_json(changes, period, ct, cache_dir)
        if changes:
            result[ct] = [asdict(c) for c in changes]
        scraped += 1

        time.sleep(PSP_REQUEST_DELAY)
        if progress_callback:
            progress_callback(i, total)

    logger.info(
        "[tisk pipeline] Law changes for period {}: {} cached, {} new, {} with changes",
        period,
        len(result) - scraped,
        scraped,
        len(result),
    )
    return result

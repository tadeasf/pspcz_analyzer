"""Scrape legislative history and law changes from psp.cz."""

import time
from dataclasses import asdict
from pathlib import Path

from loguru import logger

from pspcz_analyzer.config import (
    PSP_REQUEST_DELAY,
    TISKY_HISTORIE_DIR,
    TISKY_LAW_CHANGES_DIR,
    TISKY_META_DIR,
)
from pspcz_analyzer.data.history_scraper import (
    TiskHistory,
    load_history_json,
    save_history_json,
    scrape_tisk_history,
)
from pspcz_analyzer.data.law_changes_scraper import (
    load_law_changes_json,
    save_law_changes_json,
    scrape_proposed_law_changes,
)


def scrape_histories_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
) -> dict:
    """Scrape legislative history pages for all tisky in a period.

    Caches results as JSON files. Skips already-cached tisky.
    Returns {ct: TiskHistory} dict.
    """
    hist_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
    hist_dir.mkdir(parents=True, exist_ok=True)

    histories: dict[int, TiskHistory] = {}
    total = len(ct_numbers)
    scraped = 0

    for i, ct in enumerate(ct_numbers, 1):
        json_path = hist_dir / f"{ct}.json"

        # Load from cache if available
        if json_path.exists():
            h = load_history_json(json_path)
            if h:
                histories[ct] = h
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
) -> dict[int, list[dict]]:
    """Scrape law change pages (snzp=1) for all tisky in a period.

    Caches results as JSON. Returns {ct: [law_change_dicts]}.
    """
    law_changes_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_LAW_CHANGES_DIR
    law_changes_dir.mkdir(parents=True, exist_ok=True)

    result: dict[int, list[dict]] = {}
    total = len(ct_numbers)
    scraped = 0

    for i, ct in enumerate(ct_numbers, 1):
        # Load from cache
        cached = load_law_changes_json(period, ct, cache_dir)
        if cached is not None:
            result[ct] = [asdict(c) for c in cached]
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

    logger.info(
        "[tisk pipeline] Law changes for period {}: {} cached, {} new, {} with changes",
        period,
        len(result) - scraped,
        scraped,
        len(result),
    )
    return result

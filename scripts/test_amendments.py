"""Standalone test script for the amendment pipeline on period 9.

Period 9 (2021-2025) has full data with many third readings, making it
ideal for testing.  Running via the full app is slow (the tisk pipeline
processes all stages sequentially), so this script loads only what the
amendment pipeline actually needs:

  1. Load period data (voting records + tisk lookup)
  2. Scrape legislative histories (cached after first run)
  3. Refresh tisk_lookup with history data
  4. Identify third-reading candidates
  5. Run the amendment pipeline

First run takes ~17 min (1011 history scrapes @ 1s each).
Subsequent runs are near-instant (everything cached).

Usage:
    uv run python scripts/test_amendments.py [--period N]
"""

# ruff: noqa: E402
import sys
import time
from pathlib import Path

# Ensure the project root is on sys.path when running as a standalone script
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import argparse

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    PERIOD_ORGAN_IDS,
    PERIOD_YEARS,
    TISKY_HISTORIE_DIR,
    TISKY_META_DIR,
)
from pspcz_analyzer.services.amendments.pipeline import (
    AmendmentProgress,
    _identify_third_reading_bods,
    _run_pipeline_sync,
)
from pspcz_analyzer.services.data_service import DataService
from pspcz_analyzer.services.tisk.io import load_history_json
from pspcz_analyzer.services.tisk.metadata_scraper import scrape_histories_sync


def _inject_histories(ds: DataService, period: int) -> int:
    """Scrape histories and inject them into tisk_lookup entries.

    Returns:
        Number of tisky with history data.
    """
    # Gather ct numbers from the tisky table
    assert ds._tisky is not None  # noqa: SLF001
    organ_id = PERIOD_ORGAN_IDS[period]
    period_tisky = ds._tisky.filter(  # noqa: SLF001
        (pl.col("id_obdobi") == organ_id) & pl.col("ct").is_not_null()
    )
    ct_numbers = sorted(period_tisky.get_column("ct").unique().to_list())
    logger.info("Found {} tisky for period {}", len(ct_numbers), period)

    # Scrape (cached after first run)
    t0 = time.perf_counter()
    histories = scrape_histories_sync(period, ct_numbers, DEFAULT_CACHE_DIR)
    elapsed = time.perf_counter() - t0
    logger.info("History scraping took {:.1f}s — {} histories loaded", elapsed, len(histories))

    # Also load any already-cached histories from disk for completeness
    hist_dir = DEFAULT_CACHE_DIR / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
    if hist_dir.exists():
        for path in hist_dir.glob("*.json"):
            ct = int(path.stem)
            if ct not in histories:
                h = load_history_json(path)
                if h:
                    histories[ct] = h

    # Inject into tisk_lookup
    pd = ds._periods[period]  # noqa: SLF001
    injected = 0
    for tisk in pd.tisk_lookup.values():
        h = histories.get(tisk.ct)
        if h:
            tisk.history = h
            injected += 1

    logger.info("Injected history into {}/{} tisk_lookup entries", injected, len(pd.tisk_lookup))
    return injected


def _run_test(period: int) -> None:
    """Run the amendment pipeline test for the given period."""
    logger.info("=== Amendment pipeline test — period {} ({}) ===", period, PERIOD_YEARS[period])

    # Step 1: Load data
    logger.info("Step 1/4: Loading period data...")
    ds = DataService(cache_dir=DEFAULT_CACHE_DIR)
    ds._load_period(period)  # noqa: SLF001
    pd = ds._periods[period]  # noqa: SLF001
    logger.info(
        "Loaded: {} votes, {} tisk_lookup entries",
        pd.votes.height,
        len(pd.tisk_lookup),
    )

    # Step 2: Scrape and inject histories
    logger.info("Step 2/4: Scraping legislative histories...")
    _inject_histories(ds, period)

    # Step 3: Identify candidates
    logger.info("Step 3/4: Identifying third-reading candidates...")
    candidates = _identify_third_reading_bods(pd)
    if not candidates:
        logger.warning("No third-reading candidates found for period {}!", period)
        sys.exit(1)

    logger.info("Found {} candidate third-reading bills:", len(candidates))
    for schuze, bod, ct, nazev in candidates[:10]:
        logger.info("  schuze={} bod={} ct={} — {}", schuze, bod, ct, nazev)
    if len(candidates) > 10:
        logger.info("  ... and {} more", len(candidates) - 10)

    # Step 4: Run full pipeline
    logger.info("Step 4/4: Running amendment pipeline...")
    progress = AmendmentProgress()
    t0 = time.perf_counter()
    bills = _run_pipeline_sync(period, pd, DEFAULT_CACHE_DIR, progress)
    elapsed = time.perf_counter() - t0

    # Summary
    logger.info("=== Results ===")
    logger.info("Pipeline completed in {:.1f}s", elapsed)
    logger.info("Bills found: {}", progress.bills_found)
    logger.info("Bills parsed: {}", progress.bills_parsed)
    logger.info("Total amendments: {}", sum(b.amendment_count for b in bills))

    for bill in bills[:5]:
        logger.info(
            "  ct={} schuze={} bod={} — {} amendments (confidence: {:.0%})",
            bill.ct,
            bill.schuze,
            bill.bod,
            bill.amendment_count,
            bill.parse_confidence,
        )
        if bill.parse_warnings:
            for w in bill.parse_warnings:
                logger.info("    ⚠ {}", w)
    if len(bills) > 5:
        logger.info("  ... and {} more bills", len(bills) - 5)


def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(description="Test amendment pipeline for a period")
    parser.add_argument(
        "--period",
        type=int,
        default=9,
        help="Electoral period to test (default: 9)",
    )
    args = parser.parse_args()

    if args.period not in PERIOD_YEARS:
        logger.error("Unknown period {}. Available: {}", args.period, list(PERIOD_YEARS.keys()))
        sys.exit(1)

    _run_test(args.period)


if __name__ == "__main__":
    main()

"""Amendment candidate identification.

Functions for ensuring tisk history data exists and identifying
third-reading agenda items that likely had amendment votes.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import TISKY_HISTORIE_DIR, TISKY_META_DIR
from pspcz_analyzer.models.amendment_models import BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.tisk.cache_manager import TiskCacheManager
from pspcz_analyzer.services.tisk.metadata_scraper import scrape_histories_sync


def _ensure_tisk_histories(
    period: int,
    period_data: PeriodData,
    cache_dir: Path,
) -> None:
    """Ensure tisk history data exists and is loaded into memory.

    The amendment candidate identification requires tisk.history.stages
    to detect third-reading bills. If the tisk pipeline hasn't scraped
    histories yet, do it now as a prerequisite. Always loads the cached
    histories into in-memory TiskInfo objects.

    Args:
        period: Electoral period number.
        period_data: Loaded period data with tisk_lookup.
        cache_dir: Base cache directory.
    """
    hist_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_HISTORIE_DIR
    needs_scrape = not hist_dir.exists() or not any(hist_dir.glob("*.json"))

    if needs_scrape:
        ct_numbers = sorted({t.ct for t in period_data.tisk_lookup.values() if t.ct})
        if not ct_numbers:
            return

        logger.info(
            "[amendment pipeline] No tisk history data found for period {}, "
            "scraping {} tisk histories as prerequisite...",
            period,
            len(ct_numbers),
        )
        scrape_histories_sync(period, ct_numbers, cache_dir)

    # Always load histories into in-memory TiskInfo objects
    cache_mgr = TiskCacheManager(cache_dir)
    cache_mgr.invalidate(period)
    history_map = cache_mgr.load_history_cache(period)

    # Detect stale histories missing amendment sub-tisk data and re-scrape
    stale_cts = [ct for ct, h in history_map.items() if h.amendment_tisk_ct1 is None and h.stages]
    if stale_cts:
        logger.info(
            "[amendment pipeline] Re-scraping {} stale tisk histories for period {} "
            "(missing amendment sub-tisk data)",
            len(stale_cts),
            period,
        )
        refreshed = scrape_histories_sync(period, stale_cts, cache_dir)
        history_map.update(refreshed)

    loaded = 0
    for tisk in period_data.tisk_lookup.values():
        hist = history_map.get(tisk.ct)
        if hist is not None:
            tisk.history = hist
            loaded += 1

    logger.info(
        "[amendment pipeline] Tisk histories: {} loaded into memory for period {}",
        loaded,
        period,
    )


def _identify_third_reading_bods(
    period_data: PeriodData,
) -> list[tuple[int, int, int, str]]:
    """Identify agenda items that likely had third-reading amendment votes.

    Looks for (schuze, bod) pairs where:
    - There's a tisk with legislative history showing 3. čtení
    - There are multiple votes on the same (schuze, bod)

    Args:
        period_data: Loaded period data with tisk_lookup.

    Returns:
        List of (schuze, bod, ct, tisk_nazev) tuples.
    """
    candidates: list[tuple[int, int, int, str]] = []

    # Group votes by (schuze, bod) and find those with multiple votes
    void_ids = period_data.void_votes.get_column("id_hlasovani")
    votes = period_data.votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    vote_counts = (
        votes.group_by(["schuze", "bod"])
        .agg(pl.col("id_hlasovani").count().alias("n_votes"))
        .filter(pl.col("n_votes") > 1)  # Multiple votes = likely amendments
    )

    for row in vote_counts.iter_rows(named=True):
        schuze = row["schuze"]
        bod = row["bod"]
        tisk = period_data.get_tisk(schuze, bod)
        if tisk is None:
            continue

        # Check if tisk history shows 3. čtení
        has_third_reading = False
        if tisk.history and tisk.history.stages:
            for stage in tisk.history.stages:
                if stage.stage_type == "3_cteni":
                    has_third_reading = True
                    break

        if has_third_reading:
            candidates.append((schuze, bod, tisk.ct, tisk.nazev))

    logger.info(
        "[amendment pipeline] Identified {} candidates from {} tisk entries for period {}",
        len(candidates),
        len(period_data.tisk_lookup),
        period_data.period,
    )
    return candidates


def _resolve_vote_ids(
    amendments: list[BillAmendmentData],
    period_data: PeriodData,
) -> None:
    """Resolve vote_number (cislo) to id_hlasovani via the votes DataFrame.

    Mutates the AmendmentVote objects in-place.

    Args:
        amendments: List of bill amendment data with parsed votes.
        period_data: Period data containing the votes DataFrame.
    """
    for bill in amendments:
        schuze_votes = period_data.votes.filter(pl.col("schuze") == bill.schuze)

        all_amends = list(bill.amendments)
        if bill.final_vote:
            all_amends.append(bill.final_vote)

        for amend in all_amends:
            if amend.vote_number == 0:
                continue
            match = schuze_votes.filter(pl.col("cislo") == amend.vote_number)
            if match.height > 0:
                amend.id_hlasovani = match.item(0, "id_hlasovani")

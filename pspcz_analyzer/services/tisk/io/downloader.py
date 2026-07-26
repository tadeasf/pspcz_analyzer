"""Download tisk PDFs from psp.cz."""

import time
from pathlib import Path

import httpx
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    PSP_ORIG2_BASE_URL,
    PSP_REQUEST_DELAY,
    TISKY_PDF_DIR,
)
from pspcz_analyzer.fileio import stream_to_atomic
from pspcz_analyzer.services.tisk.io.scraper import get_best_pdf


def _stream_pdf(url: str, dest: Path) -> None:
    """Stream a URL to ``dest`` atomically.

    On failure ``dest`` is left untouched (never a partial PDF), so a stale
    cached copy survives a failed re-download.
    """
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            stream_to_atomic(response, dest)


def download_tisk_pdf(
    period: int,
    ct: int,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> Path | None:
    """Download a single tisk PDF. Returns the cached path or None if unavailable."""
    pdf_dir = cache_dir / TISKY_PDF_DIR / str(period)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    dest = pdf_dir / f"{ct}.pdf"

    if dest.exists() and not force:
        logger.debug("Cached PDF: {}", dest)
        return dest

    doc = get_best_pdf(period, ct)
    if doc is None:
        logger.warning("No PDF found for tisk {}/{}", period, ct)
        return None

    url = f"{PSP_ORIG2_BASE_URL}?idd={doc.idd}"
    logger.info("Downloading PDF tisk {}/{} (idd={}) ...", period, ct, doc.idd)

    try:
        _stream_pdf(url, dest)
    except httpx.HTTPError:
        logger.exception("Failed to download tisk {}/{}", period, ct)
        return None

    logger.info("Downloaded {} ({:.1f} KB)", dest.name, dest.stat().st_size / 1e3)
    return dest


def download_subtisk_pdf(
    period: int,
    ct: int,
    ct1: int,
    idd: int,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> Path | None:
    """Download a sub-tisk PDF by idd. File naming: ``{ct}_{ct1}.pdf``."""
    pdf_dir = cache_dir / TISKY_PDF_DIR / str(period)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    dest = pdf_dir / f"{ct}_{ct1}.pdf"

    if dest.exists() and not force:
        logger.debug("Cached sub-tisk PDF: {}", dest)
        return dest

    url = f"{PSP_ORIG2_BASE_URL}?idd={idd}"
    logger.info("Downloading sub-tisk PDF {}/{}/{} (idd={}) ...", period, ct, ct1, idd)

    try:
        _stream_pdf(url, dest)
    except httpx.HTTPError:
        logger.exception("Failed to download sub-tisk {}/{}/{}", period, ct, ct1)
        return None

    logger.info("Downloaded {} ({:.1f} KB)", dest.name, dest.stat().st_size / 1e3)
    return dest


def download_period_tisky(
    period: int,
    tisk_numbers: list[int],
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> dict[int, Path]:
    """Download PDFs for multiple tisky with rate limiting.

    Returns a mapping of ct -> downloaded PDF path (skips failures).
    """
    results: dict[int, Path] = {}
    total = len(tisk_numbers)

    for i, ct in enumerate(tisk_numbers, 1):
        logger.info("[{}/{}] Tisk {}", i, total, ct)
        path = download_tisk_pdf(period, ct, cache_dir, force)
        if path is not None:
            results[ct] = path

        # Rate limit — be polite to psp.cz
        if i < total:
            time.sleep(PSP_REQUEST_DELAY)

    logger.info("Downloaded {}/{} tisk PDFs for period {}", len(results), total, period)
    return results

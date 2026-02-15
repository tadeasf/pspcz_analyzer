"""Download tisk PDFs and extract text from them."""

import time
from pathlib import Path

import httpx
import pymupdf
from loguru import logger

from pspcz_analyzer.config import (
    PSP_ORIG2_BASE_URL,
    PSP_REQUEST_DELAY,
    TISKY_PDF_DIR,
    TISKY_TEXT_DIR,
)
from pspcz_analyzer.data.tisk_scraper import get_best_pdf


def download_one(period: int, ct: int, idd: int, cache_dir: Path, force: bool) -> Path | None:
    """Download a single PDF by its idd. Returns path or None."""
    pdf_dir = cache_dir / TISKY_PDF_DIR / str(period)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    dest = pdf_dir / f"{ct}.pdf"

    if dest.exists() and not force:
        return dest

    url = f"{PSP_ORIG2_BASE_URL}?idd={idd}"
    try:
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=65536):
                        f.write(chunk)
        return dest
    except Exception:
        logger.opt(exception=True).warning("Failed to download tisk {}/{}", period, ct)
        dest.unlink(missing_ok=True)
        return None


def extract_one(pdf_path: Path, period: int, ct: int, cache_dir: Path, force: bool) -> Path | None:
    """Extract text from a single PDF. Returns text path or None."""
    text_dir = cache_dir / TISKY_TEXT_DIR / str(period)
    text_dir.mkdir(parents=True, exist_ok=True)
    dest = text_dir / f"{ct}.txt"

    if dest.exists() and not force:
        return dest

    try:
        doc = pymupdf.open(pdf_path)
        pages = [str(page.get_text()) for page in doc]
        doc.close()
        text = "\n\n".join(pages)
    except Exception:
        logger.opt(exception=True).warning("Failed to extract text from {}", pdf_path.name)
        return None

    if not text.strip():
        return None

    dest.write_text(text, encoding="utf-8")
    return dest


def process_period_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
    force: bool = False,
) -> tuple[dict[int, Path], dict[int, Path]]:
    """Synchronous pipeline: scrape -> download -> extract for all ct numbers.

    Returns (pdf_paths, text_paths).
    """
    pdf_paths: dict[int, Path] = {}
    text_paths: dict[int, Path] = {}
    total = len(ct_numbers)

    for i, ct in enumerate(ct_numbers, 1):
        # Check caches first (fast path — no HTTP needed)
        pdf_dir = cache_dir / TISKY_PDF_DIR / str(period)
        text_dir = cache_dir / TISKY_TEXT_DIR / str(period)
        pdf_cached = pdf_dir / f"{ct}.pdf"
        text_cached = text_dir / f"{ct}.txt"

        if text_cached.exists() and not force:
            text_paths[ct] = text_cached
            if pdf_cached.exists():
                pdf_paths[ct] = pdf_cached
            continue

        if pdf_cached.exists() and not force:
            pdf_paths[ct] = pdf_cached
            # Just need extraction
            txt = extract_one(pdf_cached, period, ct, cache_dir, force)
            if txt:
                text_paths[ct] = txt
            continue

        # Need to scrape + download
        if i % 50 == 0 or i == 1:
            logger.info("[tisk pipeline] Period {}: processing {}/{}", period, i, total)

        doc = get_best_pdf(period, ct)
        if doc is None:
            time.sleep(PSP_REQUEST_DELAY)
            continue

        pdf = download_one(period, ct, doc.idd, cache_dir, force)
        time.sleep(PSP_REQUEST_DELAY)

        if pdf is None:
            continue
        pdf_paths[ct] = pdf

        txt = extract_one(pdf, period, ct, cache_dir, force)
        if txt:
            text_paths[ct] = txt

    return pdf_paths, text_paths

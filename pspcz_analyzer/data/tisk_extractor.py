"""Extract text from tisk PDFs using PyMuPDF."""

from pathlib import Path

import pymupdf
from loguru import logger

from pspcz_analyzer.config import DEFAULT_CACHE_DIR, TISKY_TEXT_DIR


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract all text from a PDF file using PyMuPDF."""
    try:
        doc = pymupdf.open(pdf_path)
        pages = [str(page.get_text()) for page in doc]
        doc.close()
        return "\n\n".join(pages)
    except Exception:
        logger.exception("Failed to extract text from {}", pdf_path)
        return ""


def extract_and_cache(
    pdf_path: Path,
    period: int,
    ct: int,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> Path | None:
    """Extract text from a PDF and cache it as a .txt file."""
    text_dir = cache_dir / TISKY_TEXT_DIR / str(period)
    text_dir.mkdir(parents=True, exist_ok=True)
    dest = text_dir / f"{ct}.txt"

    if dest.exists() and not force:
        logger.debug("Cached text: {}", dest)
        return dest

    text = extract_text_from_pdf(pdf_path)
    if not text.strip():
        logger.warning("Empty text extracted from {} (scanned PDF?)", pdf_path.name)
        return None

    dest.write_text(text, encoding="utf-8")
    logger.info("Extracted text for tisk {} ({:.1f} KB)", ct, dest.stat().st_size / 1e3)
    return dest


def extract_period_texts(
    period: int,
    pdf_paths: dict[int, Path],
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
) -> dict[int, Path]:
    """Extract text from all PDFs for a period.

    Returns mapping of ct -> text file path (skips failures).
    """
    results: dict[int, Path] = {}
    total = len(pdf_paths)

    for i, (ct, pdf_path) in enumerate(sorted(pdf_paths.items()), 1):
        logger.info("[{}/{}] Extracting text from tisk {}", i, total, ct)
        path = extract_and_cache(pdf_path, period, ct, cache_dir, force)
        if path is not None:
            results[ct] = path

    logger.info("Extracted text for {}/{} tisky in period {}", len(results), total, period)
    return results

"""Extract text from tisk PDFs using PyMuPDF, with HTML fallback.

psp.cz ``orig2.sqw`` sometimes serves HTML content instead of real PDFs.
When PyMuPDF fails or returns empty text, we check if the file is actually
HTML and parse it with BeautifulSoup as a fallback.
"""

import re
from pathlib import Path

import pymupdf
from bs4 import BeautifulSoup
from loguru import logger

from pspcz_analyzer.config import DEFAULT_CACHE_DIR, TISKY_TEXT_DIR

# Suppress noisy MuPDF C-level warnings/errors on malformed PDFs from psp.cz
# (e.g. "no XObject subtype specified", "unknown cid font type").
# Text extraction still works — these are non-fatal parse warnings.
pymupdf.TOOLS.mupdf_display_warnings(False)
pymupdf.TOOLS.mupdf_display_errors(False)

_HTML_MARKER_RE = re.compile(rb"<(?:html|!doctype|head)\b", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


def _extract_text_from_html(pdf_path: Path) -> str:
    """Try to extract text from a file that is actually HTML.

    Reads the first 512 bytes to detect HTML markers. If found, parses
    with BeautifulSoup and extracts text from ``<body>``.

    Args:
        pdf_path: Path to the file (may be .pdf extension but HTML content).

    Returns:
        Extracted text, or empty string if not HTML or parsing fails.
    """
    try:
        raw = pdf_path.read_bytes()
    except Exception:
        return ""

    head = raw[:512]
    if not _HTML_MARKER_RE.search(head):
        return ""

    try:
        html_text = raw.decode("utf-8", errors="replace")
    except Exception:
        try:
            html_text = raw.decode("windows-1250", errors="replace")
        except Exception:
            return ""

    try:
        soup = BeautifulSoup(html_text, "html.parser")
        body = soup.body
        if body is None:
            body = soup
        text = body.get_text(separator=" ", strip=True)
        text = _WHITESPACE_RE.sub(" ", text).strip()
        if text:
            logger.info("Extracted text from HTML-as-PDF: {} ({} chars)", pdf_path.name, len(text))
        return text
    except Exception:
        logger.debug("Failed to parse HTML from {}", pdf_path.name)
        return ""


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract all text from a PDF file using PyMuPDF, with HTML fallback.

    When PyMuPDF fails or returns empty text (common for HTML files saved
    as .pdf by psp.cz), falls back to BeautifulSoup HTML parsing.
    """
    try:
        doc = pymupdf.open(pdf_path)
        pages = [str(page.get_text()) for page in doc]
        doc.close()
        text = "\n\n".join(pages)
        if text.strip():
            return text
    except Exception:
        pass

    # Fallback: check if file is actually HTML
    html_text = _extract_text_from_html(pdf_path)
    if html_text:
        return html_text

    logger.warning("Failed to extract text from {} (not PDF or HTML)", pdf_path.name)
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

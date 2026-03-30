"""Amendment PDF download, parsing, and merge with steno data.

Functions for downloading amendment PDFs, parsing their structure,
and merging PDF-parsed amendments with steno-parsed vote data.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from pspcz_analyzer.models.amendment_models import AmendmentVote, BillAmendmentData
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.services.amendments.pdf_parser import PdfAmendment, parse_amendment_pdf
from pspcz_analyzer.services.tisk.io import (
    download_subtisk_pdf,
    extract_text_from_pdf,
    scrape_all_subtisk_documents,
)
from pspcz_analyzer.services.tisk.io.history_scraper import TiskHistory


def _download_amendment_pdf(
    period: int,
    ct: int,
    amendment_ct1: int | None,
    amendment_idd: int | None,
    cache_dir: Path,
) -> str:
    """Download amendment sub-tisk PDF and extract text.

    Uses the known CT1/idd from history scraping if available,
    otherwise falls back to iterating sub-tisk versions.

    Args:
        period: Electoral period number.
        ct: Tisk number.
        amendment_ct1: Known amendment sub-tisk CT1 (from history).
        amendment_idd: Known idd for direct download (from history).
        cache_dir: Base cache directory.

    Returns:
        Extracted PDF text, or empty string on failure.
    """
    # Fast path: download directly using known CT1/idd
    if amendment_ct1 is not None and amendment_idd is not None:
        pdf_path = download_subtisk_pdf(period, ct, amendment_ct1, amendment_idd, cache_dir)
        if pdf_path is not None:
            text = extract_text_from_pdf(pdf_path)
            if text.strip():
                return text

    # Fallback: iterate sub-tisk versions
    try:
        versions = scrape_all_subtisk_documents(period, ct)
    except Exception:
        logger.warning("[amendment pipeline] Failed to scrape sub-tisk versions for ct={}", ct)
        return ""

    # Filter for amendment sub-versions (CT1 >= 2) with downloadable PDFs
    amendment_versions = [v for v in versions if v.ct1 >= 2 and v.idd is not None]
    if not amendment_versions:
        return ""

    texts: list[str] = []
    for ver in amendment_versions:
        pdf_path = download_subtisk_pdf(period, ct, ver.ct1, ver.idd, cache_dir)  # type: ignore[arg-type]
        if pdf_path is None:
            continue
        text = extract_text_from_pdf(pdf_path)
        if text.strip():
            texts.append(text)

    return "\n\n---\n\n".join(texts)


def _pdf_download_and_parse(
    bills: list[BillAmendmentData],
    period: int,
    cache_dir: Path,
    period_data: PeriodData,
) -> tuple[dict[int, list[PdfAmendment]], dict[int, str]]:
    """Download amendment PDFs and parse their structure.

    Groups bills by CT to avoid duplicate downloads. Populates
    bill.amendment_tisk_ct1 and bill.amendment_tisk_idd.

    Args:
        bills: List of bill amendment data.
        period: Electoral period number.
        cache_dir: Base cache directory.
        period_data: Period data with tisk_lookup for history access.

    Returns:
        Tuple of (ct -> list[PdfAmendment], ct -> raw PDF text).
    """
    pdf_data: dict[int, list[PdfAmendment]] = {}
    ct_texts: dict[int, str] = {}

    # Build ct -> TiskHistory lookup from period_data
    ct_history: dict[int, TiskHistory] = {}
    for tisk in period_data.tisk_lookup.values():
        if tisk.history is not None:
            ct_history[tisk.ct] = tisk.history

    # Collect unique CTs
    unique_cts = sorted({bill.ct for bill in bills})

    for ct in unique_cts:
        history = ct_history.get(ct)
        amendment_ct1 = history.amendment_tisk_ct1 if history else None
        amendment_idd = history.amendment_tisk_idd if history else None

        text = _download_amendment_pdf(period, ct, amendment_ct1, amendment_idd, cache_dir)
        ct_texts[ct] = text

        if text:
            parsed = parse_amendment_pdf(text)
            pdf_data[ct] = parsed
        else:
            pdf_data[ct] = []

    # Populate bill-level metadata fields (not the large PDF text)
    for bill in bills:
        history = ct_history.get(bill.ct)
        if history:
            bill.amendment_tisk_ct1 = history.amendment_tisk_ct1
            bill.amendment_tisk_idd = history.amendment_tisk_idd

    extracted = sum(1 for t in ct_texts.values() if t)
    parsed_count = sum(1 for p in pdf_data.values() if p)
    logger.info(
        "[amendment pipeline] PDFs: {}/{} CTs have text, {}/{} parsed into amendments",
        extracted,
        len(unique_cts),
        parsed_count,
        len(unique_cts),
    )

    return pdf_data, ct_texts


def _pop_numeric_variants(
    steno_by_letter: dict[str, AmendmentVote],
    pdf_letter: str,
) -> list[AmendmentVote]:
    """Pop ALL steno amendments matching numeric variants of a PDF letter.

    E.g., PDF letter "A" matches steno "A1", "A2", "A3" — all are returned.

    Args:
        steno_by_letter: Mutable dict of steno amendments keyed by letter.
        pdf_letter: PDF amendment letter (e.g. "A").

    Returns:
        List of matching steno AmendmentVote objects (may be empty).
    """
    matches: list[AmendmentVote] = []
    keys_to_pop = [
        key
        for key in sorted(steno_by_letter.keys())
        if key.startswith(pdf_letter) and key[len(pdf_letter) :].isdigit()
    ]
    for key in keys_to_pop:
        matches.append(steno_by_letter.pop(key))
    return matches


def _merge_pdf_and_steno(
    bills: list[BillAmendmentData],
    pdf_data: dict[int, list[PdfAmendment]],
) -> None:
    """Merge PDF-parsed amendment structure with steno-parsed vote data.

    For each bill:
    - PDF amendments with matching steno data: copy vote info from steno,
      text/name from PDF
    - PDF-only amendments (no matching steno): create unvoted AmendmentVote
    - Steno-only leftovers (oral amendments): keep as-is

    Args:
        bills: List of bill amendment data (mutated in-place).
        pdf_data: Mapping of ct -> list[PdfAmendment].
    """
    total_pdf_matched = 0
    total_pdf_only = 0
    total_steno_only = 0

    for bill in bills:
        pdf_amendments = pdf_data.get(bill.ct, [])
        if not pdf_amendments:
            continue

        # Build steno lookup by letter (uppercase, stripped)
        steno_by_letter: dict[str, AmendmentVote] = {}
        for amend in bill.amendments:
            key = amend.letter.strip().upper()
            steno_by_letter[key] = amend

        merged: list[AmendmentVote] = []

        for pdf_amend in pdf_amendments:
            pdf_key = pdf_amend.letter.strip().upper()

            # Try exact match first
            steno = steno_by_letter.pop(pdf_key, None)
            steno_matches: list[AmendmentVote] = [steno] if steno else []

            # Try numeric variants: "A" matches steno "A1", "A2", "A3"
            if not steno_matches:
                steno_matches = _pop_numeric_variants(steno_by_letter, pdf_key)

            # Also check grouped_with: if steno has grouped_with letters,
            # try to match those PDF letters too
            for s in list(steno_matches):
                for grouped_letter in s.grouped_with:
                    gl = grouped_letter.strip().upper()
                    # Extract base letter (e.g. "F2" -> "F")
                    base = gl.rstrip("0123456789")
                    if base and base in steno_by_letter:
                        steno_by_letter.pop(base)

            if steno_matches:
                # Enrich all matching steno entries with PDF data.
                # All matches get raw_text — steno "E1" means the entire
                # letter E section, typically voted en bloc.
                for steno_item in steno_matches:
                    steno_item.amendment_text = pdf_amend.raw_text
                    steno_item.pdf_submitter_names = list(pdf_amend.submitter_names)
                    if not steno_item.submitter_names and pdf_amend.submitter_names:
                        steno_item.submitter_names = list(pdf_amend.submitter_names)
                    merged.append(steno_item)
                    total_pdf_matched += 1
            else:
                # PDF-only: no steno match → no vote linkage, skip
                total_pdf_only += 1

        # Append steno-only leftovers (oral amendments not in PDF)
        for steno_leftover in steno_by_letter.values():
            merged.append(steno_leftover)
            total_steno_only += 1

        bill.amendments = merged

    logger.info(
        "[amendment pipeline] Merge: {} PDF+steno matched, {} PDF-only, {} steno-only",
        total_pdf_matched,
        total_pdf_only,
        total_steno_only,
    )

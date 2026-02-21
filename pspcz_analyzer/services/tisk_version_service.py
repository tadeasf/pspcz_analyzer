"""Sub-tisk version downloading and LLM diff analysis."""

import json
import time
from dataclasses import asdict
from pathlib import Path

import pymupdf
from loguru import logger

from pspcz_analyzer.config import (
    PSP_REQUEST_DELAY,
    TISKY_META_DIR,
    TISKY_TEXT_DIR,
    TISKY_VERSION_DIFFS_DIR,
)
from pspcz_analyzer.data.tisk_downloader import download_subtisk_pdf
from pspcz_analyzer.data.tisk_scraper import SubTiskVersion, scrape_all_subtisk_documents
from pspcz_analyzer.services.ollama_service import OllamaClient, OpenAIClient, create_llm_client


def download_subtisk_versions_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
) -> dict[int, list[dict]]:
    """Download all sub-tisk versions (CT1=0..N) for tisky in a period.

    Caches scan results as JSON per-ct so restarts skip already-processed tisky.
    Returns {ct: [SubTiskVersion dicts]}.
    """
    # JSON cache dir for sub-tisk scan results
    scan_dir = cache_dir / TISKY_META_DIR / str(period) / "subtisk_versions"
    scan_dir.mkdir(parents=True, exist_ok=True)

    result: dict[int, list[dict]] = {}
    total = len(ct_numbers)
    scraped = 0

    for i, ct in enumerate(ct_numbers, 1):
        scan_cache = scan_dir / f"{ct}.json"

        # Load from JSON cache if available
        if scan_cache.exists():
            try:
                data = json.loads(scan_cache.read_text(encoding="utf-8"))
                if data:  # non-empty means this ct has sub-versions
                    result[ct] = data
                continue
            except Exception:
                logger.debug("Bad cache for ct={}, re-scraping", ct)

        if i % 50 == 0 or i == 1:
            logger.info(
                "[tisk pipeline] Scraping sub-tisk versions for period {}: {}/{}",
                period,
                i,
                total,
            )

        # Scrape sub-tisk pages to find versions
        versions_data = scrape_all_subtisk_documents(period, ct)
        scraped += 1

        if len(versions_data) <= 1:
            # Only CT1=0 or nothing — save empty list to cache so we don't re-scrape
            scan_cache.write_text("[]", encoding="utf-8")
            time.sleep(PSP_REQUEST_DELAY)
            continue

        text_dir = cache_dir / TISKY_TEXT_DIR / str(period)
        version_dicts = []

        for v in versions_data:
            if v.idd and v.ct1 > 0:  # CT1=0 is already downloaded by main pipeline
                pdf = download_subtisk_pdf(period, ct, v.ct1, v.idd, cache_dir)
                time.sleep(PSP_REQUEST_DELAY)

                # Extract text if PDF downloaded
                if pdf:
                    _extract_subtisk_text(pdf, text_dir, ct, v)

            vd = asdict(v)
            version_dicts.append(vd)

        # Save scan result to cache (even if version_dicts is just CT1=0)
        scan_cache.write_text(
            json.dumps(version_dicts, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if version_dicts:
            result[ct] = version_dicts

    logger.info(
        "[tisk pipeline] Sub-tisk versions for period {}: {} cached, {} new, {} with multiple versions",
        period,
        total - scraped,
        scraped,
        len(result),
    )
    return result


def _extract_subtisk_text(pdf: Path, text_dir: Path, ct: int, v: SubTiskVersion) -> None:
    """Extract text from a sub-tisk PDF and update the version's flags."""
    txt_dest = text_dir / f"{ct}_{v.ct1}.txt"
    txt_dest.parent.mkdir(parents=True, exist_ok=True)
    if not txt_dest.exists():
        try:
            doc = pymupdf.open(pdf)
            pages = [str(page.get_text()) for page in doc]
            doc.close()
            text = "\n\n".join(pages)
            if text.strip():
                txt_dest.write_text(text, encoding="utf-8")
                v.has_text = True
        except Exception:
            logger.opt(exception=True).warning(
                "Failed to extract text from {}",
                pdf.name,
            )
    else:
        v.has_text = True
    v.has_pdf = True


def analyze_version_diffs_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
) -> tuple[dict[str, str], dict[str, str]]:
    """Run LLM comparison on consecutive sub-tisk versions.

    Returns ({"{ct}_{ct1}": diff_cs}, {"{ct}_{ct1}": diff_en}).
    """
    llm = create_llm_client()
    if not llm.is_available():
        logger.info("[tisk pipeline] LLM not available, skipping version diff analysis")
        return {}, {}

    text_dir = cache_dir / TISKY_TEXT_DIR / str(period)
    diff_dir = cache_dir / TISKY_META_DIR / str(period) / TISKY_VERSION_DIFFS_DIR
    diff_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, str] = {}
    result_en: dict[str, str] = {}

    for ct in ct_numbers:
        if not text_dir.exists():
            continue

        versions = _collect_version_texts(text_dir, ct)
        if len(versions) < 2:
            continue

        # Compare consecutive pairs
        for j in range(len(versions) - 1):
            ct1_old, path_old = versions[j]
            ct1_new, path_new = versions[j + 1]
            diff_key = f"{ct}_{ct1_new}"
            diff_file = diff_dir / f"{diff_key}.txt"
            diff_file_en = diff_dir / f"{diff_key}_en.txt"

            # Check cache — both CS and EN
            if diff_file.exists():
                result[diff_key] = diff_file.read_text(encoding="utf-8")
                if diff_file_en.exists():
                    result_en[diff_key] = diff_file_en.read_text(encoding="utf-8")
                continue

            summaries = _compare_version_pair_bilingual(
                llm, path_old, path_new, ct1_old, ct1_new, period, ct
            )
            if summaries["cs"]:
                diff_file.write_text(summaries["cs"], encoding="utf-8")
                result[diff_key] = summaries["cs"]
            if summaries["en"]:
                diff_file_en.write_text(summaries["en"], encoding="utf-8")
                result_en[diff_key] = summaries["en"]

    logger.info(
        "[tisk pipeline] Version diffs for period {}: {} comparisons",
        period,
        len(result),
    )
    return result, result_en


def _collect_version_texts(text_dir: Path, ct: int) -> list[tuple[int, Path]]:
    """Find all text versions for a tisk, sorted by CT1 number."""
    versions: list[tuple[int, Path]] = []
    base_txt = text_dir / f"{ct}.txt"
    if base_txt.exists():
        versions.append((0, base_txt))
    for txt_path in sorted(text_dir.glob(f"{ct}_*.txt")):
        parts = txt_path.stem.split("_")
        if len(parts) == 2:
            try:
                ct1 = int(parts[1])
                versions.append((ct1, txt_path))
            except ValueError:
                continue
    versions.sort(key=lambda x: x[0])
    return versions


def _compare_version_pair_bilingual(
    llm: OllamaClient | OpenAIClient,
    path_old: Path,
    path_new: Path,
    ct1_old: int,
    ct1_new: int,
    period: int,
    ct: int,
) -> dict[str, str]:
    """Compare two consecutive version texts using LLM, bilingual output."""
    text_old = path_old.read_text(encoding="utf-8")
    text_new = path_new.read_text(encoding="utf-8")

    logger.info(
        "[tisk pipeline] Comparing versions CT1={} vs CT1={} for tisk {}/{} (bilingual)",
        ct1_old,
        ct1_new,
        period,
        ct,
    )
    return llm.compare_versions_bilingual(
        text_old,
        text_new,
        ct1_old,
        ct1_new,
    )

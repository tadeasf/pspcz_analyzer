"""Background pipeline: download tisk PDFs, extract text, classify topics.

Runs as an asyncio background task so the web server stays responsive.
"""

import asyncio
import time
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    PERIOD_ORGAN_IDS,
    PSP_REQUEST_DELAY,
    TISKY_META_DIR,
    TISKY_PDF_DIR,
    TISKY_TEXT_DIR,
)
from pspcz_analyzer.data.tisk_scraper import get_best_pdf
from pspcz_analyzer.services.topic_service import classify_tisk_primary


def _download_one(period: int, ct: int, idd: int, cache_dir: Path, force: bool) -> Path | None:
    """Download a single PDF by its idd. Returns path or None."""
    import httpx

    from pspcz_analyzer.config import PSP_ORIG2_BASE_URL

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


def _extract_one(pdf_path: Path, period: int, ct: int, cache_dir: Path, force: bool) -> Path | None:
    """Extract text from a single PDF. Returns text path or None."""
    import pymupdf

    text_dir = cache_dir / TISKY_TEXT_DIR / str(period)
    text_dir.mkdir(parents=True, exist_ok=True)
    dest = text_dir / f"{ct}.txt"

    if dest.exists() and not force:
        return dest

    try:
        doc = pymupdf.open(pdf_path)
        pages = [page.get_text() for page in doc]
        doc.close()
        text = "\n\n".join(pages)
    except Exception:
        logger.opt(exception=True).warning("Failed to extract text from {}", pdf_path.name)
        return None

    if not text.strip():
        return None

    dest.write_text(text, encoding="utf-8")
    return dest


def _process_period_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
    force: bool = False,
) -> tuple[dict[int, Path], dict[int, Path]]:
    """Synchronous pipeline: scrape → download → extract for all ct numbers.

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
            txt = _extract_one(pdf_cached, period, ct, cache_dir, force)
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

        pdf = _download_one(period, ct, doc.idd, cache_dir, force)
        time.sleep(PSP_REQUEST_DELAY)

        if pdf is None:
            continue
        pdf_paths[ct] = pdf

        txt = _extract_one(pdf, period, ct, cache_dir, force)
        if txt:
            text_paths[ct] = txt

    return pdf_paths, text_paths


def _classify_and_save(period: int, text_paths: dict[int, Path], cache_dir: Path) -> dict[int, list[str]]:
    """Run keyword classification on extracted texts, save parquet, return topic map."""
    meta_dir = cache_dir / TISKY_META_DIR / str(period)
    meta_dir.mkdir(parents=True, exist_ok=True)

    topic_map: dict[int, list[str]] = {}
    records = []
    for ct, text_path in sorted(text_paths.items()):
        text = text_path.read_text(encoding="utf-8")
        topic = classify_tisk_primary(text, "")
        records.append({"ct": ct, "topic": topic or ""})
        if topic:
            topic_map[ct] = [topic]

    if records:
        df = pl.DataFrame(records)
        df.write_parquet(meta_dir / "topic_classifications.parquet")
        classified = sum(1 for r in records if r["topic"])
        logger.info(
            "[tisk pipeline] Classified {}/{} tisky for period {}",
            classified, len(records), period,
        )

    return topic_map


class TiskPipelineService:
    """Manages background tisk processing for loaded periods."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir
        self._tasks: dict[int, asyncio.Task] = {}

    def start_period(
        self,
        period: int,
        ct_numbers: list[int],
        on_complete=None,
    ) -> None:
        """Start background processing for a period. Idempotent — skips if already running."""
        if period in self._tasks and not self._tasks[period].done():
            logger.debug("Tisk pipeline already running for period {}", period)
            return

        task = asyncio.create_task(
            self._run_period(period, ct_numbers, on_complete),
            name=f"tisk-pipeline-{period}",
        )
        self._tasks[period] = task
        logger.info(
            "[tisk pipeline] Started background processing for period {} ({} tisky)",
            period, len(ct_numbers),
        )

    async def _run_period(self, period: int, ct_numbers: list[int], on_complete) -> None:
        """Run the full pipeline in a thread to avoid blocking the event loop."""
        try:
            pdf_paths, text_paths = await asyncio.to_thread(
                _process_period_sync, period, ct_numbers, self.cache_dir,
            )
            topic_map = await asyncio.to_thread(
                _classify_and_save, period, text_paths, self.cache_dir,
            )
            logger.info(
                "[tisk pipeline] Period {} complete: {} PDFs, {} texts, {} topics",
                period, len(pdf_paths), len(text_paths), len(topic_map),
            )
            if on_complete:
                on_complete(period, text_paths, topic_map)
        except Exception:
            logger.opt(exception=True).error("[tisk pipeline] Failed for period {}", period)

    def is_running(self, period: int) -> bool:
        task = self._tasks.get(period)
        return task is not None and not task.done()

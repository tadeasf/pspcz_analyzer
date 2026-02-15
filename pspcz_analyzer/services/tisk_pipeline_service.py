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
    TISKY_HISTORIE_DIR,
    TISKY_META_DIR,
    TISKY_PDF_DIR,
    TISKY_TEXT_DIR,
)
from pspcz_analyzer.data.tisk_scraper import get_best_pdf
from pspcz_analyzer.services.topic_service import classify_tisk_primary_label


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


def _classify_and_save(
    period: int, text_paths: dict[int, Path], cache_dir: Path,
) -> tuple[dict[int, list[str]], dict[int, str]]:
    """Run topic classification on extracted texts, save parquet, return maps.

    Uses Ollama AI when available (free-form topics), falls back to keyword matching.
    Saves incrementally after each tisk and resumes from where it left off.
    Returns (topic_map, summary_map).
    """
    from pspcz_analyzer.services.ollama_service import (
        OllamaClient,
        deserialize_topics,
        serialize_topics,
    )

    meta_dir = cache_dir / TISKY_META_DIR / str(period)
    meta_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = meta_dir / "topic_classifications.parquet"

    # Load existing records to resume from (if any)
    existing: dict[int, dict] = {}
    if parquet_path.exists():
        df = pl.read_parquet(parquet_path)
        for row in df.iter_rows(named=True):
            existing[row["ct"]] = row

    # Figure out which tisky still need processing
    remaining = {ct: p for ct, p in text_paths.items() if ct not in existing}

    ollama = OllamaClient()
    use_ai = ollama.is_available()
    total = len(text_paths)
    already = len(existing)

    if already:
        logger.info(
            "[tisk pipeline] Resuming: {} already done, {} remaining out of {} total",
            already, len(remaining), total,
        )

    if use_ai:
        logger.info("[tisk pipeline] Ollama available, using AI classification + summarization ({} to process)", len(remaining))
    else:
        logger.info("[tisk pipeline] Ollama not available, using keyword classification ({} to process)", len(remaining))

    # Start from existing records
    records = list(existing.values())

    for i, (ct, text_path) in enumerate(sorted(remaining.items()), already + 1):
        text = text_path.read_text(encoding="utf-8")
        topics: list[str] = []
        summary = ""
        source = "keyword"

        if use_ai:
            logger.info("[tisk pipeline] [{}/{}] AI classifying tisk ct={} ...", i, total, ct)
            topics = ollama.classify_topics(text, "")
            if topics:
                source = f"ollama:{ollama.model}"
            logger.info("[tisk pipeline] [{}/{}] AI summarizing tisk ct={} ...", i, total, ct)
            summary = ollama.summarize(text, "")
            logger.info(
                "[tisk pipeline] [{}/{}] tisk ct={} -> topics={} summary={}chars ({})",
                i, total, ct, topics or "(none)", len(summary), source,
            )
        else:
            kw_topic = classify_tisk_primary_label(text, "")
            if kw_topic:
                topics = [kw_topic]
            if i % 20 == 0 or i == total:
                logger.info("[tisk pipeline] [{}/{}] keyword classification progress", i, total)

        # Fall back to keyword classification if AI didn't produce topics
        if use_ai and not topics:
            kw_topic = classify_tisk_primary_label(text, "")
            if kw_topic:
                topics = [kw_topic]
                source = "keyword"
                logger.debug("[tisk pipeline] tisk ct={} AI returned no topics, keyword fallback -> {}", ct, kw_topic)

        record = {
            "ct": ct,
            "topic": serialize_topics(topics),
            "summary": summary,
            "source": source,
        }
        records.append(record)

        # Save after every tisk so progress is never lost
        df = pl.DataFrame(records)
        df.write_parquet(parquet_path)

    # Build return maps from all records (existing + new)
    topic_map: dict[int, list[str]] = {}
    summary_map: dict[int, str] = {}
    for r in records:
        parsed = deserialize_topics(r["topic"])
        if parsed:
            topic_map[r["ct"]] = parsed
        if r.get("summary"):
            summary_map[r["ct"]] = r["summary"]

    classified = len(topic_map)
    ai_count = sum(1 for r in records if r.get("source", "").startswith("ollama"))
    logger.info(
        "[tisk pipeline] Classified {}/{} tisky for period {} (AI: {}, keyword: {})",
        classified, len(records), period, ai_count, classified - ai_count,
    )

    return topic_map, summary_map


def _consolidate_topics(
    period: int, cache_dir: Path,
) -> tuple[dict[int, list[str]], dict[int, str]]:
    """Run LLM-powered topic deduplication after classification.

    Reads the parquet, collects all unique topic labels, asks the LLM to
    consolidate similar ones, applies the mapping, and re-writes the parquet.

    Returns updated (topic_map, summary_map).
    """
    from pspcz_analyzer.services.ollama_service import (
        OllamaClient,
        deserialize_topics,
        serialize_topics,
    )

    meta_dir = cache_dir / TISKY_META_DIR / str(period)
    parquet_path = meta_dir / "topic_classifications.parquet"
    consolidated_marker = meta_dir / "topics_consolidated.done"

    if not parquet_path.exists():
        logger.warning("[tisk pipeline] No parquet to consolidate for period {}", period)
        return {}, {}

    df = pl.read_parquet(parquet_path)
    records = df.to_dicts()

    # If consolidation was already done, just return the maps from the parquet
    if consolidated_marker.exists():
        logger.info(
            "[tisk pipeline] Topics already consolidated for period {}, skipping",
            period,
        )
        topic_map: dict[int, list[str]] = {}
        summary_map: dict[int, str] = {}
        for r in records:
            parsed = deserialize_topics(r.get("topic", ""))
            if parsed:
                topic_map[r["ct"]] = parsed
            if r.get("summary"):
                summary_map[r["ct"]] = r["summary"]
        return topic_map, summary_map

    # Collect all unique topic labels
    all_topics: set[str] = set()
    for r in records:
        for t in deserialize_topics(r.get("topic", "")):
            all_topics.add(t)

    unique_topics = sorted(all_topics)

    if len(unique_topics) <= 10:
        logger.info(
            "[tisk pipeline] Only {} unique topics for period {}, skipping consolidation",
            len(unique_topics), period,
        )
        # Still build and return the maps
        topic_map = {}
        summary_map = {}
        for r in records:
            parsed = deserialize_topics(r["topic"])
            if parsed:
                topic_map[r["ct"]] = parsed
            if r.get("summary"):
                summary_map[r["ct"]] = r["summary"]
        # Mark as done even if skipped (few topics)
        consolidated_marker.touch()
        return topic_map, summary_map

    ollama = OllamaClient()
    if not ollama.is_available():
        logger.info("[tisk pipeline] Ollama not available, skipping topic consolidation")
        topic_map = {}
        summary_map = {}
        for r in records:
            parsed = deserialize_topics(r.get("topic", ""))
            if parsed:
                topic_map[r["ct"]] = parsed
            if r.get("summary"):
                summary_map[r["ct"]] = r["summary"]
        return topic_map, summary_map

    logger.info(
        "[tisk pipeline] Consolidating topics for period {}: {} unique topics",
        period, len(unique_topics),
    )
    mapping = ollama.consolidate_topics(unique_topics)

    # Count how many actually changed
    changed = sum(1 for old, new in mapping.items() if old != new)
    canonical = len(set(mapping.values()))
    logger.info(
        "[tisk pipeline] Consolidating topics for period {}: {} unique -> {} canonical ({} remapped)",
        period, len(unique_topics), canonical, changed,
    )

    # Apply mapping to all records
    for r in records:
        old_topics = deserialize_topics(r.get("topic", ""))
        new_topics = [mapping.get(t, t) for t in old_topics]
        # Deduplicate while preserving order
        seen: set[str] = set()
        deduped: list[str] = []
        for t in new_topics:
            if t not in seen:
                seen.add(t)
                deduped.append(t)
        r["topic"] = serialize_topics(deduped)

    # Re-write parquet
    df = pl.DataFrame(records)
    df.write_parquet(parquet_path)

    # Write marker so we don't re-consolidate on next startup
    consolidated_marker.touch()

    # Build return maps
    topic_map = {}
    summary_map = {}
    for r in records:
        parsed = deserialize_topics(r["topic"])
        if parsed:
            topic_map[r["ct"]] = parsed
        if r.get("summary"):
            summary_map[r["ct"]] = r["summary"]

    return topic_map, summary_map


def _scrape_histories_sync(
    period: int,
    ct_numbers: list[int],
    cache_dir: Path,
) -> dict:
    """Scrape legislative history pages for all tisky in a period.

    Caches results as JSON files. Skips already-cached tisky.
    Returns {ct: TiskHistory} dict.
    """
    from pspcz_analyzer.data.history_scraper import (
        TiskHistory,
        load_history_json,
        save_history_json,
        scrape_tisk_history,
    )

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
                period, i, total,
            )

        h = scrape_tisk_history(period, ct)
        if h:
            save_history_json(h, json_path)
            histories[ct] = h
            scraped += 1

        time.sleep(PSP_REQUEST_DELAY)

    logger.info(
        "[tisk pipeline] History scraping for period {}: {} cached, {} new, {} total",
        period, len(histories) - scraped, scraped, len(histories),
    )
    return histories


class TiskPipelineService:
    """Manages background tisk processing for loaded periods."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = cache_dir
        self._tasks: dict[int, asyncio.Task] = {}
        self._all_task: asyncio.Task | None = None

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

    def start_all_periods(
        self,
        period_ct_numbers: list[tuple[int, list[int]]],
        on_complete=None,
    ) -> None:
        """Process all periods sequentially in one background task (newest first).

        period_ct_numbers: list of (period, ct_numbers) tuples, ordered by priority.
        """
        if self._all_task is not None and not self._all_task.done():
            logger.debug("All-periods pipeline already running")
            return

        self._all_task = asyncio.create_task(
            self._run_all_periods(period_ct_numbers, on_complete),
            name="tisk-pipeline-all",
        )
        total_tisky = sum(len(cts) for _, cts in period_ct_numbers)
        logger.info(
            "[tisk pipeline] Started sequential processing of {} periods ({} tisky total)",
            len(period_ct_numbers), total_tisky,
        )

    async def _run_all_periods(
        self,
        period_ct_numbers: list[tuple[int, list[int]]],
        on_complete,
    ) -> None:
        """Process periods one by one, sequentially."""
        for period, ct_numbers in period_ct_numbers:
            if not ct_numbers:
                continue
            logger.info(
                "[tisk pipeline] === Starting period {} ({} tisky) ===",
                period, len(ct_numbers),
            )
            try:
                # Scrape legislative history pages (fast, cached)
                histories = await asyncio.to_thread(
                    _scrape_histories_sync, period, ct_numbers, self.cache_dir,
                )
                pdf_paths, text_paths = await asyncio.to_thread(
                    _process_period_sync, period, ct_numbers, self.cache_dir,
                )
                topic_map, summary_map = await asyncio.to_thread(
                    _classify_and_save, period, text_paths, self.cache_dir,
                )
                # Consolidate similar/duplicate topics
                topic_map, summary_map = await asyncio.to_thread(
                    _consolidate_topics, period, self.cache_dir,
                )
                logger.info(
                    "[tisk pipeline] Period {} complete: {} histories, {} PDFs, {} texts, {} topics",
                    period, len(histories), len(pdf_paths), len(text_paths), len(topic_map),
                )
                if on_complete:
                    on_complete(period, text_paths, topic_map, summary_map, histories)
            except Exception:
                logger.opt(exception=True).error(
                    "[tisk pipeline] Failed for period {}, continuing to next", period,
                )
        logger.info("[tisk pipeline] === All periods processed ===")

    async def _run_period(self, period: int, ct_numbers: list[int], on_complete) -> None:
        """Run the full pipeline in a thread to avoid blocking the event loop."""
        try:
            # Scrape legislative history pages (fast, cached)
            histories = await asyncio.to_thread(
                _scrape_histories_sync, period, ct_numbers, self.cache_dir,
            )
            pdf_paths, text_paths = await asyncio.to_thread(
                _process_period_sync, period, ct_numbers, self.cache_dir,
            )
            topic_map, summary_map = await asyncio.to_thread(
                _classify_and_save, period, text_paths, self.cache_dir,
            )
            # Consolidate similar/duplicate topics
            topic_map, summary_map = await asyncio.to_thread(
                _consolidate_topics, period, self.cache_dir,
            )
            logger.info(
                "[tisk pipeline] Period {} complete: {} histories, {} PDFs, {} texts, {} topics",
                period, len(histories), len(pdf_paths), len(text_paths), len(topic_map),
            )
            if on_complete:
                on_complete(period, text_paths, topic_map, summary_map, histories)
        except Exception:
            logger.opt(exception=True).error("[tisk pipeline] Failed for period {}", period)

    def is_running(self, period: int) -> bool:
        task = self._tasks.get(period)
        return task is not None and not task.done()

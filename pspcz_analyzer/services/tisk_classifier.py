"""Topic classification and consolidation for parliamentary prints."""

from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import TISKY_META_DIR
from pspcz_analyzer.services.ollama_service import (
    OllamaClient,
    deserialize_topics,
    serialize_topics,
)
from pspcz_analyzer.services.topic_service import classify_tisk_primary_label


def classify_and_save(
    period: int,
    text_paths: dict[int, Path],
    cache_dir: Path,
) -> tuple[dict[int, list[str]], dict[int, str]]:
    """Run topic classification on extracted texts, save parquet, return maps.

    Uses Ollama AI when available (free-form topics), falls back to keyword matching.
    Saves incrementally after each tisk and resumes from where it left off.
    Returns (topic_map, summary_map).
    """
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
            already,
            len(remaining),
            total,
        )

    if use_ai:
        logger.info(
            "[tisk pipeline] Ollama available, using AI classification + summarization ({} to process)",
            len(remaining),
        )
    else:
        logger.info(
            "[tisk pipeline] Ollama not available, using keyword classification ({} to process)",
            len(remaining),
        )

    # Start from existing records
    records = list(existing.values())

    for i, (ct, text_path) in enumerate(sorted(remaining.items()), already + 1):
        record = _classify_single_tisk(ct, text_path, ollama, use_ai, i, total)
        records.append(record)

        # Save after every tisk so progress is never lost
        df = pl.DataFrame(records)
        df.write_parquet(parquet_path)

    # Build return maps from all records (existing + new)
    return _build_topic_summary_maps(records, period)


def _classify_single_tisk(
    ct: int,
    text_path: Path,
    ollama: OllamaClient,
    use_ai: bool,
    i: int,
    total: int,
) -> dict:
    """Classify a single tisk and return its record dict."""
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
            i,
            total,
            ct,
            topics or "(none)",
            len(summary),
            source,
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
            logger.debug(
                "[tisk pipeline] tisk ct={} AI returned no topics, keyword fallback -> {}",
                ct,
                kw_topic,
            )

    return {
        "ct": ct,
        "topic": serialize_topics(topics),
        "summary": summary,
        "source": source,
    }


def consolidate_topics(
    period: int,
    cache_dir: Path,
) -> tuple[dict[int, list[str]], dict[int, str]]:
    """Run LLM-powered topic deduplication after classification.

    Reads the parquet, collects all unique topic labels, asks the LLM to
    consolidate similar ones, applies the mapping, and re-writes the parquet.

    Returns updated (topic_map, summary_map).
    """
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
        return _build_topic_summary_maps(records, period, log=False)

    # Collect all unique topic labels
    all_topics: set[str] = set()
    for r in records:
        for t in deserialize_topics(r.get("topic", "")):
            all_topics.add(t)

    unique_topics = sorted(all_topics)

    if len(unique_topics) <= 10:
        logger.info(
            "[tisk pipeline] Only {} unique topics for period {}, skipping consolidation",
            len(unique_topics),
            period,
        )
        consolidated_marker.touch()
        return _build_topic_summary_maps(records, period, log=False)

    ollama = OllamaClient()
    if not ollama.is_available():
        logger.info("[tisk pipeline] Ollama not available, skipping topic consolidation")
        return _build_topic_summary_maps(records, period, log=False)

    logger.info(
        "[tisk pipeline] Consolidating topics for period {}: {} unique topics",
        period,
        len(unique_topics),
    )
    mapping = ollama.consolidate_topics(unique_topics)

    # Count how many actually changed
    changed = sum(1 for old, new in mapping.items() if old != new)
    canonical = len(set(mapping.values()))
    logger.info(
        "[tisk pipeline] Consolidating topics for period {}: {} unique -> {} canonical ({} remapped)",
        period,
        len(unique_topics),
        canonical,
        changed,
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

    return _build_topic_summary_maps(records, period)


def _build_topic_summary_maps(
    records: list[dict],
    period: int,
    log: bool = True,
) -> tuple[dict[int, list[str]], dict[int, str]]:
    """Build topic and summary maps from classification records."""
    topic_map: dict[int, list[str]] = {}
    summary_map: dict[int, str] = {}
    for r in records:
        parsed = deserialize_topics(r.get("topic", ""))
        if parsed:
            topic_map[r["ct"]] = parsed
        if r.get("summary"):
            summary_map[r["ct"]] = r["summary"]

    if log:
        classified = len(topic_map)
        ai_count = sum(1 for r in records if r.get("source", "").startswith("ollama"))
        logger.info(
            "[tisk pipeline] Classified {}/{} tisky for period {} (AI: {}, keyword: {})",
            classified,
            len(records),
            period,
            ai_count,
            classified - ai_count,
        )

    return topic_map, summary_map

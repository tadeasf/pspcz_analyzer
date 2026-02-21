"""Topic classification and consolidation for parliamentary prints."""

from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import TISKY_META_DIR
from pspcz_analyzer.services.ollama_service import (
    OllamaClient,
    OpenAIClient,
    create_llm_client,
    deserialize_topics,
    serialize_topics,
)
from pspcz_analyzer.services.topic_service import classify_tisk_primary_label


def classify_and_save(
    period: int,
    text_paths: dict[int, Path],
    cache_dir: Path,
) -> tuple[dict[int, list[str]], dict[int, str], dict[int, str]]:
    """Run topic classification on extracted texts, save parquet, return maps.

    Uses Ollama AI when available (free-form topics), falls back to keyword matching.
    Saves incrementally after each tisk and resumes from where it left off.
    Returns (topic_map, summary_map, summary_en_map).
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

    llm = create_llm_client()
    use_ai = llm.is_available()
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
            "[tisk pipeline] LLM available ({}), using AI classification + summarization ({} to process)",
            llm.model,
            len(remaining),
        )
    else:
        logger.info(
            "[tisk pipeline] LLM not available, using keyword classification ({} to process)",
            len(remaining),
        )

    # Start from existing records
    records = list(existing.values())

    for i, (ct, text_path) in enumerate(sorted(remaining.items()), already + 1):
        record = _classify_single_tisk(ct, text_path, llm, use_ai, i, total)
        records.append(record)

        # Save after every tisk so progress is never lost
        df = pl.DataFrame(records)
        df.write_parquet(parquet_path)

    # Build return maps from all records (existing + new)
    return _build_topic_summary_maps(records, period)


def _classify_single_tisk(
    ct: int,
    text_path: Path,
    llm: OllamaClient | OpenAIClient,
    use_ai: bool,
    i: int,
    total: int,
) -> dict:
    """Classify a single tisk and return its record dict."""
    text = text_path.read_text(encoding="utf-8")
    topics: list[str] = []
    topics_en: list[str] = []
    summary = ""
    source = "keyword"

    summary_en = ""
    if use_ai:
        logger.info(
            "[tisk pipeline] [{}/{}] AI classifying tisk ct={} (bilingual) ...", i, total, ct
        )
        topics, topics_en = llm.classify_topics_bilingual(text, "")
        if topics:
            source = f"llm:{llm.model}"
        logger.info(
            "[tisk pipeline] [{}/{}] AI summarizing tisk ct={} (bilingual) ...", i, total, ct
        )
        summaries = llm.summarize_bilingual(text, "")
        summary = summaries["cs"]
        summary_en = summaries["en"]
        logger.info(
            "[tisk pipeline] [{}/{}] tisk ct={} -> topics={} topics_en={} summary={}chars summary_en={}chars ({})",
            i,
            total,
            ct,
            topics or "(none)",
            topics_en or "(none)",
            len(summary),
            len(summary_en),
            source,
        )
    else:
        kw_result = classify_tisk_primary_label(text, "")
        if kw_result:
            topics = [kw_result[0]]
            topics_en = [kw_result[1]]
        if i % 20 == 0 or i == total:
            logger.info("[tisk pipeline] [{}/{}] keyword classification progress", i, total)

    # Fall back to keyword classification if AI didn't produce topics
    if use_ai and not topics:
        kw_result = classify_tisk_primary_label(text, "")
        if kw_result:
            topics = [kw_result[0]]
            topics_en = [kw_result[1]]
            source = "keyword"
            logger.debug(
                "[tisk pipeline] tisk ct={} AI returned no topics, keyword fallback -> {}",
                ct,
                kw_result[0],
            )

    return {
        "ct": ct,
        "topic": serialize_topics(topics),
        "topic_en": serialize_topics(topics_en),
        "summary": summary,
        "summary_en": summary_en,
        "source": source,
    }


def consolidate_topics(
    period: int,
    cache_dir: Path,
) -> tuple[dict[int, list[str]], dict[int, str], dict[int, str]]:
    """Run LLM-powered topic deduplication after classification.

    Reads the parquet, collects all unique topic labels, asks the LLM to
    consolidate similar ones, applies the mapping, and re-writes the parquet.

    Returns updated (topic_map, summary_map, summary_en_map).
    """
    meta_dir = cache_dir / TISKY_META_DIR / str(period)
    parquet_path = meta_dir / "topic_classifications.parquet"
    consolidated_marker = meta_dir / "topics_consolidated.done"

    if not parquet_path.exists():
        logger.warning("[tisk pipeline] No parquet to consolidate for period {}", period)
        return {}, {}, {}

    df = pl.read_parquet(parquet_path)
    records = df.to_dicts()

    # If consolidation was already done, just return the maps from the parquet
    if consolidated_marker.exists():
        logger.info(
            "[tisk pipeline] Topics already consolidated for period {}, skipping",
            period,
        )
        return _build_topic_summary_maps(records, period, log=False)

    # Collect all unique topic labels (Czech and English)
    all_topics_cs: set[str] = set()
    all_topics_en: set[str] = set()
    for r in records:
        for t in deserialize_topics(r.get("topic", "")):
            all_topics_cs.add(t)
        for t in deserialize_topics(r.get("topic_en", "")):
            all_topics_en.add(t)

    unique_topics_cs = sorted(all_topics_cs)
    unique_topics_en = sorted(all_topics_en)

    if len(unique_topics_cs) <= 10 and len(unique_topics_en) <= 10:
        logger.info(
            "[tisk pipeline] Only {} CS + {} EN unique topics for period {}, skipping consolidation",
            len(unique_topics_cs),
            len(unique_topics_en),
            period,
        )
        consolidated_marker.touch()
        return _build_topic_summary_maps(records, period, log=False)

    llm = create_llm_client()
    if not llm.is_available():
        logger.info("[tisk pipeline] LLM not available, skipping topic consolidation")
        return _build_topic_summary_maps(records, period, log=False)

    logger.info(
        "[tisk pipeline] Consolidating topics for period {}: {} CS + {} EN unique topics",
        period,
        len(unique_topics_cs),
        len(unique_topics_en),
    )
    mapping_cs, mapping_en = llm.consolidate_topics_bilingual(unique_topics_cs, unique_topics_en)

    # Count how many actually changed
    changed_cs = sum(1 for old, new in mapping_cs.items() if old != new)
    changed_en = sum(1 for old, new in mapping_en.items() if old != new)
    logger.info(
        "[tisk pipeline] Consolidated topics for period {}: CS {} -> {} ({} remapped), EN {} -> {} ({} remapped)",
        period,
        len(unique_topics_cs),
        len(set(mapping_cs.values())),
        changed_cs,
        len(unique_topics_en),
        len(set(mapping_en.values())),
        changed_en,
    )

    # Apply mappings to all records
    for r in records:
        old_topics = deserialize_topics(r.get("topic", ""))
        new_topics = _apply_topic_mapping(old_topics, mapping_cs)
        r["topic"] = serialize_topics(new_topics)

        old_topics_en = deserialize_topics(r.get("topic_en", ""))
        new_topics_en = _apply_topic_mapping(old_topics_en, mapping_en)
        r["topic_en"] = serialize_topics(new_topics_en)

    # Re-write parquet
    df = pl.DataFrame(records)
    df.write_parquet(parquet_path)

    # Write marker so we don't re-consolidate on next startup
    consolidated_marker.touch()

    return _build_topic_summary_maps(records, period)


def _apply_topic_mapping(topics: list[str], mapping: dict[str, str]) -> list[str]:
    """Apply a consolidation mapping to a list of topics, deduplicating."""
    new_topics = [mapping.get(t, t) for t in topics]
    seen: set[str] = set()
    deduped: list[str] = []
    for t in new_topics:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped


def _build_topic_summary_maps(
    records: list[dict],
    period: int,
    log: bool = True,
) -> tuple[dict[int, list[str]], dict[int, str], dict[int, str]]:
    """Build topic, summary, and summary_en maps from classification records.

    Also populates the topic_en_map on the module-level _topic_en_maps dict
    so it can be retrieved by the cache manager.
    """
    topic_map: dict[int, list[str]] = {}
    topic_en_map: dict[int, list[str]] = {}
    summary_map: dict[int, str] = {}
    summary_en_map: dict[int, str] = {}
    for r in records:
        parsed = deserialize_topics(r.get("topic", ""))
        if parsed:
            topic_map[r["ct"]] = parsed
        parsed_en = deserialize_topics(r.get("topic_en", ""))
        if parsed_en:
            topic_en_map[r["ct"]] = parsed_en
        if r.get("summary"):
            summary_map[r["ct"]] = r["summary"]
        if r.get("summary_en"):
            summary_en_map[r["ct"]] = r["summary_en"]

    # Store English topic map for retrieval by cache manager
    _topic_en_maps[period] = topic_en_map

    if log:
        classified = len(topic_map)
        ai_count = sum(
            1 for r in records
            if r.get("source", "").startswith(("ollama", "llm:"))
        )
        logger.info(
            "[tisk pipeline] Classified {}/{} tisky for period {} (AI: {}, keyword: {})",
            classified,
            len(records),
            period,
            ai_count,
            classified - ai_count,
        )

    return topic_map, summary_map, summary_en_map


# Module-level store for English topic maps (populated by _build_topic_summary_maps)
_topic_en_maps: dict[int, dict[int, list[str]]] = {}


def get_topic_en_map(period: int) -> dict[int, list[str]]:
    """Get the English topic map for a period (populated during classify_and_save)."""
    return _topic_en_maps.get(period, {})

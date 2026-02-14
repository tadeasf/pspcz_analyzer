"""Parquet caching layer for parsed DataFrames."""

from collections.abc import Callable
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import DEFAULT_CACHE_DIR, PARQUET_DIR


def _parquet_dir(cache_dir: Path) -> Path:
    d = cache_dir / PARQUET_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_or_parse(
    table_name: str,
    source_path: Path,
    parse_fn: Callable[[], pl.DataFrame],
    cache_dir: Path = DEFAULT_CACHE_DIR,
) -> pl.DataFrame:
    """Load from parquet cache if fresh, otherwise parse and cache.

    Args:
        table_name: Used as the parquet filename (e.g. "hl_hlasovani_9").
        source_path: The source file/dir whose mtime determines cache staleness.
        parse_fn: Called to produce the DataFrame if cache is stale.
        cache_dir: Root cache directory.
    """
    parquet_path = _parquet_dir(cache_dir) / f"{table_name}.parquet"

    if parquet_path.exists() and source_path.exists():
        if parquet_path.stat().st_mtime > source_path.stat().st_mtime:
            logger.info("Loading {} from parquet cache", table_name)
            return pl.read_parquet(parquet_path)

    logger.info("Parsing {} (cache miss or stale)", table_name)
    df = parse_fn()
    df.write_parquet(parquet_path)
    logger.info("Cached {} ({} rows)", table_name, df.height)
    return df

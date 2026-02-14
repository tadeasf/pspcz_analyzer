"""CLI to download tisk PDFs and extract text for a parliamentary period.

Usage:
    uv run python -m pspcz_analyzer.cli.fetch_tisky --period 9
    uv run python -m pspcz_analyzer.cli.fetch_tisky --period 9 --limit 5 --force
    uv run python -m pspcz_analyzer.cli.fetch_tisky --period 9 --download-only
"""

import argparse
import sys
from pathlib import Path

import polars as pl
from loguru import logger

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    PERIOD_ORGAN_IDS,
    PERIOD_YEARS,
    TISKY_META_DIR,
)
from pspcz_analyzer.data.cache import get_or_parse
from pspcz_analyzer.data.downloader import download_tisky_data
from pspcz_analyzer.data.parser import parse_unl
from pspcz_analyzer.data.tisk_downloader import download_period_tisky
from pspcz_analyzer.data.tisk_extractor import extract_period_texts
from pspcz_analyzer.models.schemas import TISKY_COLUMNS, TISKY_DTYPES


def _find_file(directory: Path, filename: str) -> Path:
    """Find a file in directory tree (case-insensitive)."""
    for f in directory.rglob(filename):
        return f
    for f in directory.rglob("*"):
        if f.name.lower() == filename.lower():
            return f
    msg = f"File {filename} not found in {directory}"
    raise FileNotFoundError(msg)


def _load_tisk_numbers(period: int, cache_dir: Path) -> list[int]:
    """Load tisky.unl and return ct numbers for the given period."""
    tisky_dir = download_tisky_data(cache_dir)
    tisky = get_or_parse(
        "tisky", tisky_dir,
        lambda: parse_unl(
            _find_file(tisky_dir, "tisky.unl"),
            TISKY_COLUMNS, TISKY_DTYPES,
        ),
        cache_dir,
    )

    organ_id = PERIOD_ORGAN_IDS[period]
    period_tisky = tisky.filter(
        (pl.col("id_obdobi") == organ_id) & pl.col("ct").is_not_null()
    )
    ct_numbers = sorted(period_tisky.get_column("ct").unique().to_list())
    logger.info("Period {} (organ {}): {} unique tisky", period, organ_id, len(ct_numbers))
    return ct_numbers


def _run_classification(period: int, text_paths: dict[int, Path], cache_dir: Path) -> None:
    """Run topic classification on extracted texts and save results."""
    # Deferred import — topic_service may not exist yet during Phase 2
    try:
        from pspcz_analyzer.services.topic_service import classify_tisk_primary
    except ImportError:
        logger.info("topic_service not available, skipping classification")
        return

    meta_dir = cache_dir / TISKY_META_DIR / str(period)
    meta_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for ct, text_path in sorted(text_paths.items()):
        text = text_path.read_text(encoding="utf-8")
        topic = classify_tisk_primary(text, "")
        records.append({"ct": ct, "topic": topic or ""})

    if records:
        df = pl.DataFrame(records)
        out = meta_dir / "topic_classifications.parquet"
        df.write_parquet(out)
        classified = sum(1 for r in records if r["topic"])
        logger.info(
            "Classified {}/{} tisky for period {}, saved to {}",
            classified, len(records), period, out,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download tisk PDFs and extract text for a parliamentary period.",
    )
    parser.add_argument(
        "--period", type=int, required=True,
        help=f"Electoral period number ({', '.join(f'{k} ({v})' for k, v in sorted(PERIOD_YEARS.items()))})",
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=DEFAULT_CACHE_DIR,
        help=f"Cache directory (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument("--force", action="store_true", help="Re-download and re-extract everything")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tisky to process (0 = all)")
    parser.add_argument("--download-only", action="store_true", help="Only download PDFs, skip text extraction")

    args = parser.parse_args()

    if args.period not in PERIOD_YEARS:
        logger.error("Unknown period {}. Available: {}", args.period, list(PERIOD_YEARS.keys()))
        sys.exit(1)

    logger.info("Fetching tisky for period {} ({})", args.period, PERIOD_YEARS[args.period])

    # 1. Get tisk numbers from open data
    ct_numbers = _load_tisk_numbers(args.period, args.cache_dir)
    if args.limit > 0:
        ct_numbers = ct_numbers[:args.limit]
        logger.info("Limited to {} tisky", len(ct_numbers))

    # 2. Download PDFs
    pdf_paths = download_period_tisky(args.period, ct_numbers, args.cache_dir, args.force)

    if args.download_only:
        logger.info("Download-only mode, skipping extraction")
        return

    # 3. Extract text
    text_paths = extract_period_texts(args.period, pdf_paths, args.cache_dir, args.force)

    # 4. Run topic classification if available
    _run_classification(args.period, text_paths, args.cache_dir)

    logger.info("Done! {} PDFs downloaded, {} texts extracted", len(pdf_paths), len(text_paths))


if __name__ == "__main__":
    main()

"""Build a fully-enriched cache for a set of periods, then back it up.

Runs the complete processing + enrichment pipeline (tisk FULL + amendment
FULL) for each requested electoral period, then packs the whole cache
directory into a single ``.tar.gz`` archive. Ship that archive to the
production server and extract it over its cache dir to deploy a new
version without re-downloading or re-processing anything.

Per period the script runs, in order:

  1. Download raw data (voting UNL for the period + shared poslanci /
     schuze / tisky ZIPs). Idempotent — already-present files are skipped
     unless ``--force-download`` is passed. This guarantees the archive
     contains the raw sources, not just derived artefacts.
  2. Tisk pipeline in FULL mode (PDF download + text extraction + LLM topic
     classification + bilingual summaries + version diffs + histories).
  3. Amendment pipeline in FULL mode (PDF + stenographic parsing + vote /
     submitter resolution + LLM summaries). This is auto-chained by the
     tisk pipeline's completion callback; the script also starts it
     explicitly if the chain did not fire, so it always runs.

Both pipelines expose their ``asyncio.Task`` via ``get_task(period)``, so
the script awaits each to completion before moving on. Periods are
processed sequentially (one event loop) to keep LLM / network load and the
log readable.

The archive stores paths *relative to the cache directory*, so on the
production host you can extract straight into whatever ``PSPCZ_CACHE_DIR``
points at, regardless of the directory name::

    # on the build machine
    uv run python scripts/build_and_backup_cache.py

    # copy pspcz-cache-*.tar.gz to prod, then (app stopped):
    tar xzf pspcz-cache-*.tar.gz -C "$PSPCZ_CACHE_DIR"

Usage::

    uv run python scripts/build_and_backup_cache.py
    uv run python scripts/build_and_backup_cache.py --periods 10 9 8
    uv run python scripts/build_and_backup_cache.py --no-backup
    nohup uv run python scripts/build_and_backup_cache.py > build.log 2>&1 &
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path

# Ensure the project root is on sys.path when running as a standalone script.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from loguru import logger  # noqa: E402

from pspcz_analyzer.config import DEFAULT_CACHE_DIR  # noqa: E402
from pspcz_analyzer.data.downloader import (  # noqa: E402
    download_poslanci_data,
    download_schuze_data,
    download_tisky_data,
    download_voting_data,
)
from pspcz_analyzer.logging_config import setup_logging  # noqa: E402
from pspcz_analyzer.models.pipeline_progress import AmendmentMode, TiskMode  # noqa: E402
from pspcz_analyzer.services.data_service import DataService  # noqa: E402

DEFAULT_PERIODS: tuple[int, ...] = (10, 9, 8, 7, 6, 5)


@dataclass
class PeriodResult:
    """Outcome of processing a single period."""

    period: int
    tisk_started: bool
    tisk_finished: bool
    amendment_status: str


def _download_raw(cache_dir: Path, period: int, *, force: bool) -> None:
    """Download raw sources for *period* plus the shared tables.

    All calls are idempotent: existing files are reused unless *force*.
    """
    logger.info("[download] Shared tables (poslanci/schuze/tisky)…")
    download_poslanci_data(cache_dir, force=force)
    download_schuze_data(cache_dir, force=force)
    download_tisky_data(cache_dir, force=force)
    logger.info("[download] Voting data for period {}…", period)
    download_voting_data(period, cache_dir, force=force)


async def _process_period(svc: DataService, period: int) -> PeriodResult:
    """Run tisk FULL then amendment FULL for *period*, awaiting both.

    Returns:
        A :class:`PeriodResult` summarising what ran.
    """
    logger.info("=== Period {}: starting tisk pipeline (FULL) ===", period)
    tisk_started = svc.start_tisk_pipeline(period, mode=TiskMode.FULL)
    if not tisk_started:
        logger.warning(
            "[period {}] Tisk pipeline did not start (missing tisky table or "
            "no ct numbers) — skipping enrichment",
            period,
        )
        return PeriodResult(period, False, False, "skipped")

    tisk_task = svc.tisk_pipeline.get_task(period)
    if tisk_task is not None:
        await tisk_task

    # The tisk completion callback normally chains the amendment pipeline.
    # Give it a beat, then start it ourselves if the chain did not fire so
    # enrichment always runs.
    await asyncio.sleep(0.5)
    amend_task = svc.amendment_pipeline.get_task(period)
    if amend_task is None:
        logger.info("[period {}] Chained amendment task absent — starting explicitly", period)
        if svc.start_amendment_pipeline(period, mode=AmendmentMode.FULL):
            amend_task = svc.amendment_pipeline.get_task(period)
    if amend_task is not None:
        await amend_task

    tisk_finished = tisk_task is not None and tisk_task.done()
    amend_prog = svc.amendment_pipeline.progress.get(period)
    if amend_prog is None:
        amend_status = "unknown"
    else:
        status = amend_prog.status
        amend_status = status.value if hasattr(status, "value") else str(status)
    logger.info(
        "=== Period {} done (tisk_finished={}, amendment={}) ===",
        period,
        tisk_finished,
        amend_status,
    )
    return PeriodResult(period, True, tisk_finished, amend_status)


async def _run_all(
    svc: DataService,
    periods: list[int],
    *,
    skip_download: bool,
    force_download: bool,
) -> list[PeriodResult]:
    """Process every period sequentially on a single event loop.

    Args:
        svc: Initialised data service.
        periods: Periods to process, in order.
        skip_download: When True, reuse cached raw sources.
        force_download: When True, re-download raw sources unconditionally.

    Returns:
        One :class:`PeriodResult` per period.
    """
    results: list[PeriodResult] = []
    for period in periods:
        if skip_download:
            logger.info("[period {}] --skip-download set, using cached raw data", period)
        else:
            await asyncio.to_thread(_download_raw, svc.cache_dir, period, force=force_download)
        try:
            results.append(await _process_period(svc, period))
        except Exception:
            logger.opt(exception=True).error("[period {}] Processing failed", period)
            results.append(PeriodResult(period, False, False, "error"))
    return results


def _make_backup(cache_dir: Path, out_path: Path) -> Path:
    """Pack *cache_dir* contents (relative paths) into a gzipped tarball.

    Args:
        cache_dir: Resolved cache directory to archive.
        out_path: Destination ``.tar.gz`` path.

    Returns:
        The resolved archive path.
    """
    cache_dir = cache_dir.resolve()
    out_path = out_path.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(p for p in cache_dir.rglob("*") if p.is_file())
    logger.info("[backup] Writing {} files from {} → {}", len(files), cache_dir, out_path)

    done = 0
    with tarfile.open(out_path, "w:gz") as tar:
        for path in files:
            tar.add(path, arcname=str(path.relative_to(cache_dir)))
            done += 1
            if done % 500 == 0:
                logger.info("[backup] …archived {} files", done)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    logger.info("[backup] Done — {} files, {:.1f} MB", done, size_mb)
    return out_path


def _print_summary(results: list[PeriodResult]) -> None:
    """Log a compact per-period outcome table."""
    logger.info("─────────────────────────────────────────────")
    logger.info("Build summary")
    logger.info(" period │ tisk        │ amendment")
    logger.info("────────┼─────────────┼────────────")
    for result in results:
        if result.tisk_finished:
            tisk = "finished"
        elif result.tisk_started:
            tisk = "started"
        else:
            tisk = "skipped"
        logger.info("  {:>4}  │ {:<11} │ {}", result.period, tisk, result.amendment_status)
    logger.info("─────────────────────────────────────────────")


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Build a fully-enriched cache for several periods and back it up.",
    )
    parser.add_argument(
        "--periods",
        nargs="+",
        type=int,
        default=list(DEFAULT_PERIODS),
        metavar="N",
        help=f"Electoral periods to process, newest first (default: {list(DEFAULT_PERIODS)}).",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Cache directory to build and archive (default: {DEFAULT_CACHE_DIR}).",
    )
    parser.add_argument(
        "--backup-out",
        type=Path,
        default=None,
        help="Output archive path (default: ./pspcz-cache-<timestamp>.tar.gz).",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Run the pipelines but skip creating the backup archive.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Do not download raw sources (use whatever is already cached).",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download raw sources even if already present.",
    )
    return parser


def main() -> int:
    """Entry point: build each period, then back up the cache directory."""
    setup_logging()
    args = _build_arg_parser().parse_args()

    cache_dir: Path = args.cache_dir
    periods: list[int] = args.periods
    logger.info("Building enriched cache for periods {} in {}", periods, cache_dir)

    svc = DataService(cache_dir)
    svc.initialize(period=periods[0])

    results = asyncio.run(
        _run_all(
            svc,
            periods,
            skip_download=args.skip_download,
            force_download=args.force_download,
        )
    )
    _print_summary(results)

    if args.no_backup:
        logger.info("--no-backup set; skipping archive creation.")
        return 0

    out_path: Path = args.backup_out or Path(f"pspcz-cache-{time.strftime('%Y%m%d-%H%M%S')}.tar.gz")
    archive = _make_backup(cache_dir, out_path)
    logger.info("Backup ready: {}", archive)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

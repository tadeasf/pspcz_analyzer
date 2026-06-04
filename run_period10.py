"""Throwaway runner: drive the full tisk + amendment pipeline for one period.

Mirrors the backend lifespan init (DataService + runtime config), then runs the
FULL tisk pipeline for the period and waits for it to finish. The tisk pipeline's
on-complete callback chains the amendment pipeline, so we wait for that too.

Usage: uv run python run_period10.py
"""

import asyncio

from loguru import logger

from pspcz_analyzer.logging_config import setup_logging
from pspcz_analyzer.models.pipeline_progress import TiskMode
from pspcz_analyzer.services.data_service import DataService
from pspcz_analyzer.services.runtime_config import (
    apply_runtime_config,
    load_runtime_config,
)

PERIOD = 10


async def main() -> None:
    """Initialize the data service and run both pipelines to completion."""
    setup_logging()
    svc = DataService()
    apply_runtime_config(load_runtime_config(svc.cache_dir))
    svc.initialize(period=PERIOD)
    logger.info("Initialized; starting FULL tisk pipeline for period {}", PERIOD)

    if not svc.start_tisk_pipeline(PERIOD, mode=TiskMode.FULL):
        logger.error("Failed to start tisk pipeline for period {}", PERIOD)
        return

    # Give the background task a moment to register, then wait it out.
    await asyncio.sleep(2)
    while svc.tisk_pipeline.is_running(PERIOD):
        await asyncio.sleep(5)
    logger.info("Tisk pipeline finished; waiting for chained amendment pipeline ...")

    # The amendment pipeline is started inside the tisk on-complete callback.
    await asyncio.sleep(3)
    while svc.amendment_pipeline.is_running(PERIOD):
        await asyncio.sleep(5)

    logger.info("All pipelines complete for period {}", PERIOD)


if __name__ == "__main__":
    asyncio.run(main())

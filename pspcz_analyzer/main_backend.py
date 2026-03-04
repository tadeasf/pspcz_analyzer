"""Backend entrypoint — admin dashboard with pipeline management."""

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from loguru import logger

from pspcz_analyzer.admin.auth import AdminAuthMiddleware
from pspcz_analyzer.admin.log_stream import log_broadcaster
from pspcz_analyzer.admin.routes import router as admin_router
from pspcz_analyzer.config import ADMIN_PORT, DEFAULT_PERIOD
from pspcz_analyzer.logging_config import setup_logging
from pspcz_analyzer.services.daily_refresh_service import DailyRefreshService
from pspcz_analyzer.services.data_service import DataService
from pspcz_analyzer.services.runtime_config import (
    apply_runtime_config,
    load_runtime_config,
)

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize pipeline data service, log broadcaster, and daily refresh."""
    # Load and apply runtime config overrides
    svc = DataService()
    runtime_config = load_runtime_config(svc.cache_dir)
    apply_runtime_config(runtime_config)

    svc.initialize(period=DEFAULT_PERIOD)
    app.state.data = svc
    logger.info("Backend data service initialized.")

    # Start log broadcaster
    log_broadcaster.start()

    # Start background tisk pipeline for all periods (newest first)
    svc.start_all_tisk_pipelines()

    # Start daily data refresh scheduler
    refresh_svc = DailyRefreshService(svc)
    refresh_svc.start()
    app.state.refresh = refresh_svc

    yield

    # Graceful shutdown
    await refresh_svc.stop()
    await svc.tisk_pipeline.cancel_all()
    log_broadcaster.stop()


app = FastAPI(
    title="PSP.cz Analyzer — Admin Backend",
    description="Pipeline management and admin dashboard",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

# Admin auth: IP whitelist + session
app.add_middleware(AdminAuthMiddleware)

# Mount admin routes
app.include_router(admin_router)


def main() -> None:
    """Run the backend admin server."""
    dev_mode = os.environ.get("PSPCZ_DEV", "1") == "1"
    uvicorn.run(
        "pspcz_analyzer.main_backend:app",
        host="0.0.0.0",
        port=ADMIN_PORT,
        reload=dev_mode,
    )


if __name__ == "__main__":
    main()

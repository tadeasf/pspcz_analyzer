"""Czech Parliamentary Voting Analyzer — FastAPI application."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import markdown as _md
import markupsafe
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.i18n import setup_jinja2_i18n
from pspcz_analyzer.i18n.middleware import LocaleMiddleware
from pspcz_analyzer.logging_config import setup_logging
from pspcz_analyzer.middleware import SecurityHeadersMiddleware
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.api import router as api_router
from pspcz_analyzer.routes.api import templates as api_templates
from pspcz_analyzer.routes.charts import router as charts_router
from pspcz_analyzer.routes.pages import router as pages_router
from pspcz_analyzer.routes.pages import templates as pages_templates
from pspcz_analyzer.services.daily_refresh_service import DailyRefreshService
from pspcz_analyzer.services.data_service import DataService

setup_logging()

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    svc = DataService()
    svc.initialize(period=DEFAULT_PERIOD)
    app.state.data = svc
    logger.info("Data service initialized, server ready.")

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


app = FastAPI(
    title="Czech Parliamentary Voting Analyzer",
    description="OSINT tool for analyzing open voting data from the Czech Chamber of Deputies (psp.cz)",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

# Security: rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

# Security: response headers
app.add_middleware(SecurityHeadersMiddleware)

# i18n: per-request locale from cookie
app.add_middleware(LocaleMiddleware)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Mount static files
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(pages_router)
app.include_router(api_router, prefix="/api")
app.include_router(charts_router, prefix="/charts")


# Register shared Jinja2 filters on all template instances
def _md_filter(text: str) -> markupsafe.Markup:
    """Convert markdown to HTML, safe for Jinja2 rendering."""
    if not text:
        return markupsafe.Markup("")
    html = _md.markdown(text, extensions=["nl2br"])
    return markupsafe.Markup(html)


for t in (templates, api_templates, pages_templates):
    t.env.filters["markdown"] = _md_filter
    setup_jinja2_i18n(t.env)


def main() -> None:
    dev_mode = os.environ.get("PSPCZ_DEV", "1") == "1"
    uvicorn.run(
        "pspcz_analyzer.main:app",
        host="0.0.0.0",
        port=8000,
        reload=dev_mode,
    )


if __name__ == "__main__":
    main()

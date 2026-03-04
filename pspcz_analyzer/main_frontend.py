"""Frontend entrypoint — public web app with read-only data access."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import markdown as _md
import markupsafe
import nh3
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from pspcz_analyzer.config import DEFAULT_PERIOD, PORT
from pspcz_analyzer.i18n import setup_jinja2_i18n
from pspcz_analyzer.i18n.middleware import LocaleMiddleware
from pspcz_analyzer.logging_config import setup_logging
from pspcz_analyzer.middleware import SecurityHeadersMiddleware
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.amendments import router as amendments_router
from pspcz_analyzer.routes.amendments import templates as amendments_templates
from pspcz_analyzer.routes.charts import router as charts_router
from pspcz_analyzer.routes.feedback import router as feedback_router
from pspcz_analyzer.routes.feedback import templates as feedback_templates
from pspcz_analyzer.routes.health import router as health_router
from pspcz_analyzer.routes.laws import router as laws_router
from pspcz_analyzer.routes.laws import templates as laws_templates
from pspcz_analyzer.routes.pages import router as pages_router
from pspcz_analyzer.routes.pages import templates as pages_templates
from pspcz_analyzer.routes.tisk import router as tisk_router
from pspcz_analyzer.routes.tisk import templates as tisk_templates
from pspcz_analyzer.routes.voting import router as voting_router
from pspcz_analyzer.routes.voting import templates as voting_templates
from pspcz_analyzer.services.data_reader import DataReader

setup_logging()

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize read-only data service and file watcher."""
    svc = DataReader()
    svc.initialize(period=DEFAULT_PERIOD)
    app.state.data = svc
    logger.info("Frontend data service initialized, server ready.")

    # Start file watcher to detect backend pipeline outputs
    svc.start_watcher()

    yield

    # Graceful shutdown
    await svc.stop_watcher()


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
app.include_router(voting_router, prefix="/api")
app.include_router(amendments_router, prefix="/api")
app.include_router(laws_router, prefix="/api")
app.include_router(tisk_router, prefix="/api")
app.include_router(feedback_router, prefix="/api")
app.include_router(health_router, prefix="/api")
app.include_router(charts_router, prefix="/charts")


# Register shared Jinja2 filters on all template instances
def _md_filter(text: str) -> markupsafe.Markup:
    """Convert markdown to HTML, sanitized for safe Jinja2 rendering."""
    if not text:
        return markupsafe.Markup("")
    raw_html = _md.markdown(text, extensions=["nl2br"])
    safe_html = nh3.clean(raw_html)
    return markupsafe.Markup(safe_html)


for t in (
    templates,
    pages_templates,
    voting_templates,
    amendments_templates,
    laws_templates,
    tisk_templates,
    feedback_templates,
):
    t.env.filters["markdown"] = _md_filter
    setup_jinja2_i18n(t.env)


def main() -> None:
    """Run the frontend server."""
    dev_mode = os.environ.get("PSPCZ_DEV", "1") == "1"
    uvicorn.run(
        "pspcz_analyzer.main_frontend:app",
        host="0.0.0.0",
        port=PORT,
        reload=dev_mode,
    )


if __name__ == "__main__":
    main()

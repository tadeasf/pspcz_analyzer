"""Czech Parliamentary Voting Analyzer — FastAPI application."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.logging_config import setup_logging
from pspcz_analyzer.routes.api import router as api_router
from pspcz_analyzer.routes.charts import router as charts_router
from pspcz_analyzer.routes.pages import router as pages_router
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
    yield


app = FastAPI(
    title="Czech Parliamentary Voting Analyzer",
    lifespan=lifespan,
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Mount static files
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(pages_router)
app.include_router(api_router, prefix="/api")
app.include_router(charts_router, prefix="/charts")


def main() -> None:
    import uvicorn

    uvicorn.run(
        "pspcz_analyzer.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    main()

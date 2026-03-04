"""HTMX partial endpoints — laws/bills listing."""

from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.utils import validate_period
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.law_service import list_laws

router = APIRouter(tags=["Laws"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/laws", response_class=HTMLResponse)
@limiter.limit("120/minute")
async def laws_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    search: str = Query(default="", max_length=200),
    status_filter: str = Query(default="", alias="status"),
    topic: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1, le=1000),
):
    """Return paginated laws list as an HTMX partial."""
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    key = f"laws:{period}:{search}:{status_filter}:{topic}:{page}:{lang}"
    result = analysis_cache.get_or_compute(
        key,
        lambda: list_laws(
            pd,
            search=search,
            status_filter=status_filter,
            topic_filter=topic,
            page=page,
            lang=lang,
        ),
    )
    return templates.TemplateResponse(
        "partials/laws_list.html",
        {
            "request": request,
            "period": period,
            "search": search,
            "status_filter": status_filter,
            "topic": topic,
            "lang": lang,
            **result,
        },
    )

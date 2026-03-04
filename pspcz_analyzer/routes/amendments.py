"""HTMX partial endpoints — amendments and amendment coalitions."""

from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.utils import validate_period
from pspcz_analyzer.services.amendment_service import list_amendment_bills
from pspcz_analyzer.services.amendments.coalition_service import compute_amendment_coalitions
from pspcz_analyzer.services.analysis_cache import analysis_cache

router = APIRouter(tags=["Amendments"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/amendments", response_class=HTMLResponse)
@limiter.limit("120/minute")
async def amendments_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    search: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1, le=1000),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    key = f"amendments:{period}:{search}:{page}:{lang}"
    result = analysis_cache.get_or_compute(
        key,
        lambda: list_amendment_bills(pd, search=search, page=page),
    )
    return templates.TemplateResponse(
        "partials/amendments_list.html",
        {
            "request": request,
            "period": period,
            "search": search,
            "lang": lang,
            **result,
        },
    )


@router.get("/amendment-coalitions", response_class=HTMLResponse)
@limiter.limit("15/minute")
async def amendment_coalitions_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    key = f"amendment-coalitions:{period}:{lang}"
    result = analysis_cache.get_or_compute(
        key,
        lambda: compute_amendment_coalitions(pd),
    )
    return templates.TemplateResponse(
        "partials/coalition_analysis.html",
        {
            "request": request,
            "period": period,
            "lang": lang,
            **result,
        },
    )

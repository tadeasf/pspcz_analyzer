"""HTMX partial endpoints — voting analysis (loyalty, attendance, similarity, votes)."""

from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.middleware import run_with_timeout
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.utils import validate_period
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.attendance_service import compute_attendance
from pspcz_analyzer.services.loyalty_service import compute_loyalty
from pspcz_analyzer.services.similarity_service import compute_cross_party_similarity
from pspcz_analyzer.services.votes_service import list_votes

router = APIRouter(tags=["Voting Analysis"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/loyalty", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def loyalty_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = Query(default=30, ge=1, le=200),
    party: str = Query(default="", max_length=200),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"loyalty:{period}:{top}:{party}"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            key, lambda: compute_loyalty(pd, top=top, party_filter=party or None)
        ),
        timeout=15.0,
        label="loyalty analysis",
    )
    return templates.TemplateResponse(
        "partials/loyalty_table.html",
        {"request": request, "rows": rows, "lang": getattr(request.state, "lang", "cs")},
    )


@router.get("/attendance", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def attendance_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = Query(default=30, ge=1, le=200),
    sort: str = Query(default="worst", max_length=20),
    party: str = Query(default="", max_length=200),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"attendance:{period}:{top}:{sort}:{party}"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            key, lambda: compute_attendance(pd, top=top, sort=sort, party_filter=party or None)
        ),
        timeout=15.0,
        label="attendance analysis",
    )
    return templates.TemplateResponse(
        "partials/attendance_table.html",
        {"request": request, "rows": rows, "lang": getattr(request.state, "lang", "cs")},
    )


@router.get("/similarity", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def similarity_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = Query(default=20, ge=1, le=200),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"similarity:{period}:{top}"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            key, lambda: compute_cross_party_similarity(pd, top=top)
        ),
        timeout=30.0,
        label="similarity analysis",
    )
    return templates.TemplateResponse(
        "partials/similarity_table.html",
        {"request": request, "rows": rows, "lang": getattr(request.state, "lang", "cs")},
    )


@router.get("/votes", response_class=HTMLResponse)
@limiter.limit("120/minute")
async def votes_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    search: str = Query(default="", max_length=200),
    outcome: str = Query(default="", max_length=20),
    topic: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1, le=1000),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    key = f"votes:{period}:{search}:{outcome}:{topic}:{page}:{lang}"
    result = analysis_cache.get_or_compute(
        key,
        lambda: list_votes(
            pd, search=search, page=page, outcome_filter=outcome, topic_filter=topic, lang=lang
        ),
    )
    return templates.TemplateResponse(
        "partials/votes_list.html",
        {
            "request": request,
            "period": period,
            "search": search,
            "outcome": outcome,
            "topic": topic,
            "lang": lang,
            **result,
        },
    )

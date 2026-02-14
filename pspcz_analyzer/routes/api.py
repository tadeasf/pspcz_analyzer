"""HTMX partial endpoints — return HTML fragments."""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.services.activity_service import compute_activity
from pspcz_analyzer.services.attendance_service import compute_attendance
from pspcz_analyzer.services.loyalty_service import compute_loyalty
from pspcz_analyzer.services.similarity_service import compute_cross_party_similarity
from pspcz_analyzer.services.votes_service import list_votes

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/loyalty")
async def loyalty_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = 30,
    party: str = "",
):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = compute_loyalty(pd, top=top, party_filter=party or None)
    return templates.TemplateResponse(
        "partials/loyalty_table.html",
        {"request": request, "rows": rows},
    )


@router.get("/attendance")
async def attendance_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = 30,
    sort: str = "worst",
):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = compute_attendance(pd, top=top, sort=sort)
    return templates.TemplateResponse(
        "partials/attendance_table.html",
        {"request": request, "rows": rows},
    )


@router.get("/similarity")
async def similarity_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = 20,
):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = compute_cross_party_similarity(pd, top=top)
    return templates.TemplateResponse(
        "partials/similarity_table.html",
        {"request": request, "rows": rows},
    )


@router.get("/active")
async def active_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = 50,
    party: str = "",
):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = compute_activity(pd, top=top, party_filter=party or None)
    return templates.TemplateResponse(
        "partials/active_table.html",
        {"request": request, "rows": rows},
    )


@router.get("/votes")
async def votes_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    search: str = "",
    outcome: str = "",
    page: int = 1,
):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    result = list_votes(pd, search=search, page=page, outcome_filter=outcome)
    return templates.TemplateResponse(
        "partials/votes_list.html",
        {
            "request": request,
            "period": period,
            "search": search,
            "outcome": outcome,
            **result,
        },
    )

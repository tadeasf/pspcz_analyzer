"""HTMX partial endpoints — voting analysis (loyalty, attendance, similarity, votes)."""

from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import (
    ATTENDANCE_MIN_VOTES,
    DEFAULT_PERIOD,
    LOYALTY_CHART_MIN_VOTES,
    LOYALTY_FINAL_CHART_MIN_VOTES,
)
from pspcz_analyzer.middleware import run_with_timeout
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.utils import validate_period, with_push_url
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.attendance_service import (
    attendance_view,
    club_strips,
    compute_attendance,
)
from pspcz_analyzer.services.loyalty_service import compute_loyalty, filter_sort_page
from pspcz_analyzer.services.similarity_service import (
    compute_cross_party_similarity,
    similarity_map,
)
from pspcz_analyzer.services.vote_context import final_vote_ids
from pspcz_analyzer.services.votes_service import list_votes

router = APIRouter(tags=["Voting Analysis"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/loyalty", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def loyalty_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    party: str = Query(default="", max_length=50),
    search: str = Query(default="", max_length=100),
    sort: str = Query(default="rate", max_length=10),
    page: int = Query(default=1, ge=1, le=100),
    per_page: int = Query(default=10, ge=0, le=500),
    full: int = Query(default=0, ge=0, le=1),
    scope: str = Query(default="all", max_length=10),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    final_only = scope == "final"
    ids = final_vote_ids(pd) if final_only else None
    all_rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            f"loyalty:{period}:{'final' if final_only else 'all'}",
            lambda: compute_loyalty(pd, top=None, vote_ids=ids),
        ),
        timeout=15.0,
        label="loyalty analysis",
    )
    view = filter_sort_page(all_rows, search, party, sort, page, per_page)
    # final votes are a few dozen per term: a lower floor keeps the chart meaningful
    chart_min = LOYALTY_FINAL_CHART_MIN_VOTES if final_only else LOYALTY_CHART_MIN_VOTES
    chart = sorted(
        (
            r
            for r in all_rows
            if r["active_votes"] >= chart_min and (not party or r["party"] == party)
        ),
        key=lambda r: r["rebellion_pct"],
        reverse=True,
    )[:15]
    response = templates.TemplateResponse(
        "partials/loyalty_table.html",
        {
            "request": request,
            "period": period,
            "party": party,
            "search": search,
            "sort": sort,
            "per_page": per_page,
            "full": full,
            "scope": "final" if final_only else "all",
            "chart": chart,
            "chart_min": chart_min,
            "all_count": len(all_rows),
            "lang": getattr(request.state, "lang", "cs"),
            **view,
        },
    )
    return with_push_url(response, request, "/loyalty/all" if full else "/loyalty")


@router.get("/attendance", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def attendance_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    sort: str = Query(default="worst", max_length=20),
    party: str = Query(default="", max_length=50),
    search: str = Query(default="", max_length=100),
    per_page: int = Query(default=30, ge=0, le=500),
    page: int = Query(default=1, ge=1, le=100),
    view: str = Query(default="table", max_length=10),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"attendance:{period}:{sort}:{party}"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            key, lambda: compute_attendance(pd, top=None, sort=sort, party_filter=party or None)
        ),
        timeout=15.0,
        label="attendance analysis",
    )
    ctx: dict = {
        "request": request,
        "period": period,
        "sort": sort,
        "party": party,
        "search": search,
        "per_page": per_page,
        "view": view,
        "min_votes": ATTENDANCE_MIN_VOTES,
        "lang": getattr(request.state, "lang", "cs"),
        **attendance_view(rows, search, page, per_page),
    }
    if view == "chart":
        ctx["strips"] = club_strips(rows)
        ctx["lowest"] = sorted(
            (r for r in rows if not r["small_sample"] and r["attendance_pct"] is not None),
            key=lambda r: r["attendance_pct"],
        )[:12]
    response = templates.TemplateResponse("partials/attendance_table.html", ctx)
    return with_push_url(response, request, "/attendance")


async def _all_pairs(pd: object, period: int) -> dict[str, list[dict]]:
    """Every eligible pair of both similarity tables, cached per period."""
    return await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            f"similarity:{period}:all",
            lambda: compute_cross_party_similarity(pd, top=None),  # type: ignore[arg-type]
        ),
        timeout=30.0,
        label="similarity analysis",
    )


PAIRS_PER_PAGE = 50


@router.get("/similarity/pairs", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def similarity_pairs_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    table: str = Query(default="cross_party", pattern="^(cross_party|gov_opp)$"),
    page: int = Query(default=1, ge=1, le=10000),
):
    """All pairs of one similarity table, a page at a time (#116 "Zobrazit všechny dvojice")."""
    validate_period(period)
    pd = request.app.state.data.get_period(period)
    rows = (await _all_pairs(pd, period))[table]
    total_pages = max(1, (len(rows) + PAIRS_PER_PAGE - 1) // PAIRS_PER_PAGE)
    page = min(page, total_pages)
    start = (page - 1) * PAIRS_PER_PAGE
    return templates.TemplateResponse(
        "partials/similarity_pairs.html",
        {
            "request": request,
            "period": period,
            "table": table,
            "rows": rows[start : start + PAIRS_PER_PAGE],
            "offset": start,
            "page": page,
            "total_pages": total_pages,
        },
    )


@router.get("/similarity", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def similarity_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = await _all_pairs(pd, period)
    sim_map = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(f"simmap:{period}", lambda: similarity_map(pd)),
        timeout=30.0,
        label="similarity map",
    )
    return templates.TemplateResponse(
        "partials/similarity_table.html",
        {
            "request": request,
            "tables": rows,
            "period": period,
            "map": sim_map,
            "lang": getattr(request.state, "lang", "cs"),
        },
    )


@router.get("/votes", response_class=HTMLResponse)
@limiter.limit("120/minute")
async def votes_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    search: str = Query(default="", max_length=200),
    outcome: str = Query(default="", max_length=20),
    topic: str = Query(default="", max_length=200),
    kind: str = Query(default="", max_length=20),
    page: int = Query(default=1, ge=1, le=1000),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    key = f"votes:{period}:{search}:{outcome}:{topic}:{kind}:{page}:{lang}"
    result = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            key,
            lambda: list_votes(
                pd,
                search=search,
                page=page,
                outcome_filter=outcome,
                topic_filter=topic,
                lang=lang,
                kind_filter=kind,
            ),
        ),
        timeout=15.0,
        label="votes search",
    )
    response = templates.TemplateResponse(
        "partials/votes_list.html",
        {
            "request": request,
            "period": period,
            "search": search,
            "outcome": outcome,
            "topic": topic,
            "kind": kind,
            "lang": lang,
            **result,
        },
    )
    return with_push_url(response, request, "/votes")

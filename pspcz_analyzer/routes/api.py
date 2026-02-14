"""HTMX partial endpoints — return HTML fragments."""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
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
    topic: str = "",
    page: int = 1,
):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    result = list_votes(pd, search=search, page=page, outcome_filter=outcome, topic_filter=topic)
    return templates.TemplateResponse(
        "partials/votes_list.html",
        {
            "request": request,
            "period": period,
            "search": search,
            "outcome": outcome,
            "topic": topic,
            **result,
        },
    )


@router.get("/tisk-text")
async def tisk_text_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    ct: int = 0,
):
    """Return extracted tisk text as an HTML fragment for HTMX loading."""
    data_svc = request.app.state.data
    text = data_svc.tisk_text.get_text(period, ct)
    if text is None:
        return HTMLResponse(
            '<article style="background: #fff3cd; padding: 1rem;">'
            "<p>No extracted text available for this tisk. "
            "Run the CLI to download and extract tisk PDFs:</p>"
            f"<pre><code>uv run python -m pspcz_analyzer.cli.fetch_tisky --period {period}</code></pre>"
            "</article>"
        )
    # Escape HTML and preserve whitespace
    import html as html_mod
    escaped = html_mod.escape(text)
    return HTMLResponse(
        '<article style="max-height: 60vh; overflow-y: auto; background: #f8f9fa; '
        'padding: 1rem; border: 1px solid #dee2e6; border-radius: 0.5rem;">'
        f'<pre style="white-space: pre-wrap; word-wrap: break-word; font-size: 0.85rem;">{escaped}</pre>'
        "</article>"
    )

"""HTMX partial endpoints — return HTML fragments."""

from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import DEFAULT_PERIOD, PERIOD_YEARS
from pspcz_analyzer.middleware import run_with_timeout
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.attendance_service import compute_attendance
from pspcz_analyzer.services.loyalty_service import compute_loyalty
from pspcz_analyzer.services.similarity_service import compute_cross_party_similarity
from pspcz_analyzer.services.votes_service import list_votes

router = APIRouter(tags=["API - HTMX Partials"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


def validate_period(period: int) -> int:
    if period not in PERIOD_YEARS:
        raise HTTPException(404, detail=f"Unknown period {period}")
    return period


@router.get("/loyalty", response_class=HTMLResponse)
@limiter.limit("10/minute")
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
        {"request": request, "rows": rows},
    )


@router.get("/attendance", response_class=HTMLResponse)
@limiter.limit("10/minute")
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
        {"request": request, "rows": rows},
    )


@router.get("/similarity", response_class=HTMLResponse)
@limiter.limit("10/minute")
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
        {"request": request, "rows": rows},
    )


@router.get("/votes", response_class=HTMLResponse)
@limiter.limit("15/minute")
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
    key = f"votes:{period}:{search}:{outcome}:{topic}:{page}"
    result = analysis_cache.get_or_compute(
        key,
        lambda: list_votes(
            pd, search=search, page=page, outcome_filter=outcome, topic_filter=topic
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
            **result,
        },
    )


@router.get("/tisk-text", response_class=HTMLResponse)
@limiter.limit("15/minute")
async def tisk_text_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    ct: int = Query(default=0, ge=0, le=999999),
    ct1: int = Query(default=-1, ge=-1, le=999999),
):
    """Return extracted tisk text as an HTML fragment for HTMX loading.

    When ct1 >= 0, loads sub-tisk text ({ct}_{ct1}.txt) instead of main text.
    """
    validate_period(period)
    data_svc = request.app.state.data
    if ct1 >= 0:
        from pspcz_analyzer.config import TISKY_TEXT_DIR

        text_path = data_svc.cache_dir / TISKY_TEXT_DIR / str(period) / f"{ct}_{ct1}.txt"
        text = text_path.read_text(encoding="utf-8") if text_path.exists() else None
    else:
        text = data_svc.tisk_text.get_text(period, ct)
    if text is None:
        return HTMLResponse(
            '<article style="background: #fff3cd; padding: 1rem;">'
            "<p>No extracted text available for this tisk yet. "
            "The background pipeline will download and extract it automatically.</p>"
            "</article>"
        )
    import html as html_mod

    escaped = html_mod.escape(text)
    return HTMLResponse(
        '<article style="max-height: 60vh; overflow-y: auto; background: #f8f9fa; '
        'padding: 1rem; border: 1px solid #dee2e6; border-radius: 0.5rem;">'
        f'<pre style="white-space: pre-wrap; word-wrap: break-word; font-size: 0.85rem;">{escaped}</pre>'
        "</article>"
    )


@router.get("/tisk-evolution", response_class=HTMLResponse)
@limiter.limit("15/minute")
async def tisk_evolution_api(
    request: Request,
    period: int = DEFAULT_PERIOD,
    ct: int = Query(default=0, ge=0, le=999999),
):
    """Return the legislative evolution partial (law changes + sub-tisk versions)."""
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    tisk = None
    if pd:
        for t in pd.tisk_lookup.values():
            if t.ct == ct:
                tisk = t
                break

    law_changes = tisk.law_changes if tisk else []
    sub_versions = tisk.sub_versions if tisk else []

    return templates.TemplateResponse(
        "partials/tisk_evolution.html",
        {
            "request": request,
            "period": period,
            "ct": ct,
            "law_changes": law_changes,
            "sub_versions": sub_versions,
        },
    )


@router.get("/related-bills", response_class=HTMLResponse)
@limiter.limit("5/minute")
async def related_bills_api(
    request: Request,
    idsb: int = Query(default=0, ge=0, le=999999),
):
    """Lazy-load related bills for a specific law (scrapes on demand, caches)."""
    if idsb <= 0:
        return HTMLResponse("<p>Invalid law reference.</p>")

    from dataclasses import asdict

    from pspcz_analyzer.config import DEFAULT_CACHE_DIR
    from pspcz_analyzer.data.law_changes_scraper import (
        load_related_bills_json,
        save_related_bills_json,
        scrape_related_bills,
    )

    cache_dir = DEFAULT_CACHE_DIR
    cached = load_related_bills_json(idsb, cache_dir)
    if cached is not None:
        bills = [asdict(b) for b in cached]
    else:
        raw_bills = await run_with_timeout(
            scrape_related_bills,
            idsb,
            timeout=15.0,
            label="related bills scrape",
        )
        save_related_bills_json(raw_bills, idsb, cache_dir)
        bills = [asdict(b) for b in raw_bills]

    if not bills:
        return HTMLResponse(
            '<p style="color: #6c757d; font-size: 0.85rem;">'
            "No related bills found for this law.</p>"
        )

    rows_html = ""
    for b in bills:
        url = b.get("url", "")
        cislo = b.get("cislo", "?")
        link = f'<a href="{url}" target="_blank">{cislo}</a>' if url else cislo
        rows_html += (
            f"<tr>"
            f"<td>{link}</td>"
            f"<td>{b.get('kratky_nazev', '')}</td>"
            f"<td>{b.get('typ_tisku', '')}</td>"
            f"<td>{b.get('stav', '')}</td>"
            f"</tr>"
        )

    return HTMLResponse(
        '<table style="font-size: 0.85rem; margin: 0.5rem 0;">'
        "<thead><tr>"
        "<th>Tisk</th><th>Title</th><th>Type</th><th>Status</th>"
        "</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table>"
    )


@router.get("/health", response_class=JSONResponse, tags=["Health"])
@limiter.limit("120/minute")
async def health(request: Request):
    """Health check endpoint."""
    data_svc = request.app.state.data
    return {"status": "ok", "periods_loaded": list(data_svc.loaded_periods)}

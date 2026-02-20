"""HTMX partial endpoints — return HTML fragments."""

import asyncio
import html as html_mod
import time
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlparse

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    DEFAULT_PERIOD,
    GITHUB_FEEDBACK_ENABLED,
    PERIOD_YEARS,
    TISKY_TEXT_DIR,
)
from pspcz_analyzer.data.law_changes_scraper import (
    load_related_bills_json,
    save_related_bills_json,
    scrape_related_bills,
)
from pspcz_analyzer.i18n import gettext as _t
from pspcz_analyzer.middleware import run_with_timeout
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.attendance_service import compute_attendance
from pspcz_analyzer.services.feedback_service import GitHubFeedbackClient
from pspcz_analyzer.services.loyalty_service import compute_loyalty
from pspcz_analyzer.services.ollama_service import OllamaClient
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
        {"request": request, "rows": rows, "lang": getattr(request.state, "lang", "cs")},
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
        {"request": request, "rows": rows, "lang": getattr(request.state, "lang", "cs")},
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
        {"request": request, "rows": rows, "lang": getattr(request.state, "lang", "cs")},
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
        text_path = data_svc.cache_dir / TISKY_TEXT_DIR / str(period) / f"{ct}_{ct1}.txt"
        text = text_path.read_text(encoding="utf-8") if text_path.exists() else None
    else:
        text = data_svc.tisk_text.get_text(period, ct)
    if text is None:
        return HTMLResponse(
            '<article style="background: #fff3cd; padding: 1rem;">'
            f"<p>{html_mod.escape(_t('tisk.no_text'))}</p>"
            "</article>"
        )
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
            "lang": getattr(request.state, "lang", "cs"),
            "feedback_enabled": GITHUB_FEEDBACK_ENABLED,
        },
    )


def _safe_url(url: str) -> str:
    """Return url only if scheme is http/https, else empty string."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        if parsed.scheme in ("http", "https"):
            return url
    except ValueError:
        pass
    return ""


@router.get("/related-bills", response_class=HTMLResponse)
@limiter.limit("5/minute")
async def related_bills_api(
    request: Request,
    idsb: int = Query(default=0, ge=0, le=999999),
):
    """Lazy-load related bills for a specific law (scrapes on demand, caches)."""
    if idsb <= 0:
        return HTMLResponse(f"<p>{html_mod.escape(_t('related.invalid'))}</p>")

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
            f"{html_mod.escape(_t('related.no_bills'))}</p>"
        )

    rows_html = ""
    for b in bills:
        raw_url = _safe_url(b.get("url", ""))
        cislo = html_mod.escape(str(b.get("cislo", "?")))
        if raw_url:
            safe_href = html_mod.escape(raw_url)
            link = f'<a href="{safe_href}" target="_blank" rel="noopener">{cislo}</a>'
        else:
            link = cislo
        nazev = html_mod.escape(str(b.get("kratky_nazev", "")))
        typ = html_mod.escape(str(b.get("typ_tisku", "")))
        stav = html_mod.escape(str(b.get("stav", "")))
        rows_html += f"<tr><td>{link}</td><td>{nazev}</td><td>{typ}</td><td>{stav}</td></tr>"

    return HTMLResponse(
        '<table style="font-size: 0.85rem; margin: 0.5rem 0;">'
        "<thead><tr>"
        f"<th>{html_mod.escape(_t('related.th.tisk'))}</th>"
        f"<th>{html_mod.escape(_t('related.th.title'))}</th>"
        f"<th>{html_mod.escape(_t('related.th.type'))}</th>"
        f"<th>{html_mod.escape(_t('related.th.status'))}</th>"
        "</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table>"
    )


def _validate_origin(request: Request) -> bool:
    """Check that Origin or Referer matches the request host."""
    expected = request.url.hostname
    for header in ("origin", "referer"):
        value = request.headers.get(header)
        if value:
            try:
                return urlparse(value).hostname == expected
            except ValueError:
                return False
    return False


def _validate_feedback_fields(title: str, body: str) -> str | None:
    """Return an error message if feedback fields are invalid, else None."""
    if len(title) < 5 or len(title) > 200 or len(body) < 10 or len(body) > 2000:
        return _t("feedback.error_validation")
    return None


@router.post("/feedback", response_class=HTMLResponse)
@limiter.limit("3/hour")
async def feedback_api(
    request: Request,
    vote_id: int = Form(default=0),
    period: int = Form(default=0),
    title: str = Form(default=""),
    body: str = Form(default=""),
):
    """Submit user feedback as a GitHub issue."""
    lang = getattr(request.state, "lang", "cs")
    suffix = ""

    if not _validate_origin(request):
        return templates.TemplateResponse(
            "partials/feedback_result.html",
            {
                "request": request,
                "success": False,
                "error_message": _t("feedback.error_csrf"),
                "feedback_id_suffix": suffix,
                "lang": lang,
            },
        )

    if not GITHUB_FEEDBACK_ENABLED:
        return templates.TemplateResponse(
            "partials/feedback_result.html",
            {
                "request": request,
                "success": False,
                "error_message": _t("feedback.disabled"),
                "feedback_id_suffix": suffix,
                "lang": lang,
            },
        )

    validation_error = _validate_feedback_fields(title, body)
    if validation_error:
        return templates.TemplateResponse(
            "partials/feedback_result.html",
            {
                "request": request,
                "success": False,
                "error_message": validation_error,
                "feedback_id_suffix": suffix,
                "lang": lang,
            },
        )

    page_url = str(request.headers.get("referer", f"/votes/{vote_id}?period={period}"))
    client = GitHubFeedbackClient()
    result = await asyncio.to_thread(
        client.create_issue, title, body, vote_id, period, page_url, lang
    )

    if result:
        return templates.TemplateResponse(
            "partials/feedback_result.html",
            {
                "request": request,
                "success": True,
                "issue_url": result["html_url"],
                "feedback_id_suffix": suffix,
                "lang": lang,
            },
        )
    return templates.TemplateResponse(
        "partials/feedback_result.html",
        {
            "request": request,
            "success": False,
            "error_message": _t("feedback.error_generic"),
            "feedback_id_suffix": suffix,
            "lang": lang,
        },
    )


@router.get("/health", response_class=JSONResponse, tags=["Health"])
@limiter.limit("120/minute")
async def health(request: Request):
    """Health check endpoint."""
    data_svc = request.app.state.data
    return {"status": "ok", "periods_loaded": list(data_svc.loaded_periods)}


# ── Ollama diagnostic constants ──────────────────────────────────────────

_SMOKE_TEST_TITLE = "Novela zákona o státní službě"
_SMOKE_TEST_TEXT = (
    "Navrhovaná novela zákona č. 234/2014 Sb., o státní službě, zavádí nový systém "
    "hodnocení výkonu státních zaměstnanců založený na klíčových ukazatelích výkonnosti. "
    "Služební orgán bude oprávněn stanovit individuální výkonnostní cíle pro každého "
    "zaměstnance a jejich plnění bude podmínkou pro přiznání osobního příplatku.\n\n"
    "Dále se mění ustanovení § 72 odst. 1 tak, že se zkracuje doba potřebná pro vznik "
    "nároku na služební volno z pěti na tři roky nepřetržité služby. Současně se ruší "
    "povinnost služebního úřadu zajistit zastupitelnost zaměstnanců v době jejich "
    "nepřítomnosti delší než 30 dnů.\n\n"
    "Přechodná ustanovení stanoví, že stávající zaměstnanci budou hodnoceni podle nových "
    "pravidel nejpozději od 1. ledna následujícího kalendářního roku po nabytí účinnosti "
    "této novely."
)


@router.get("/api/ollama/health", response_class=JSONResponse, tags=["Health"])
@limiter.limit("10/minute")
async def ollama_health(request: Request) -> dict:
    """Check Ollama connectivity and model availability."""
    client = OllamaClient()
    available = await asyncio.to_thread(client.is_available)
    return {
        "available": available,
        "base_url": client.base_url,
        "model": client.model,
    }


def _build_smoke_error(error: str, duration: float, model: str) -> dict:
    """Build a failure response dict for the smoke-test endpoint."""
    return {
        "success": False,
        "error": error,
        "duration_seconds": round(duration, 2),
        "model": model,
    }


@router.get("/api/ollama/smoke-test", response_class=JSONResponse, tags=["Health"])
@limiter.limit("2/minute")
async def ollama_smoke_test(request: Request) -> dict:
    """Run concurrent bilingual generation to verify Ollama end-to-end."""
    client = OllamaClient()
    start = time.monotonic()

    available = await asyncio.to_thread(client.is_available)
    if not available:
        duration = time.monotonic() - start
        raise HTTPException(
            status_code=503,
            detail=_build_smoke_error("Ollama is not available", duration, client.model),
        )

    try:
        cs_result, en_result = await asyncio.gather(
            asyncio.to_thread(client.summarize, _SMOKE_TEST_TEXT, _SMOKE_TEST_TITLE),
            asyncio.to_thread(client.summarize_en, _SMOKE_TEST_TEXT, _SMOKE_TEST_TITLE),
        )
    except Exception as exc:
        duration = time.monotonic() - start
        raise HTTPException(
            status_code=502,
            detail=_build_smoke_error(str(exc), duration, client.model),
        ) from exc

    duration = time.monotonic() - start
    return {
        "success": True,
        "model": client.model,
        "duration_seconds": round(duration, 2),
        "summary_cs": cs_result,
        "summary_en": en_result,
        "summary_cs_length": len(cs_result),
        "summary_en_length": len(en_result),
    }

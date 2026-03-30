"""HTMX partial endpoints — tisk text, evolution, and related bills."""

import html as html_mod
from dataclasses import asdict
from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import (
    DEFAULT_CACHE_DIR,
    DEFAULT_PERIOD,
    GITHUB_FEEDBACK_ENABLED,
    TISKY_TEXT_DIR,
)
from pspcz_analyzer.i18n import gettext as _t
from pspcz_analyzer.middleware import run_with_timeout
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.utils import _safe_url, validate_period
from pspcz_analyzer.services.tisk.io import (
    load_related_bills_json,
    save_related_bills_json,
    scrape_related_bills,
)

router = APIRouter(tags=["Tisk"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/tisk-text", response_class=HTMLResponse)
@limiter.limit("120/minute")
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
@limiter.limit("120/minute")
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


@router.get("/related-bills", response_class=HTMLResponse)
@limiter.limit("30/minute")
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

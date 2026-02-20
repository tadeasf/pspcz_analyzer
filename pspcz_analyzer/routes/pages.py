"""HTML page routes (full-page renders)."""

from pathlib import Path
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import Response

from pspcz_analyzer.config import DEFAULT_PERIOD, GITHUB_FEEDBACK_ENABLED
from pspcz_analyzer.i18n import SUPPORTED_LANGUAGES
from pspcz_analyzer.i18n import gettext as _t
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.api import validate_period
from pspcz_analyzer.services.votes_service import vote_detail

router = APIRouter(tags=["Pages"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


def _ctx(request: Request, period: int, **kwargs) -> dict:
    """Build common template context with period info."""
    data_svc = request.app.state.data
    return {
        "request": request,
        "period": period,
        "periods": data_svc.available_periods,
        "lang": getattr(request.state, "lang", "cs"),
        **kwargs,
    }


def _safe_referer(referer: str | None) -> str:
    """Extract path from referer, rejecting external URLs."""
    if not referer:
        return "/"
    try:
        parsed = urlparse(referer)
        if parsed.scheme or parsed.netloc:
            path = parsed.path or "/"
            if parsed.query:
                path = f"{path}?{parsed.query}"
            if path.startswith("//"):
                return "/"
            return path
        if referer.startswith("//"):
            return "/"
        return referer
    except ValueError:
        return "/"


@router.get("/set-lang/{lang}")
async def set_lang(request: Request, lang: str) -> Response:
    """Set the UI language via cookie and redirect back."""
    if lang not in SUPPORTED_LANGUAGES:
        lang = "cs"
    redirect_path = _safe_referer(request.headers.get("referer"))
    response = RedirectResponse(url=redirect_path, status_code=303)
    response.set_cookie(
        "lang",
        lang,
        max_age=365 * 24 * 3600,
        samesite="lax",
        httponly=True,
        secure=request.url.scheme == "https",
    )
    return response


@router.get("/")
@limiter.limit("60/minute")
async def index(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    return templates.TemplateResponse(
        "index.html",
        _ctx(request, period, stats=pd.stats, active_page="index"),
    )


@router.get("/loyalty")
@limiter.limit("60/minute")
async def loyalty_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    return templates.TemplateResponse(
        "loyalty.html",
        _ctx(request, period, active_page="loyalty"),
    )


@router.get("/attendance")
@limiter.limit("60/minute")
async def attendance_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    return templates.TemplateResponse(
        "attendance.html",
        _ctx(request, period, active_page="attendance"),
    )


@router.get("/similarity")
@limiter.limit("60/minute")
async def similarity_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    return templates.TemplateResponse(
        "similarity.html",
        _ctx(request, period, active_page="similarity"),
    )


@router.get("/votes")
@limiter.limit("60/minute")
async def votes_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    return templates.TemplateResponse(
        "votes.html",
        _ctx(request, period, active_page="votes", topics=pd.get_all_topic_labels(lang)),
    )


@router.get("/votes/{vote_id}")
@limiter.limit("60/minute")
async def vote_detail_page(request: Request, vote_id: int, period: int = DEFAULT_PERIOD):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    detail = vote_detail(pd, vote_id, lang)
    if detail is None:
        return templates.TemplateResponse(
            "votes.html",
            _ctx(request, period, active_page="votes", error=_t("vote.not_found")),
        )
    return templates.TemplateResponse(
        "vote_detail.html",
        _ctx(
            request,
            period,
            detail=detail,
            active_page="votes",
            feedback_enabled=GITHUB_FEEDBACK_ENABLED,
        ),
    )

"""HTML page routes (full-page renders)."""

from pathlib import Path
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import Response

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.i18n import SUPPORTED_LANGUAGES
from pspcz_analyzer.i18n import gettext as _t
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.context import excuses_as_of
from pspcz_analyzer.routes.utils import validate_period
from pspcz_analyzer.services.amendment_listing import law_amendments
from pspcz_analyzer.services.law_service import get_all_status_labels
from pspcz_analyzer.services.law_service import law_detail as get_law_detail
from pspcz_analyzer.services.legislation_overview import homepage_blocks
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


def _period_clubs(request: Request, period: int) -> list[str]:
    """Clubs of the period's MPs (latest club), largest first."""
    parties = request.app.state.data.get_period(period).mp_info.get_column("party").drop_nulls()
    return parties.value_counts(sort=True).get_column("party").to_list()


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
    lang = getattr(request.state, "lang", "cs")
    return templates.TemplateResponse(
        "index.html",
        _ctx(
            request,
            period,
            active_page="index",
            blocks=homepage_blocks(pd, lang),
        ),
    )


@router.get("/loyalty")
@limiter.limit("60/minute")
async def loyalty_page(request: Request, period: int = DEFAULT_PERIOD, tab: str = ""):
    validate_period(period)
    clubs = _period_clubs(request, period)
    return templates.TemplateResponse(
        "loyalty.html",
        _ctx(
            request,
            period,
            active_page="loyalty",
            tab="podobnost" if tab == "podobnost" else "rebelie",
            clubs=clubs,
        ),
    )


@router.get("/loyalty/all")
@limiter.limit("60/minute")
async def loyalty_all_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    return templates.TemplateResponse(
        "loyalty_all.html",
        _ctx(request, period, active_page="loyalty", clubs=_period_clubs(request, period)),
    )


@router.get("/attendance")
@limiter.limit("60/minute")
async def attendance_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    return templates.TemplateResponse(
        "attendance.html",
        _ctx(request, period, active_page="attendance", clubs=_period_clubs(request, period)),
    )


@router.get("/jak-se-v-tom-vyznat")
@limiter.limit("60/minute")
async def guide_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    return templates.TemplateResponse("guide.html", _ctx(request, period, active_page="guide"))


@router.get("/nahlasit-chybu")
@limiter.limit("60/minute")
async def report_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    source_page = _safe_referer(request.headers.get("referer"))
    return templates.TemplateResponse(
        "report.html", _ctx(request, period, active_page="report", source_page=source_page)
    )


@router.get("/similarity")
@limiter.limit("60/minute")
async def similarity_page(request: Request, period: int = DEFAULT_PERIOD) -> Response:
    """Old URL: similarity is now a tab of the loyalty page (#105, #116)."""
    validate_period(period)
    return RedirectResponse(f"/loyalty?period={period}&tab=podobnost", status_code=301)


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
            excuses_as_of=excuses_as_of(data_svc.cache_dir, period),
        ),
    )


@router.get("/laws")
@limiter.limit("60/minute")
async def laws_page(request: Request, period: int = DEFAULT_PERIOD, search: str = ""):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    return templates.TemplateResponse(
        "laws.html",
        _ctx(
            request,
            period,
            active_page="laws",
            topics=pd.get_all_topic_labels(lang),
            statuses=get_all_status_labels(pd),
            initial_search=search,
        ),
    )


@router.get("/laws/{ct}")
@limiter.limit("60/minute")
async def law_detail_page(request: Request, ct: int, period: int = DEFAULT_PERIOD):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    detail = get_law_detail(pd, ct, data_svc.cache_dir, lang)
    if detail is None:
        return templates.TemplateResponse(
            "laws.html",
            _ctx(request, period, active_page="laws"),
        )
    return templates.TemplateResponse(
        "law_detail.html",
        _ctx(request, period, detail=detail, active_page="laws"),
    )


def _clubs(pd: PeriodData) -> list[str]:
    """Club names of the period's MPs, for the amendments club filter."""
    return sorted({p for p in pd.mp_info.get_column("party").to_list() if p})


@router.get("/amendments")
@limiter.limit("60/minute")
async def amendments_page(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    pd = request.app.state.data.get_period(period)
    return templates.TemplateResponse(
        "amendments.html",
        _ctx(request, period, active_page="amendments", clubs=_clubs(pd)),
    )


@router.get("/amendments/{schuze}/{bod}")
@limiter.limit("60/minute")
async def amendment_detail_page(
    request: Request, schuze: int, bod: int, period: int = DEFAULT_PERIOD
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    lang = getattr(request.state, "lang", "cs")
    detail = law_amendments(pd, schuze, bod, lang)
    if detail is None:
        return templates.TemplateResponse(
            "amendments.html",
            _ctx(
                request,
                period,
                active_page="amendments",
                clubs=_clubs(pd),
                error=_t("pn.not_found"),
            ),
        )
    return templates.TemplateResponse(
        "amendment_detail.html",
        _ctx(request, period, detail=detail, active_page="amendments"),
    )

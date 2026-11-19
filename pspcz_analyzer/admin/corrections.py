"""Admin corrections: vote questions, AI briefs, amendment summaries, homepage pick (TASKS 2X).

Changes go to the period's overrides file (services/overrides) with who and when;
the public site applies them on its next request.
"""

import asyncio
import re
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from pspcz_analyzer.admin.auth import get_session_username
from pspcz_analyzer.admin.routes import templates
from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.models.tisk_models import PeriodData
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.services.overrides import BRIEF_REVIEWS, KINDS, load_overrides, set_override

router = APIRouter(prefix="/admin")

_LETTER_RE = re.compile(r"^[A-Z]{1,3}\d{0,3}$")
MAX_SHORT = 500
MAX_LONG = 3000


class CorrectionError(ValueError):
    """Invalid correction input; the message is shown to the admin."""


def _text(form: dict[str, str], name: str, limit: int, required: bool = True) -> str:
    value = " ".join(form.get(name, "").split())
    if required and not value:
        raise CorrectionError(f"'{name}' is empty")
    if len(value) > limit:
        raise CorrectionError(f"'{name}' is longer than {limit} characters")
    return value


def _int(value: str, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise CorrectionError(f"'{name}' must be a number") from exc


def _vote_question(pd: PeriodData, form: dict[str, str]) -> tuple[str, dict]:
    vid = _int(form.get("key", ""), "vote id")
    if pd.votes.filter(pd.votes["id_hlasovani"] == vid).height == 0:
        raise CorrectionError(f"vote {vid} is not in this period")
    fields = {
        "question": _text(form, "question", MAX_SHORT),
        "question_en": _text(form, "question_en", MAX_SHORT, required=False),
    }
    return str(vid), fields


def _brief(pd: PeriodData, form: dict[str, str]) -> tuple[str, dict]:
    ct = _int(form.get("key", ""), "tisk")
    if ct not in {t.ct for t in pd.tisk_lookup.values()}:
        raise CorrectionError(f"tisk {ct} is not in this period")
    review = form.get("review", "")
    if review not in BRIEF_REVIEWS:
        raise CorrectionError("unknown review value")
    fields: dict = {"review": review}
    if review == "edited":
        fields |= {
            "nazev": _text(form, "nazev", MAX_SHORT),
            "shrnuti": _text(form, "shrnuti", MAX_LONG),
        }
    return str(ct), fields


def _amendment_summary(pd: PeriodData, form: dict[str, str]) -> tuple[str, dict]:
    ct = _int(form.get("ct", ""), "tisk")
    letter = form.get("letter", "").strip().upper()
    if not _LETTER_RE.match(letter):
        raise CorrectionError("letter must look like A, B2 or AA")
    known = {
        a.letter.strip().upper()
        for b in pd.amendment_data.values()
        if b.ct == ct
        for a in b.amendments
    }
    if letter not in known:
        raise CorrectionError(f"tisk {ct} has no amendment letter {letter}")
    fields = {
        "summary": _text(form, "summary", MAX_LONG),
        "summary_en": _text(form, "summary_en", MAX_LONG, required=False),
    }
    return f"{ct}:{letter}", fields


def _homepage_pick(pd: PeriodData, form: dict[str, str]) -> tuple[str, dict]:
    ct = _int(form.get("ct", ""), "tisk")
    if ct not in {t.ct for t in pd.tisk_lookup.values() if t.id_druh in (1, 2, 3)}:
        raise CorrectionError(f"tisk {ct} is not a bill of this period")
    return "talked_about", {"ct": ct}


def _parse(pd: PeriodData, kind: str, form: dict[str, str]) -> tuple[str, dict]:
    """Validated key and fields of one correction."""
    match kind:
        case "vote_question":
            return _vote_question(pd, form)
        case "brief":
            return _brief(pd, form)
        case "amendment_summary":
            return _amendment_summary(pd, form)
        case "homepage_pick":
            return _homepage_pick(pd, form)
        case _:
            raise CorrectionError("unknown kind")


@router.get("/corrections", response_class=HTMLResponse)
async def corrections_page(
    request: Request, period: int = DEFAULT_PERIOD, msg: str = "", error: str = ""
) -> HTMLResponse:
    """Current corrections of a period plus forms to add or remove them."""
    svc = request.app.state.data
    if period not in svc.available_periods:
        period = DEFAULT_PERIOD
    return templates.TemplateResponse(
        "corrections.html",
        {
            "request": request,
            "admin_user": get_session_username(request),
            "available_periods": svc.available_periods,
            "period": period,
            "ov": load_overrides(svc.cache_dir, period),
            "msg": msg[:200],
            "error": error[:200],
        },
    )


@router.post("/api/corrections", response_model=None)
@limiter.limit("30/minute")
async def save_correction(request: Request) -> RedirectResponse:
    """Add, replace or delete one correction, then go back to the page."""
    svc = request.app.state.data
    form = {k: str(v) for k, v in (await request.form()).items()}
    period = int(form["period"]) if form.get("period", "").isdigit() else 0
    kind = form.get("kind", "")
    back = f"/admin/corrections?period={period}"
    if period not in svc.available_periods or kind not in KINDS:
        return RedirectResponse(f"{back}&error=unknown+period+or+kind", status_code=303)
    user = get_session_username(request) or "admin"
    try:
        if form.get("action") == "delete":
            key, fields = form.get("key", ""), None
        else:
            pd = await asyncio.to_thread(svc.get_period, period)
            key, fields = _parse(pd, kind, form)
        await asyncio.to_thread(set_override, svc.cache_dir, period, kind, key, fields, user)
    except CorrectionError as exc:
        return RedirectResponse(f"{back}&error={quote(str(exc))}", status_code=303)
    done = "deleted" if fields is None else "saved"
    return RedirectResponse(f"{back}&msg={quote(f'{kind} {key} {done}')}", status_code=303)

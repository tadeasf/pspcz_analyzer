"""HTMX partial endpoints — user feedback submission."""

import asyncio
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pspcz_analyzer.config import GITHUB_FEEDBACK_ENABLED
from pspcz_analyzer.i18n import gettext as _t
from pspcz_analyzer.middleware import is_same_origin
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.services.feedback_service import GitHubFeedbackClient

router = APIRouter(tags=["Feedback"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


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

    if not is_same_origin(request):
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

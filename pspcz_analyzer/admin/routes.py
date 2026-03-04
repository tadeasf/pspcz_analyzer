"""Admin dashboard routes — pipeline management, config editor, log viewer."""

import asyncio
import contextlib
import shutil
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from loguru import logger

from pspcz_analyzer.admin.auth import (
    _SESSION_COOKIE,
    create_session_cookie,
    get_session_username,
    verify_password,
)
from pspcz_analyzer.admin.log_stream import log_broadcaster
from pspcz_analyzer.admin.pipeline_history import PipelineHistory
from pspcz_analyzer.config import ADMIN_USERNAME, DEFAULT_CACHE_DIR
from pspcz_analyzer.models.pipeline_progress import (
    AmendmentMode,
    PipelineStage,
    TiskMode,
)
from pspcz_analyzer.services.data_service import DataService
from pspcz_analyzer.services.pipeline_lock import pipeline_lock
from pspcz_analyzer.services.runtime_config import (
    RuntimeConfig,
    apply_runtime_config,
    load_runtime_config,
    save_runtime_config,
)

router = APIRouter(prefix="/admin")

ADMIN_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(ADMIN_TEMPLATES_DIR))

# ── Jinja2 globals: stage labels + helpers ───────────────────────

STAGE_LABELS: dict[str, str] = {
    PipelineStage.IDLE: "Idle",
    PipelineStage.SCRAPE_HISTORIES: "Scraping histories",
    PipelineStage.DOWNLOAD_PDFS: "Downloading PDFs",
    PipelineStage.CLASSIFY: "Classifying & summarizing",
    PipelineStage.CONSOLIDATE_TOPICS: "Consolidating topics",
    PipelineStage.SCRAPE_LAW_CHANGES: "Scraping law changes",
    PipelineStage.DOWNLOAD_VERSIONS: "Downloading versions",
    PipelineStage.ANALYZE_DIFFS: "Analyzing diffs",
    PipelineStage.COMPLETED: "Completed",
    PipelineStage.FAILED: "Failed",
    # Amendment stages
    "identify": "Identifying candidates",
    "pdf_download_parse": "Downloading & parsing amendment PDFs",
    "steno_download_parse": "Downloading & parsing steno",
    "merge": "Merging PDF and steno data",
    "resolve_ids": "Resolving vote IDs",
    "resolve_submitters": "Resolving submitters",
    "llm_summarize": "LLM summarizing",
    "cache": "Saving to cache",
    "completed": "Completed",
    "failed": "Failed",
    "cancelled": "Cancelled",
}

PIPELINE_TYPE_LABELS: dict[str, str] = {
    "tisk_download": "Tisk Download",
    "tisk_classify": "Tisk Classify + Summarize",
    "tisk_diffs": "Tisk Version Diffs",
    "amendment_parse": "Amendment Parse",
    "amendment_summarize": "Amendment Summarize",
    "full": "Full Pipeline",
}


def _format_eta(seconds: float | None) -> str:
    """Format ETA seconds as human-readable string."""
    if seconds is None:
        return "—"
    if seconds <= 0:
        return "< 1s"
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as human-readable string."""
    if seconds <= 0:
        return "0s"
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


# Register helpers as Jinja2 globals
templates.env.globals["stage_labels"] = STAGE_LABELS
templates.env.globals["pipeline_type_labels"] = PIPELINE_TYPE_LABELS
templates.env.globals["format_eta"] = _format_eta
templates.env.globals["format_elapsed"] = _format_elapsed


# ── Auth routes ──────────────────────────────────────────────────


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    """Render admin login form."""
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@router.post("/login", response_model=None)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
) -> RedirectResponse | HTMLResponse:
    """Authenticate admin user."""
    if username != ADMIN_USERNAME or not verify_password(password):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid username or password"},
            status_code=401,
        )

    response = RedirectResponse(url="/admin/", status_code=303)
    response.set_cookie(
        _SESSION_COOKIE,
        create_session_cookie(username),
        httponly=True,
        samesite="lax",
        max_age=86400,
    )
    return response


@router.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    """Clear admin session."""
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie(_SESSION_COOKIE)
    return response


# ── Dashboard ────────────────────────────────────────────────────


def _cache_size_mb(cache_dir: Path = DEFAULT_CACHE_DIR) -> float:
    """Calculate total cache directory size in MB."""
    total = sum(f.stat().st_size for f in cache_dir.rglob("*") if f.is_file())
    return round(total / (1024 * 1024), 1)


def _disk_free_gb(cache_dir: Path = DEFAULT_CACHE_DIR) -> float:
    """Get free disk space on cache volume in GB."""
    usage = shutil.disk_usage(cache_dir)
    return round(usage.free / (1024 * 1024 * 1024), 1)


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Admin dashboard overview."""
    svc = request.app.state.data
    pipeline_info = pipeline_lock.current
    config = load_runtime_config(svc.cache_dir)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "admin_user": get_session_username(request),
            "pipeline_running": pipeline_lock.is_locked,
            "pipeline_info": pipeline_info,
            "loaded_periods": svc.loaded_periods,
            "available_periods": svc.available_periods,
            "cache_size_mb": _cache_size_mb(svc.cache_dir),
            "disk_free_gb": _disk_free_gb(svc.cache_dir),
            "llm_provider": config.llm_provider,
            "llm_model": config.ollama_model
            if config.llm_provider == "ollama"
            else config.openai_model,
        },
    )


# ── Pipeline management ─────────────────────────────────────────


@router.get("/pipelines", response_class=HTMLResponse)
async def pipelines_page(request: Request) -> HTMLResponse:
    """Pipeline management page."""
    svc = request.app.state.data
    history: PipelineHistory = request.app.state.pipeline_history

    return templates.TemplateResponse(
        "pipelines.html",
        {
            "request": request,
            "admin_user": get_session_username(request),
            "available_periods": svc.available_periods,
            "pipeline_running": pipeline_lock.is_locked,
            "pipeline_info": pipeline_lock.current,
            "recent_runs": history.get_runs()[:20],
        },
    )


@router.get("/partials/pipeline-status", response_class=HTMLResponse)
async def pipeline_status_partial(request: Request) -> HTMLResponse:
    """HTML partial for HTMX polling — renders pipeline status + progress bars."""
    svc = request.app.state.data
    tisk_progress = svc.tisk_pipeline.progress
    amendment_progress = svc.amendment_pipeline.progress

    return templates.TemplateResponse(
        "partials/pipeline_status.html",
        {
            "request": request,
            "pipeline_running": pipeline_lock.is_locked,
            "pipeline_info": pipeline_lock.current,
            "tisk_progress": tisk_progress,
            "amendment_progress": amendment_progress,
        },
    )


async def _monitor_pipeline(
    svc: DataService,
    pipeline_type: str,
    period: int,
    task: asyncio.Task,
    run_data: dict,
    history: PipelineHistory,
    label: str,
) -> None:
    """Background coroutine: await the real pipeline task, then release lock and record history."""
    try:
        await task
        # For "full" mode, tisk's on_complete chains to amendment — wait for that too
        if pipeline_type == "full":
            await asyncio.sleep(0.5)
            amendment_task = svc.amendment_pipeline.get_task(period)
            if amendment_task is not None and not amendment_task.done():
                await amendment_task
        history.finish_run(run_data, "success")
        logger.info("[admin] Pipeline {} finished successfully", label)
    except asyncio.CancelledError:
        history.finish_run(run_data, "cancelled")
        logger.info("[admin] Pipeline {} cancelled", label)
    except Exception as exc:
        history.finish_run(run_data, "error", str(exc))
        logger.opt(exception=True).error("[admin] Pipeline {} failed", label)
    finally:
        pipeline_lock.release()


def _get_pipeline_task(svc: DataService, pipeline_type: str, period: int) -> asyncio.Task | None:
    """Get the asyncio.Task reference for a just-started pipeline."""
    match pipeline_type:
        case "tisk_download" | "tisk_classify" | "tisk_diffs" | "full":
            return svc.tisk_pipeline.get_task(period)
        case "amendment_parse" | "amendment_summarize":
            return svc.amendment_pipeline.get_task(period)
        case _:
            return None


@router.post("/api/pipeline/start")
async def start_pipeline(
    request: Request,
    pipeline_type: str = Form(...),
    period: int = Form(...),
) -> dict:
    """Start a pipeline for a given period. Supports 6 pipeline modes.

    The lock is held until the background pipeline task finishes (or fails/cancels).
    A monitor coroutine awaits the task and handles lock release + history recording.
    """
    svc: DataService = request.app.state.data
    history: PipelineHistory = request.app.state.pipeline_history

    acquired = await pipeline_lock.acquire(pipeline_type, period)
    if not acquired:
        current = pipeline_lock.current
        return {
            "status": "blocked",
            "message": f"Pipeline already running: {current.pipeline_id if current else 'unknown'}",
        }

    run_data = PipelineHistory.create_run(pipeline_type, period)
    label = PIPELINE_TYPE_LABELS.get(pipeline_type, pipeline_type)

    try:
        started: bool
        match pipeline_type:
            case "tisk_download":
                started = svc.start_tisk_pipeline(period, mode=TiskMode.DOWNLOAD)
            case "tisk_classify":
                started = svc.start_tisk_pipeline(period, mode=TiskMode.CLASSIFY)
            case "tisk_diffs":
                started = svc.start_tisk_pipeline(period, mode=TiskMode.DIFFS)
            case "amendment_parse":
                started = svc.start_amendment_pipeline(period, mode=AmendmentMode.PARSE)
            case "amendment_summarize":
                started = svc.start_amendment_pipeline(period, mode=AmendmentMode.SUMMARIZE)
            case "full":
                # Tisk on_complete callback chains to amendment — don't double-start
                started = svc.start_tisk_pipeline(period, mode=TiskMode.FULL)
            case _:
                pipeline_lock.release()
                return {"status": "error", "message": f"Unknown pipeline type: {pipeline_type}"}
    except Exception as exc:
        pipeline_lock.release()
        history.finish_run(run_data, "error", str(exc))
        logger.opt(exception=True).error("[admin] Pipeline start failed")
        return {"status": "error", "message": str(exc)}

    if not started:
        pipeline_lock.release()
        history.finish_run(run_data, "error", "Precondition not met (data not loaded or empty)")
        return {
            "status": "error",
            "message": f"Cannot start {label}: precondition not met (data not loaded or empty)",
        }

    task = _get_pipeline_task(svc, pipeline_type, period)
    if task is None:
        # Shouldn't happen if started=True, but be safe
        pipeline_lock.release()
        history.finish_run(run_data, "error", "Task not found after start")
        return {"status": "error", "message": "Pipeline task not found after start"}

    # Spawn monitor — it holds the lock until the pipeline finishes
    asyncio.create_task(
        _monitor_pipeline(svc, pipeline_type, period, task, run_data, history, label),
        name=f"pipeline-monitor-{pipeline_type}-{period}",
    )

    return {"status": "started", "pipeline_type": pipeline_type, "period": period, "label": label}


@router.post("/api/pipeline/stop")
async def stop_pipeline(request: Request) -> dict:
    """Stop the currently running pipeline.

    Cancels all pipeline tasks. The _monitor_pipeline coroutine catches
    CancelledError and handles lock release + history recording. If the
    monitor doesn't release within 2 seconds, force-release as a safety net.
    """
    svc: DataService = request.app.state.data

    if not pipeline_lock.is_locked:
        return {"status": "ok", "message": "No pipeline running"}

    try:
        await svc.tisk_pipeline.cancel_all()
        svc.amendment_pipeline.cancel_all()

        # Yield to event loop so monitor coroutine can process cancellation
        await asyncio.sleep(0)

        # Safety timeout: if monitor doesn't release the lock, force-release
        for _ in range(20):
            if not pipeline_lock.is_locked:
                break
            await asyncio.sleep(0.1)
        else:
            logger.warning("[admin] Monitor did not release lock in time, force-releasing")
            pipeline_lock.release()
    except Exception as exc:
        logger.opt(exception=True).error("[admin] Pipeline stop failed")
        pipeline_lock.release()
        return {"status": "error", "message": str(exc)}

    return {"status": "stopped"}


@router.post("/api/pipeline/cancel/{period}")
async def cancel_period_pipeline(request: Request, period: int) -> dict:
    """Cancel a single period in the pipeline (running or pending)."""
    svc = request.app.state.data
    result = svc.cancel_period_pipeline(period)
    if not result["tisk"] and not result["amendment"]:
        return {"status": "noop", "message": f"Period {period} not running or pending"}
    return {"status": "cancelled", "period": period, **result}


@router.post("/api/pipeline/remove/{period}")
async def remove_pending_period(request: Request, period: int) -> dict:
    """Remove a pending period from the tisk pipeline queue."""
    svc = request.app.state.data
    removed = svc.remove_pending_period(period)
    if not removed:
        return {"status": "noop", "message": f"Period {period} is not pending"}
    return {"status": "removed", "period": period}


@router.get("/api/pipeline/status")
async def pipeline_status(request: Request) -> dict:
    """Get current pipeline status as JSON."""
    svc = request.app.state.data
    current = pipeline_lock.current
    tisk_progress = svc.tisk_pipeline.progress

    return {
        "running": pipeline_lock.is_locked,
        "current": {
            "pipeline_id": current.pipeline_id,
            "pipeline_type": current.pipeline_type,
            "period": current.period,
        }
        if current
        else None,
        "tisk_progress": tisk_progress.to_dict() if hasattr(tisk_progress, "to_dict") else {},
    }


@router.get("/api/pipeline/history")
async def pipeline_history_endpoint(request: Request) -> list[dict]:
    """Get recent pipeline run history."""
    history: PipelineHistory = request.app.state.pipeline_history
    return history.get_runs()


@router.get("/api/pipeline/logs")
async def pipeline_logs_sse(request: Request) -> StreamingResponse:
    """SSE endpoint for real-time pipeline log streaming."""
    return StreamingResponse(
        log_broadcaster.subscribe(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ── Config editor ────────────────────────────────────────────────


@router.get("/config", response_class=HTMLResponse)
async def config_page(request: Request) -> HTMLResponse:
    """Runtime config editor page."""
    svc = request.app.state.data
    config = load_runtime_config(svc.cache_dir)

    return templates.TemplateResponse(
        "config.html",
        {
            "request": request,
            "admin_user": get_session_username(request),
            "config": config.to_dict(mask_secrets=True),
        },
    )


@router.get("/api/config")
async def get_config(request: Request) -> dict:
    """Read runtime config (secrets masked)."""
    svc = request.app.state.data
    config = load_runtime_config(svc.cache_dir)
    return config.to_dict(mask_secrets=True)


@router.post("/api/config")
async def update_config(request: Request) -> dict:
    """Update runtime config from form data."""
    svc = request.app.state.data
    form = await request.form()
    current = load_runtime_config(svc.cache_dir)

    # Update fields from form (skip masked secrets)
    for field_name in RuntimeConfig.__dataclass_fields__:
        if field_name.startswith("_"):
            continue
        value = form.get(field_name)
        if value is None:
            # Checkboxes: absent = False
            if isinstance(getattr(current, field_name), bool):
                setattr(current, field_name, False)
            continue
        if value == "***":
            continue  # Skip masked secrets

        str_value = str(value)
        field_type = type(getattr(current, field_name))
        match field_type.__name__:
            case "bool":
                setattr(current, field_name, str_value in ("true", "1", "on"))
            case "int":
                with contextlib.suppress(ValueError):
                    setattr(current, field_name, int(str_value))
            case _:
                setattr(current, field_name, str_value)

    save_runtime_config(current, svc.cache_dir)
    apply_runtime_config(current)

    return {"status": "ok", "config": current.to_dict(mask_secrets=True)}


# ── Manual refresh ───────────────────────────────────────────────


@router.post("/api/refresh")
async def trigger_refresh(request: Request) -> dict:
    """Trigger manual data refresh."""
    svc = request.app.state.data
    try:
        await svc.refresh_all_data()
        return {"status": "ok", "message": "Data refresh complete"}
    except Exception as exc:
        logger.opt(exception=True).error("[admin] Manual refresh failed")
        return {"status": "error", "message": str(exc)}


# ── Health endpoint ──────────────────────────────────────────────


@router.get("/api/health")
async def admin_health(request: Request) -> dict:
    """Admin backend health check."""
    svc = request.app.state.data
    return {
        "status": "ok",
        "periods_loaded": svc.loaded_periods,
        "pipeline_running": pipeline_lock.is_locked,
    }

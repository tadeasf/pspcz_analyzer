"""Chart image endpoints — seaborn renders to PNG, served as StreamingResponse."""

import io

import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from matplotlib.figure import Figure

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.middleware import run_with_timeout
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.api import validate_period
from pspcz_analyzer.services.activity_service import compute_activity
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.attendance_service import compute_attendance
from pspcz_analyzer.services.loyalty_service import compute_loyalty
from pspcz_analyzer.services.similarity_service import compute_pca_coords

matplotlib.use("Agg")  # Non-interactive backend

router = APIRouter(tags=["Charts"])

# Light institutional style
sns.set_theme(style="whitegrid", palette="deep")


def _fig_to_png(fig: Figure) -> io.BytesIO:
    """Render a matplotlib figure to a PNG BytesIO buffer."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#FFFFFF")
    buf.seek(0)
    plt.close(fig)
    return buf


@router.get("/loyalty.png")
@limiter.limit("10/minute")
async def loyalty_chart(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = Query(default=20, ge=1, le=200),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"loyalty:{period}:{top}"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(key, lambda: compute_loyalty(pd, top=top)),
        timeout=20.0,
        label="loyalty chart",
    )

    fig, ax = plt.subplots(figsize=(12, max(6, len(rows) * 0.35)))
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#F7F7F7")

    names = [f"{r['jmeno']} {r['prijmeni']} ({r['party'] or '?'})" for r in rows]
    values = [r["rebellion_pct"] for r in rows]

    colors = sns.color_palette("coolwarm", len(rows))
    ax.barh(names[::-1], values[::-1], color=colors)
    ax.set_xlabel("Rebellion Rate (%)", color="#333333")
    ax.set_title("Top MP Rebels — Votes Against Party Line", color="#333333", fontsize=14)
    ax.tick_params(colors="#333333")
    for spine in ax.spines.values():
        spine.set_color("#D9D9D9")

    return StreamingResponse(_fig_to_png(fig), media_type="image/png")


@router.get("/attendance.png")
@limiter.limit("10/minute")
async def attendance_chart(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = Query(default=20, ge=1, le=200),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"attendance:{period}:{top}:worst"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            key, lambda: compute_attendance(pd, top=top, sort="worst")
        ),
        timeout=20.0,
        label="attendance chart",
    )

    fig, ax = plt.subplots(figsize=(12, max(6, len(rows) * 0.35)))
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#F7F7F7")

    names = [f"{r['jmeno']} {r['prijmeni']} ({r['party'] or '?'})" for r in rows]
    values = [r["attendance_pct"] for r in rows]

    colors = sns.color_palette("RdYlGn", len(rows))
    ax.barh(names[::-1], values[::-1], color=colors)
    ax.set_xlabel("Attendance Rate (%)", color="#333333")
    ax.set_title("Lowest Attendance — MPs Who Skip Votes", color="#333333", fontsize=14)
    ax.tick_params(colors="#333333")
    for spine in ax.spines.values():
        spine.set_color("#D9D9D9")

    return StreamingResponse(_fig_to_png(fig), media_type="image/png")


@router.get("/active.png")
@limiter.limit("10/minute")
async def active_chart(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = Query(default=25, ge=1, le=200),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"active:{period}:{top}:"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(key, lambda: compute_activity(pd, top=top)),
        timeout=20.0,
        label="activity chart",
    )

    fig, ax = plt.subplots(figsize=(12, max(6, len(rows) * 0.35)))
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#F7F7F7")

    names = [f"{r['jmeno']} {r['prijmeni']} ({r['party'] or '?'})" for r in rows]
    values = [r["active"] for r in rows]

    colors = sns.color_palette("viridis", len(rows))
    ax.barh(names[::-1], values[::-1], color=colors)
    ax.set_xlabel("Active Votes (YES + NO + ABSTAINED)", color="#333333")
    ax.set_title("Most Active MPs — Total Votes Cast", color="#333333", fontsize=14)
    ax.tick_params(colors="#333333")
    for spine in ax.spines.values():
        spine.set_color("#D9D9D9")

    return StreamingResponse(_fig_to_png(fig), media_type="image/png")


@router.get("/similarity.png")
@limiter.limit("10/minute")
async def similarity_chart(request: Request, period: int = DEFAULT_PERIOD):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"similarity_pca:{period}"
    coords = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(key, lambda: compute_pca_coords(pd)),
        timeout=30.0,
        label="similarity chart",
    )

    # Assign colors per party
    parties = sorted({c["party"] for c in coords})
    palette = dict(zip(parties, sns.color_palette("husl", len(parties)), strict=False))

    fig, ax = plt.subplots(figsize=(14, 10))
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#F7F7F7")

    for party in parties:
        pts = [c for c in coords if c["party"] == party]
        ax.scatter(
            [p["x"] for p in pts],
            [p["y"] for p in pts],
            label=party,
            color=palette[party],
            s=60,
            alpha=0.8,
            edgecolors="#333333",
            linewidths=0.5,
        )

    ax.set_xlabel("PC1", color="#333333")
    ax.set_ylabel("PC2", color="#333333")
    ax.set_title(
        "MP Voting Similarity — PCA Projection (colored by party)",
        color="#333333",
        fontsize=14,
    )
    ax.tick_params(colors="#333333")
    for spine in ax.spines.values():
        spine.set_color("#D9D9D9")

    legend = ax.legend(
        loc="upper right",
        fontsize=9,
        framealpha=0.9,
        facecolor="#FFFFFF",
        edgecolor="#D9D9D9",
    )
    for text in legend.get_texts():
        text.set_color("#333333")

    return StreamingResponse(_fig_to_png(fig), media_type="image/png")

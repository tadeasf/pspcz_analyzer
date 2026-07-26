"""Chart image endpoints — seaborn renders to PNG, served as StreamingResponse."""

import io

import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.i18n import gettext as _
from pspcz_analyzer.middleware import run_with_timeout
from pspcz_analyzer.rate_limit import limiter
from pspcz_analyzer.routes.utils import validate_period
from pspcz_analyzer.services.analysis_cache import analysis_cache
from pspcz_analyzer.services.attendance_service import compute_attendance
from pspcz_analyzer.services.loyalty_service import compute_loyalty
from pspcz_analyzer.services.similarity_service import compute_pca_coords

matplotlib.use("Agg")  # Non-interactive backend

router = APIRouter(tags=["Charts"])

# Light institutional style
sns.set_theme(style="whitegrid", palette="deep")

# Rendering itself is fast; the data computations above it have their own,
# longer timeouts.
_RENDER_TIMEOUT_S = 10.0


def _fig_to_png(fig: Figure) -> io.BytesIO:
    """Render a matplotlib figure to a PNG BytesIO buffer (does not close it)."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=200, bbox_inches="tight", facecolor="#FFFFFF")
    buf.seek(0)
    return buf


def _style_figure(fig: Figure, ax: Axes) -> None:
    """Apply the shared institutional look: backgrounds, ticks, spine colors."""
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#F7F7F7")
    ax.tick_params(colors="#333333")
    for spine in ax.spines.values():
        spine.set_color("#D9D9D9")


def _render_loyalty_png(rows: list[dict], xlabel: str, title: str) -> io.BytesIO:
    """Render the loyalty bar chart to PNG, closing the figure no matter what."""
    fig, ax = plt.subplots(figsize=(12, max(6, len(rows) * 0.35)))
    try:
        _style_figure(fig, ax)
        names = [f"{r['jmeno']} {r['prijmeni']} ({r['party'] or '?'})" for r in rows]
        values = [r["rebellion_pct"] for r in rows]
        colors = sns.color_palette("coolwarm", len(rows))
        ax.barh(names[::-1], values[::-1], color=colors)
        ax.set_xlabel(xlabel, color="#333333")
        ax.set_title(title, color="#333333", fontsize=14)
        return _fig_to_png(fig)
    finally:
        plt.close(fig)


def _render_attendance_png(
    rows: list[dict], field: str, palette: str, xlabel: str, title: str
) -> io.BytesIO:
    """Render the attendance bar chart to PNG, closing the figure no matter what."""
    # Fully-excused MPs have a null attendance rate (attendance_service) and
    # barh cannot plot None — nulls_last already ranks them last anyway.
    plottable = [r for r in rows if r[field] is not None]
    fig, ax = plt.subplots(figsize=(12, max(6, len(plottable) * 0.35)))
    try:
        _style_figure(fig, ax)
        names = [f"{r['jmeno']} {r['prijmeni']} ({r['party'] or '?'})" for r in plottable]
        values = [r[field] for r in plottable]
        colors = sns.color_palette(palette, len(plottable))
        ax.barh(names[::-1], values[::-1], color=colors)
        ax.set_xlabel(xlabel, color="#333333")
        ax.set_title(title, color="#333333", fontsize=14)
        return _fig_to_png(fig)
    finally:
        plt.close(fig)


def _render_similarity_png(coords: list[dict], xlabel: str, ylabel: str, title: str) -> io.BytesIO:
    """Render the similarity PCA scatter chart to PNG, closing the figure."""
    parties = sorted({c["party"] for c in coords})
    palette = dict(zip(parties, sns.color_palette("husl", len(parties)), strict=False))

    fig, ax = plt.subplots(figsize=(14, 10))
    try:
        _style_figure(fig, ax)
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

        ax.set_xlabel(xlabel, color="#333333")
        ax.set_ylabel(ylabel, color="#333333")
        ax.set_title(title, color="#333333", fontsize=14)

        legend = ax.legend(
            loc="upper right",
            fontsize=9,
            framealpha=0.9,
            facecolor="#FFFFFF",
            edgecolor="#D9D9D9",
        )
        for text in legend.get_texts():
            text.set_color("#333333")

        return _fig_to_png(fig)
    finally:
        plt.close(fig)


@router.get("/loyalty.png")
@limiter.limit("30/minute")
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

    # Resolve labels in request context (ContextVar propagated by run_with_timeout)
    xlabel = _("chart.loyalty.xlabel")
    title = _("chart.loyalty.title")

    buf = await run_with_timeout(
        lambda: _render_loyalty_png(rows, xlabel, title),
        timeout=_RENDER_TIMEOUT_S,
        label="loyalty chart render",
    )
    return StreamingResponse(buf, media_type="image/png")


@router.get("/attendance.png")
@limiter.limit("30/minute")
async def attendance_chart(
    request: Request,
    period: int = DEFAULT_PERIOD,
    top: int = Query(default=20, ge=1, le=200),
    sort: str = Query(default="worst", max_length=20),
    party: str = Query(default="", max_length=200),
):
    validate_period(period)
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    key = f"attendance:{period}:{top}:{sort}:{party}"
    rows = await run_with_timeout(
        lambda: analysis_cache.get_or_compute(
            key,
            lambda: compute_attendance(pd, top=top, sort=sort, party_filter=party or None),
        ),
        timeout=20.0,
        label="attendance chart",
    )

    chart_meta: dict[str, tuple[str, str, str]] = {
        # sort_key: (data_field, chart_key_prefix, palette)
        "worst": ("attendance_pct", "chart.attendance.worst", "RdYlGn"),
        "best": ("attendance_pct", "chart.attendance.best", "RdYlGn"),
        "most_active": ("active", "chart.attendance.most_active", "viridis"),
        "least_active": ("active", "chart.attendance.least_active", "viridis"),
        "most_abstained": ("abstained", "chart.attendance.most_abstained", "YlOrRd"),
        "most_excused": ("excused", "chart.attendance.most_excused", "PuBuGn"),
        "most_passive": ("passive", "chart.attendance.most_passive", "OrRd"),
        "most_absent": ("absent", "chart.attendance.most_absent", "Reds"),
        "most_yes": ("yes_votes", "chart.attendance.most_yes", "Greens"),
        "most_no": ("no_votes", "chart.attendance.most_no", "Blues"),
    }
    field, chart_key, palette = chart_meta.get(
        sort, ("attendance_pct", "chart.attendance.worst", "RdYlGn")
    )
    xlabel = _(f"{chart_key}.xlabel")
    title = _(f"{chart_key}.title")

    buf = await run_with_timeout(
        lambda: _render_attendance_png(rows, field, palette, xlabel, title),
        timeout=_RENDER_TIMEOUT_S,
        label="attendance chart render",
    )
    return StreamingResponse(buf, media_type="image/png")


@router.get("/similarity.png")
@limiter.limit("30/minute")
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

    # Resolve labels in request context (ContextVar propagated by run_with_timeout)
    xlabel = _("chart.similarity.xlabel")
    ylabel = _("chart.similarity.ylabel")
    title = _("chart.similarity.title")

    buf = await run_with_timeout(
        lambda: _render_similarity_png(coords, xlabel, ylabel, title),
        timeout=_RENDER_TIMEOUT_S,
        label="similarity chart render",
    )
    return StreamingResponse(buf, media_type="image/png")

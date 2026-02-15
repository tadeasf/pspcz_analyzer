"""Chart image endpoints — seaborn renders to PNG, served as StreamingResponse."""

import io

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import seaborn as sns
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from pspcz_analyzer.config import DEFAULT_PERIOD
from pspcz_analyzer.services.activity_service import compute_activity
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
async def loyalty_chart(request: Request, period: int = DEFAULT_PERIOD, top: int = 20):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = compute_loyalty(pd, top=top)

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
async def attendance_chart(request: Request, period: int = DEFAULT_PERIOD, top: int = 20):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = compute_attendance(pd, top=top, sort="worst")

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
async def active_chart(request: Request, period: int = DEFAULT_PERIOD, top: int = 25):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    rows = compute_activity(pd, top=top)

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
async def similarity_chart(request: Request, period: int = DEFAULT_PERIOD):
    data_svc = request.app.state.data
    pd = data_svc.get_period(period)
    coords = compute_pca_coords(pd)

    # Assign colors per party
    parties = sorted({c["party"] for c in coords})
    palette = dict(zip(parties, sns.color_palette("husl", len(parties))))

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

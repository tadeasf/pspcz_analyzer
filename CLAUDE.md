# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Czech Parliamentary Voting Analyzer — an OSINT tool that downloads, parses, and visualizes open data from psp.cz (Czech Parliament). Built as a FastAPI web app with Jinja2/HTMX frontend and Polars for data processing.

## Commands

- **Install dependencies:** `uv sync` (add `--extra dev` for test/lint tools)
- **Run the dev server:** `uv run python -m pspcz_analyzer.main` (starts on `0.0.0.0:8000` with reload)
- **Add a dependency:** `uv add <package>`
- **Run unit + API tests:** `uv run pytest -m "not integration" --cov`
- **Run integration tests:** `uv run pytest -m integration -v` (requires network — hits real psp.cz)
- **Lint:** `uv run ruff check .`
- **Format:** `uv run ruff format .`
- **Type check:** `uv run pyright`
- **Pre-commit:** `uv run pre-commit run --all-files`
- **Version bump:** `uv run bump-my-version bump patch|minor|major`

Python >= 3.12 required (pinned to 3.14 in `.python-version`).

## Architecture

### Data Pipeline

1. **`data/downloader.py`** — Downloads ZIP files from `psp.cz/eknih/cdrom/opendata` (voting, MPs, sessions, prints) via httpx
2. **`data/parser.py`** — Parses UNL files (pipe-delimited, Windows-1250 encoded, no headers, trailing pipe) into Polars DataFrames
3. **`data/cache.py`** — Parquet caching layer; re-parses only when source files are newer than cached parquet

Column definitions for all UNL tables live in `models/schemas.py`. Column names are Czech (matching psp.cz docs) for traceability.

### DataService (`services/data_service.py`)

Central orchestrator. Initialized at startup via FastAPI lifespan, stored on `app.state.data`. Manages:
- **Shared tables** (persons, MPs, organs, memberships) — loaded once across all periods
- **Per-period data** (`PeriodData` dataclass) — voting records, MP votes, void votes, MP info, tisk (print) lookups
- Periods are loaded on demand; `config.py` maps period numbers to years, organ IDs, and labels

### Analysis Services

Each in `services/`, each takes a `PeriodData` and returns `list[dict]`:
- **`loyalty_service`** — Rebellion rates (MP votes against party majority direction)
- **`attendance_service`** — Attendance % = active / (total - excused)
- **`similarity_service`** — Cosine similarity + SVD-based PCA on vote matrix (MPs × votes, +1/-1/0)
- **`activity_service`** — Raw vote participation volume
- **`votes_service`** — Vote search/list with pagination, vote detail with per-party breakdown

### Web Layer

- **`routes/pages.py`** — Full HTML page renders (Jinja2 templates)
- **`routes/api.py`** — HTMX partial endpoints returning HTML fragments
- **`routes/charts.py`** — Seaborn/matplotlib chart endpoints returning PNG via `StreamingResponse`
- Templates in `templates/`, partials in `templates/partials/`

### Configuration (`config.py`)

All psp.cz URLs, cache paths (`~/.cache/pspcz-analyzer/psp`), period-to-year mappings, and UNL format constants. The `PERIOD_ORGAN_IDS` map is critical — `id_obdobi` in the psp.cz database uses organ IDs, not period numbers.

### Domain Enums (`models/enums.py`)

Vote result codes: `A`=YES, `B`=NO, `C`=ABSTAINED, `F`=DID_NOT_VOTE, `@`=ABSENT, `M`=EXCUSED. Void votes (zmatecne) are always filtered out before analysis.

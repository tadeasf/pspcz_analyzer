# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Czech Parliamentary Voting Analyzer — an OSINT tool that downloads, parses, and visualizes open data from psp.cz (Czech Parliament). Built as a FastAPI web app with Jinja2/HTMX frontend and Polars for data processing. Supports Czech/English UI localization and bilingual AI summaries via Ollama.

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
- **Docker:** `docker compose up --build`

Python >= 3.12 required (pinned to 3.14 in `.python-version`).

## Configuration

Environment variables are loaded from `.env` via `python-dotenv` (see `.env.example`). Key variables:

- `PSPCZ_CACHE_DIR` — data cache directory (default: `~/.cache/pspcz-analyzer/psp`)
- `PSPCZ_DEV` — `1` for hot reload, `0` for production (default: `1`)
- `OLLAMA_BASE_URL` — Ollama API endpoint (default: `http://localhost:11434`)
- `OLLAMA_API_KEY` — Bearer token for remote HTTPS Ollama (default: empty)
- `OLLAMA_MODEL` — model name (default: `qwen3:8b`)

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

### i18n (`i18n/`)

Dict-based Czech/English localization using `contextvars.ContextVar` for per-request locale:
- **`i18n/__init__.py`** — `gettext()`, `ngettext()`, `setup_jinja2_i18n()` for Jinja2 i18n extension
- **`i18n/translations.py`** — all UI strings in `TRANSLATIONS["cs"]` / `TRANSLATIONS["en"]`
- **`i18n/middleware.py`** — `LocaleMiddleware` reads `lang` cookie, sets ContextVar + `request.state.lang`

Language is switched via `/set-lang/{lang}` which sets a cookie and redirects back.

### Analysis Services

Each in `services/`, each takes a `PeriodData` and returns `list[dict]`:
- **`loyalty_service`** — Rebellion rates (MP votes against party majority direction)
- **`attendance_service`** — Attendance %, vote breakdown (YES/NO/ABSTAINED), activity ranking
- **`similarity_service`** — Cosine similarity + SVD-based PCA on vote matrix (MPs × votes, +1/-1/0)
- **`votes_service`** — Vote search/list with pagination, vote detail with per-party breakdown

### Web Layer

- **`routes/pages.py`** — Full HTML page renders (Jinja2 templates) + `/set-lang/{lang}` endpoint
- **`routes/api.py`** — HTMX partial endpoints returning HTML fragments
- **`routes/charts.py`** — Seaborn/matplotlib chart endpoints returning PNG via `StreamingResponse`
- Templates in `templates/`, partials in `templates/partials/`
- All user-visible strings use `{{ _("key") }}` Jinja2 i18n calls

### Configuration (`config.py`)

Loads `.env` via `python-dotenv`. Contains psp.cz URLs, cache paths, period-to-year mappings, UNL format constants, and Ollama settings. The `PERIOD_ORGAN_IDS` map is critical — `id_obdobi` in the psp.cz database uses organ IDs, not period numbers.

### Domain Enums (`models/enums.py`)

Vote result codes: `A`=YES, `B`=NO, `C`=ABSTAINED, `F`=DID_NOT_VOTE, `@`=ABSENT, `M`=EXCUSED. Void votes (zmatecne) are always filtered out before analysis.

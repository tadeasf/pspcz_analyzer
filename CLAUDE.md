# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Czech Parliamentary Voting Analyzer — an OSINT tool that downloads, parses, and visualizes open data from psp.cz (Czech Parliament). Built as a FastAPI web app with Jinja2/HTMX frontend and Polars for data processing. Supports Czech/English UI localization and bilingual AI summaries via Ollama or any OpenAI-compatible API.

## Commands

- **Install dependencies:** `uv sync` (add `--extra dev` for test/lint tools)
- **Run the frontend:** `uv run python -m pspcz_analyzer.main_frontend` (starts on `0.0.0.0:8000` with reload)
- **Run the backend (admin):** `uv run python -m pspcz_analyzer.main_backend` (starts on `0.0.0.0:8001`)
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
- `PORT` — server port (default: `8000`)
- `LLM_PROVIDER` — LLM backend: `ollama` (default) or `openai`
- `OLLAMA_BASE_URL` — Ollama API endpoint (default: `http://localhost:11434`)
- `OLLAMA_API_KEY` — Bearer token for remote HTTPS Ollama (default: empty)
- `OLLAMA_MODEL` — model name for Ollama (default: `qwen3:8b`)
- `LLM_STRUCTURED_OUTPUT` — JSON schema structured output for all providers (`0` or `1`, default: `1`; backward-compat: reads `OLLAMA_STRUCTURED_OUTPUT` as fallback)
- `LLM_EMPTY_RETRIES` — extra attempts when free-text LLM path returns empty/unparseable results (default: `2`, `0` = no retries)
- `OPENAI_BASE_URL` — OpenAI-compatible API endpoint (default: `https://api.openai.com/v1`)
- `OPENAI_API_KEY` — API key for OpenAI-compatible backend (default: empty)
- `OPENAI_MODEL` — model name for OpenAI-compatible backend (default: `gpt-4o-mini`)
- `AI_PERIODS_LIMIT` — newest periods to process with AI, `0` = all (default: `3`)
- `DAILY_REFRESH_ENABLED` — `1` to enable daily data refresh, `0` to disable (default: `1`)
- `DAILY_REFRESH_HOUR` — hour (CET, 0-23) for daily refresh (default: `3`)
- `GITHUB_FEEDBACK_ENABLED` — enable feedback form (`0` or `1`, default: `0`)
- `GITHUB_FEEDBACK_TOKEN` — GitHub PAT for creating issues (default: empty)
- `GITHUB_FEEDBACK_REPO` — target repository (default: `tadeasf/pspcz_analyzer`)
- `GITHUB_FEEDBACK_LABELS` — labels for feedback issues (default: `user-feedback`)
- `TISK_SHORTENER` — truncate tisk text for LLM (`0` = full text, `1` = truncate, default: `0`)
- `ADMIN_PORT` — admin backend server port (default: `8001`)
- `ADMIN_USERNAME` — admin dashboard login username (default: `admin`)
- `ADMIN_PASSWORD_HASH` — bcrypt hash of the admin password (default: empty — login rejects all)
- `ADMIN_SESSION_SECRET` — HMAC secret for signing admin session cookies (default: auto-generated)
- `ADMIN_ALLOWED_IPS` — comma-separated IP/CIDR whitelist for admin access (default: `127.0.0.1,::1,172.16.0.0/12`)

## Architecture

### Data Pipeline

1. **`data/downloader.py`** — Downloads ZIP files from `psp.cz/eknih/cdrom/opendata` (voting, MPs, sessions, prints) via httpx
2. **`data/parser.py`** — Parses UNL files (pipe-delimited, Windows-1250 encoded, no headers, trailing pipe) into Polars DataFrames
3. **`data/cache.py`** — Parquet caching layer; re-parses only when source files are newer than cached parquet

Column definitions for all UNL tables live in `models/schemas.py`. Column names are Czech (matching psp.cz docs) for traceability.

### Dual-Process Architecture

The app runs as two separate FastAPI processes:
- **`main_frontend.py`** — Public web app (port 8000). Uses `DataReader` for read-only data access with file-watcher for hot-reloading pipeline outputs.
- **`main_backend.py`** — Admin dashboard (port 8001). Uses `DataService` (extends `DataReader`) with pipeline orchestration, daily refresh, and admin auth.

### DataReader / DataService (`services/data_reader.py`, `services/data_service.py`)

Two-tier data service:
- **`DataReader`** — Read-only base class. Loads shared tables, per-period data, watches filesystem for pipeline outputs. Used by the frontend.
- **`DataService`** — Extends `DataReader` with pipeline orchestration (tisk, amendment, daily refresh). Used by the backend.

Both are initialized at startup via FastAPI lifespan, stored on `app.state.data`. Manages:
- **Shared tables** (persons, MPs, organs, memberships) — loaded once across all periods
- **Per-period data** (`PeriodData` dataclass) — voting records, MP votes, void votes, MP info, tisk (print) lookups
- Periods are loaded on demand; `config.py` maps period numbers to years, organ IDs, and labels

### Admin Dashboard (`admin/`)

Password-protected admin interface (bcrypt auth + session cookies):
- **`admin/routes.py`** — Dashboard, config editor, log streaming, pipeline control
- **`admin/auth.py`** — `AdminAuthMiddleware` with IP whitelist + session management
- **`admin/log_stream.py`** — Real-time log broadcasting via SSE

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

### Tisk Pipeline (`services/tisk/`)

Background pipeline for parliamentary print (tisk) enrichment. Runs as asyncio tasks, coordinated by `TiskPipelineService`:
- **`pipeline.py`** — Orchestrator: download → extract → classify → summarize → consolidate → scrape histories
- **`classifier.py`** — LLM-based topic classification + consolidation, with keyword fallback via `topic_service`
- **`downloader_pipeline.py`** — Batch PDF download + text extraction per period
- **`metadata_scraper.py`** — Scrapes legislative histories + law changes from psp.cz HTML
- **`version_service.py`** — Downloads sub-tisk versions, generates bilingual LLM diff summaries
- **`cache_manager.py`** — Loads/caches topic classifications, summaries, version diffs, histories from Parquet/JSON/text files
- **`lookup_builder.py`** — Builds `(schuze, bod) → TiskInfo` lookup dicts linking votes to prints
- **`text_service.py`** — Cache/retrieval layer for extracted PDF text
- **`io/`** — Low-level I/O subpackage: `scraper.py` (tisk page scraping), `downloader.py` (PDF download), `extractor.py` (PDF text extraction), `history_scraper.py` (legislative history), `law_changes_scraper.py` (proposed law changes)

### Amendment Pipeline (`services/amendments/`)

Background pipeline for third-reading amendment voting analysis. Runs as asyncio tasks, coordinated by `AmendmentPipelineService`:
- **`pipeline.py`** — Orchestrator: identify → download PDFs → scrape steno → merge → resolve votes → resolve submitters → summarize → cache
- **`pdf_parser.py`** — Downloads and parses amendment PDFs, extracts amendment number, proposed changes, justification
- **`steno_scraper.py`** — Scrapes stenographic record pages from psp.cz for third-reading sessions
- **`steno_parser.py`** — Parses steno HTML into structured data: speakers, speech text, amendment references
- **`submitter_resolver.py`** — Resolves amendment submitter names to MP records, handles name variations
- **`coalition_service.py`** — Analyzes voting coalitions: which parties voted together per amendment
- **`cache_manager.py`** — Loads/saves amendment pipeline outputs as Parquet and JSON files

### Law Service (`services/law_service.py`)

Provides data for the laws browser. Loads tisk metadata and legislative histories, returns paginated/filterable bill lists.

### Amendment Service (`services/amendment_service.py`)

High-level service for web routes. Provides `get_amendment_bills()` (list bills with amendments) and `get_amendment_detail()` (full amendment data for a session/agenda point).

### LLM Integration (`services/llm/`)

Unified LLM client supporting Ollama and OpenAI-compatible providers:
- **`__init__.py`** — Public API re-exports (`LLMClient`, `create_llm_client`, `serialize_topics`, etc.)
- **`prompts.py`** — All prompt templates, JSON schemas, and formatting constants (~50 constants)
- **`client.py`** — `LLMClient` class, `create_llm_client()` factory, text sanitization/truncation helpers

### Feedback Service (`services/feedback_service.py`)

Submits user feedback as GitHub Issues. Controlled by `GITHUB_FEEDBACK_ENABLED`. Requires a `GITHUB_FEEDBACK_TOKEN` with `public_repo` scope.

### Security & Rate Limiting

- **`middleware.py`** — `SecurityHeadersMiddleware` adds CSP, HSTS, X-Content-Type-Options, X-Frame-Options, Referrer-Policy, and Permissions-Policy. XSS sanitization via nh3 for markdown content and `html.escape` for external data. CSRF protection via Origin/Referer validation on POST endpoints. Also `run_with_timeout` for ContextVar-safe thread execution.
- **`rate_limit.py`** — Per-endpoint rate limits via slowapi (e.g. 15/min for analysis APIs, 3/hour for feedback).

### Web Layer

- **`routes/pages.py`** — Full HTML page renders (Jinja2 templates) + `/set-lang/{lang}` endpoint
- **`routes/voting.py`** — HTMX partials for loyalty, attendance, similarity, votes
- **`routes/amendments.py`** — HTMX partials for amendment bills and coalitions
- **`routes/laws.py`** — HTMX partials for laws browser and law detail
- **`routes/tisk.py`** — HTMX partials for tisk text, evolution, related bills
- **`routes/feedback.py`** — Feedback submission endpoint (POST /api/feedback)
- **`routes/health.py`** — Health check, LLM health, LLM smoke test
- **`routes/utils.py`** — Shared utilities (`validate_period`, `_safe_url`)
- **`routes/charts.py`** — Seaborn/matplotlib chart endpoints returning PNG via `StreamingResponse`
- Templates in `templates/`, partials in `templates/partials/`
- All user-visible strings use `{{ _("key") }}` Jinja2 i18n calls

### Configuration (`config.py`)

Loads `.env` via `python-dotenv`. Contains psp.cz URLs, cache paths, period-to-year mappings, UNL format constants, and LLM settings (provider-agnostic `LLM_*` constants plus per-provider `OLLAMA_*` / `OPENAI_*` env vars). The `PERIOD_ORGAN_IDS` map is critical — `id_obdobi` in the psp.cz database uses organ IDs, not period numbers.

### Domain Enums (`models/enums.py`)

Vote result codes: `A`=YES, `B`=NO, `C`=ABSTAINED, `F`=DID_NOT_VOTE, `@`=ABSENT, `M`=EXCUSED. Void votes (zmatecne) are always filtered out before analysis.

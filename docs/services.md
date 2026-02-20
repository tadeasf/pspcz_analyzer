# Services

## Data Pipeline

Data flows through three stages before it reaches the analysis services:

```
psp.cz ZIPs  →  UNL files (extracted)  →  Polars DataFrames  →  Parquet cache
     ↓                  ↓                        ↓
 downloader.py      parser.py               cache.py
```

### 1. Download (`data/downloader.py`)

Downloads ZIP archives from `https://www.psp.cz/eknih/cdrom/opendata`:

- `hl-{year}ps.zip` — voting data for a specific period
- `poslanci.zip` — MP/person/organ/membership data (shared)
- `schuze.zip` — session and agenda data
- `tisky.zip` — parliamentary prints (bills, proposals)

Files are cached in `~/.cache/pspcz-analyzer/psp/raw/` (or `$PSPCZ_CACHE_DIR/raw/`). Skips download if the file already exists.

### 2. Parse (`data/parser.py`)

UNL files are pipe-delimited with no header row, Windows-1250 encoded, and have a trailing pipe on each line (producing an extra empty column that gets dropped).

- `parse_unl()` — parses a single file given column names and optional dtype casts
- `parse_unl_multi()` — parses multiple files matching a glob pattern and concatenates them (used for per-session vote files like `hl2025h1.unl`, `hl2025h2.unl`, ...)

CSV quoting is always disabled (`quote_char=None`) because UNL files never use CSV-style quoting — any double quotes in the data are literal characters.

### 3. Cache (`data/cache.py`)

`get_or_parse()` checks if a Parquet file exists and is newer than the source. If so, it loads from Parquet; otherwise it calls the parse function and caches the result. Cache lives at `~/.cache/pspcz-analyzer/psp/parquet/` (or `$PSPCZ_CACHE_DIR/parquet/`).

## DataService (`services/data_service.py`)

Central orchestrator, initialized at app startup via FastAPI lifespan and stored on `app.state.data`.

### Shared Tables

Loaded once, used across all periods:

- **osoby** — persons (id_osoba, name, etc.)
- **poslanec** — MP records linking person to period
- **organy** — organs/organizations (parties, committees, etc.)
- **zarazeni** — memberships (which person belongs to which organ)
- **schuze / bod_schuze / tisky** — sessions, agenda items, parliamentary prints

### Per-Period Data (`PeriodData`)

Loaded on demand when a period is first requested:

- **votes** (`hl_hlasovani`) — vote summaries (date, result, counts)
- **mp_votes** (`hl_poslanec`) — individual MP votes per vote event
- **void_votes** (`zmatecne`) — IDs of void votes (always filtered out)
- **mp_info** — derived table: id_poslanec → name + current party
- **tisk_lookup** — `dict[(schuze_num, bod_num), TiskInfo]` linking votes to parliamentary prints

### Tisk Lookup

Two strategies for linking votes to parliamentary prints:

1. **Primary**: schuze → bod_schuze → tisky (via session/agenda item IDs)
2. **Fallback**: text-match vote descriptions against tisk names (for new periods where schuze.zip hasn't been updated yet)

## i18n (`i18n/`)

Dict-based Czech/English UI localization. Language is determined per-request from a cookie.

### Architecture

- **`i18n/__init__.py`** — Core module: `contextvars.ContextVar` for locale, `gettext(key)` / `ngettext(singular, plural, n)` lookup functions, `setup_jinja2_i18n(env)` to install Jinja2 i18n extension
- **`i18n/translations.py`** — `TRANSLATIONS: dict[str, dict[str, str]]` with `"cs"` and `"en"` keys containing all UI strings
- **`i18n/middleware.py`** — `LocaleMiddleware(BaseHTTPMiddleware)` reads `lang` cookie, calls `set_locale()`, sets `request.state.lang`

### How It Works

1. `LocaleMiddleware` reads the `lang` cookie on each request (default: `"cs"`)
2. Sets `contextvars.ContextVar` so `gettext()` resolves the correct language
3. Jinja2 templates use `{{ _("key") }}` which calls `gettext()`
4. Chart labels and vote outcome labels also use `gettext()` for localization
5. The `/set-lang/{lang}` endpoint sets the cookie and redirects back

### ContextVar Propagation

`run_with_timeout` in `middleware.py` uses `contextvars.copy_context().run()` to propagate the locale ContextVar into thread pool workers, ensuring chart rendering and analysis computations use the correct language.

## Analysis Services

All services take a `PeriodData` instance and return `list[dict]`. Void votes are always excluded.

### Loyalty (`services/loyalty_service.py`)

Computes rebellion rates — how often an MP votes against their party's majority.

1. Filter to active votes only (YES or NO; abstentions excluded)
2. For each (vote, party) pair, determine majority direction (YES or NO; ties excluded)
3. An MP "rebels" when their vote differs from the party majority
4. `rebellion_pct = rebellions / active_votes_with_clear_direction * 100`

Supports filtering by party code.

### Attendance (`services/attendance_service.py`)

Computes participation rates with category breakdowns and vote type breakdown (YES/NO/ABSTAINED).

Vote categories:
- **Active**: YES (`A`), NO (`B`), ABSTAINED (`C`)
- **Passive**: registered but no button press (`F`)
- **Absent**: not registered (`@`)
- **Excused**: formally excused (`M`)

Formula: `attendance_pct = active / (total - excused) * 100`

Excused absences are excluded from the denominator (legitimate absences don't penalize).

Sort modes:
- `worst` — lowest attendance first (default)
- `best` — highest attendance first
- `most_active` — ranked by raw volume of active votes (YES + NO + ABSTAINED), rewarding consistent long-term participation

Supports filtering by party code.

### Similarity (`services/similarity_service.py`)

Two outputs from the same vote matrix (MPs x votes, values: +1 YES, -1 NO, 0 other):

**PCA Projection** (`compute_pca_coords`):
- Centers the matrix, runs SVD, projects to 2D
- Returns (x, y) coordinates per MP for scatter plot visualization

**Cross-Party Pairs** (`compute_cross_party_similarity`):
- Computes cosine similarity between all MP pairs
- Filters to cross-party pairs only
- Returns top N most similar pairs

### Votes (`services/votes_service.py`)

**`list_votes()`** — paginated vote listing with text search, outcome filtering, and topic filtering. Enriches each row with tisk links (to psp.cz source documents). Outcome labels are localized via `gettext()`.

**`vote_detail()`** — full breakdown of a single vote: metadata, per-party aggregates (YES/NO/ABSTAINED/etc. per party), per-MP individual votes, legislative history timeline, bilingual AI summary, and topic labels.

## Tisk Pipeline Services

The tisk (parliamentary print) pipeline runs as a background process, downloading PDFs, extracting text, classifying topics, generating bilingual summaries, and scraping legislative histories.

### Tisk Pipeline Service (`services/tisk_pipeline_service.py`)

Background processing orchestrator that coordinates the full tisk data enrichment pipeline. Started automatically at app startup for all periods (newest first).

Pipeline stages per period:
1. **Download** — fetch PDF documents from psp.cz for each print
2. **Extract** — convert PDFs to plain text using PyMuPDF
3. **Classify** — assign topic labels via Ollama LLM (or keyword fallback)
4. **Summarize** — generate bilingual (Czech + English) summaries via Ollama
5. **Consolidate** — merge per-tisk topic classifications into a single Parquet cache
6. **Scrape histories** — fetch legislative process timelines from psp.cz HTML pages

Key class: `TiskPipelineService`
- `start_period(period)` — launch background pipeline for a single period
- `start_all_periods()` — launch pipeline for all configured periods sequentially
- `is_running(period)` — check if a period's pipeline is still running
- `cancel_all()` — cancel all running pipeline tasks (used by the daily refresh service)

### Tisk Text Service (`services/tisk_text_service.py`)

Cache and retrieval layer for extracted tisk PDF text. Used by the `/api/tisk-text` endpoint for lazy-loading on vote detail pages.

Key class: `TiskTextService`
- `get_text(period, ct)` — retrieve cached plain text for a print, or `None` if not yet extracted
- `has_text(period, ct)` — check if text exists in cache
- `available_tisky(period)` — list all print numbers with cached text

Text files are stored at `~/.cache/pspcz-analyzer/psp/tisky_text/{period}/{ct}.txt`.

### Topic Service (`services/topic_service.py`)

Keyword-based topic classifier — the fast, offline fallback when Ollama is unavailable.

Uses a `TOPIC_TAXONOMY` dictionary mapping topic labels to keyword lists. A tisk is assigned a topic if its name or extracted text contains any of the topic's keywords.

Functions:
- `classify_tisk(name, text)` — returns all matching topic IDs with match counts
- `classify_tisk_primary_label(name, text)` — returns `(label_cs, label_en)` tuple of the best-matching topic, or `None`

### Ollama Service (`services/ollama_service.py`)

LLM-based topic classification, summarization, and version comparison using Ollama. This is optional — if Ollama is not running, the system falls back to keyword classification.

Supports both local Ollama (no auth) and remote HTTPS Ollama (Bearer token authentication via `OLLAMA_API_KEY`).

Key class: `OllamaClient`
- `is_available()` — health check against the Ollama API (with auth headers if configured)
- `classify_topics(name, text)` — LLM-based multi-label topic classification
- `summarize(name, text)` — generate a concise Czech summary
- `summarize_en(name, text)` — generate a concise English summary
- `summarize_bilingual(name, text)` — generate summaries in both Czech and English
- `compare_versions(text1, text2)` — generate a Czech diff summary between two tisk versions
- `compare_versions_bilingual(text1, text2)` — generate diff summaries in both Czech and English
- `consolidate_topics(topics_by_ct)` — ask the LLM to merge/deduplicate topic labels across a period

#### Diagnostic Endpoints

Two JSON endpoints for verifying Ollama connectivity without running the full tisk pipeline:

- **`GET /api/ollama/health`** — Connection check. Returns `{"available": true/false, "base_url": "...", "model": "..."}`. Rate limit: 10/minute.
- **`GET /api/ollama/smoke-test`** — Concurrent bilingual generation test using a hardcoded Czech legislative sample. Fires two parallel LLM calls (Czech + English summaries) and measures wall-clock time. Returns `{"success": true, "model": "...", "duration_seconds": 4.2, "summary_cs": "...", "summary_en": "...", ...}`. Returns 503 if Ollama is down, 502 on generation failure. Rate limit: 2/minute.

Configuration (in `config.py`, overridable via `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_API_KEY` | *(empty)* | Bearer token for remote HTTPS Ollama |
| `OLLAMA_MODEL` | `qwen3:8b` | Model for inference |
| `OLLAMA_TIMEOUT` | `300.0` | Per-request timeout in seconds |
| `OLLAMA_MAX_TEXT_CHARS` | `50000` | Max text length sent to LLM |
| `OLLAMA_VERBATIM_CHARS` | `40000` | Chars included verbatim (rest truncated) |

If Ollama is not running or unreachable, the system silently falls back to keyword-based classification.

### Tisk Version Service (`services/tisk_version_service.py`)

Compares different versions (sub-tisky) of the same parliamentary print using LLM-generated diff summaries. Produces bilingual (Czech + English) comparison summaries stored as separate text files.

### Tisk Cache Manager (`services/tisk_cache_manager.py`)

Manages loading and caching of tisk enrichment data (topic classifications, summaries, English summaries, version diffs, legislative histories) from the file-based cache.

## Daily Refresh Service (`services/daily_refresh_service.py`)

Asyncio-based daily scheduler that re-downloads fresh data from psp.cz and reloads all in-memory state. Ensures the app serves up-to-date voting data without manual restarts.

### How It Works

1. Sleeps until the configured hour (default: 03:00 CET)
2. Pauses the tisk AI pipeline via `TiskPipelineService.cancel_all()`
3. Re-downloads shared tables (MPs, organs, sessions, tisky) with `force=True`
4. Re-downloads and reloads each loaded electoral period's voting data
5. Invalidates all analysis caches
6. Restarts the tisk pipeline (resumes incrementally — no AI work is lost)

Key class: `DailyRefreshService`
- `start()` — start the scheduler loop (idempotent, respects `DAILY_REFRESH_ENABLED`)
- `stop()` — cancel the scheduler gracefully
- `trigger_now()` — manually trigger an immediate refresh (for admin/debug use)

Configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `DAILY_REFRESH_ENABLED` | `1` | `1` to enable, `0` to disable |
| `DAILY_REFRESH_HOUR` | `3` | Hour (CET, 0-23) at which the daily refresh runs |

The refresh itself takes ~1-5 minutes (downloads + parsing). The tisk AI pipeline's incremental resume logic (Parquet checkpointing, JSON caching, file caching) ensures no AI work is redone after restart.

## Data Enrichment Modules

### Tisk Downloader (`data/tisk_downloader.py`)

Downloads PDF documents for parliamentary prints from psp.cz.

- `download_tisk_pdf(period, ct)` — download a single print's PDF
- `download_period_tisky(period, ct_list)` — batch download for a period

PDFs are cached at `~/.cache/pspcz-analyzer/psp/tisky_pdf/{period}/{ct}.pdf`.

### Tisk Extractor (`data/tisk_extractor.py`)

Extracts plain text from downloaded PDF files using PyMuPDF (fitz).

- `extract_text_from_pdf(pdf_path)` — extract text from a single PDF
- `extract_and_cache(period, ct)` — extract and save to the text cache
- `extract_period_texts(period)` — batch extract all PDFs for a period

### Tisk Scraper (`data/tisk_scraper.py`)

Scrapes psp.cz HTML pages to discover available PDF documents for a given print.

- `scrape_tisk_documents(period, ct)` — returns list of `TiskDocument` objects (URLs, types)
- `get_best_pdf(documents)` — selects the most relevant PDF from available documents

### History Scraper (`data/history_scraper.py`)

Scrapes legislative process history from psp.cz HTML pages for each parliamentary print.

- `scrape_tisk_history(period, ct)` — returns a `TiskHistory` object with a list of `TiskHistoryStage` entries
- `save_history_json(period, ct, history)` / `load_history_json(period, ct)` — JSON cache persistence

Each `TiskHistoryStage` contains:
- `stage_type` — e.g. "1. čtení", "2. čtení", "3. čtení", "Senát", "Prezident"
- `label` — human-readable label
- `date` — when the stage occurred
- `outcome` — result text (approved, rejected, etc.)
- `vote_number` — link to the specific vote, if applicable

### Law Changes Scraper (`data/law_changes_scraper.py`)

Scrapes zakon.cz to discover laws affected by a parliamentary print and find related bills.

- `scrape_law_changes(period, ct)` — returns list of law change dicts (law name, amendment type)
- `scrape_related_bills(idsb)` — discovers related bills via zakon.cz cross-references
- `save_related_bills_json()` / `load_related_bills_json()` — JSON cache persistence

Related bills are cached at `~/.cache/pspcz-analyzer/psp/tisky_meta/{period}/tisky_related_bills/{idsb}.json`.

## Feedback Service (`services/feedback_service.py`)

Submits user feedback as GitHub Issues via the GitHub API.

Key class: `GitHubFeedbackClient`
- `submit(title, body, vote_id, period, page_url)` — creates a GitHub issue with metadata labels
- Requires `GITHUB_FEEDBACK_ENABLED=1` and a valid `GITHUB_FEEDBACK_TOKEN`
- Issues are labeled with `GITHUB_FEEDBACK_LABELS` (comma-separated)

Configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_FEEDBACK_ENABLED` | `0` | Enable feedback submission |
| `GITHUB_FEEDBACK_TOKEN` | *(empty)* | GitHub PAT with `public_repo` scope |
| `GITHUB_FEEDBACK_REPO` | `tadeasf/pspcz_analyzer` | Target repository |
| `GITHUB_FEEDBACK_LABELS` | `user-feedback` | Labels for issues |

## Security & Middleware

### Security Headers (`middleware.py`)

`SecurityHeadersMiddleware` adds security headers to all responses:
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Referrer-Policy: strict-origin-when-cross-origin`
- `Content-Security-Policy` — restricts script/style/image/font/connect sources to `'self'` (with `https://unpkg.com` for HTMX and `'unsafe-inline'` for inline scripts/styles)
- `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- `Permissions-Policy: camera=(), microphone=(), geolocation=(), payment=()`

### CSRF Protection

POST endpoints validate the `Origin` and `Referer` headers against the request host. Requests with mismatched or missing origins are rejected with an error message (HTTP 200 with an HTMX error partial).

### XSS Sanitization

- **Markdown content** — rendered via `markdown` library, then sanitized through `nh3` (Rust-based HTML sanitizer) before being marked safe for Jinja2
- **External data** — user-supplied and psp.cz-sourced text is escaped via `html.escape()` before rendering

### Rate Limiting (`rate_limit.py`)

Per-endpoint rate limits via slowapi/limits. Each API endpoint declares its own limit via `@limiter.limit()` decorator.

### Context-Aware Timeout (`middleware.py`)

`run_with_timeout(func, timeout, *args)` runs a synchronous function in a thread pool with a timeout. Uses `contextvars.copy_context().run()` to propagate the locale ContextVar into worker threads.

## Analysis Cache (`services/analysis_cache.py`)

In-memory TTL cache (1-hour default) for analysis results. Prevents recomputing loyalty, attendance, similarity, and vote list results on every request.

Key class: `AnalysisCache`
- `get_or_compute(key, compute_fn)` — returns cached result or computes and caches
- `invalidate_all()` — clears all cached results (called by daily refresh)

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
- **Check cached statuses against psp.cz:** `uv run python -m scripts.check_statuses --period 10 [--limit N]` (exit 1 on differences, #86/#90)
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
- `PSPCZ_DEV` — `1` for hot reload, `0` for production (default: `1`; exposed as `config.DEV`)
- `PSPCZ_CACHE_HOST_DIR` — Docker only: host directory bind-mounted to `/data/cache` (default: `./cache-data`; set an absolute path to put the cache on another disk)
- `PORT` — server port (default: `8000`)
- `GIT_SHA` — commit the Docker image was built from (build arg, `GIT_SHA=$(git rev-parse HEAD) docker compose up -d --build`); shown as `version` in both health endpoints and in the footer (default: `unknown`)
- `LLM_PROVIDER` — LLM backend: `ollama` (default) or `openai`
- `OLLAMA_BASE_URL` — Ollama API endpoint (default: `http://localhost:11434`)
- `OLLAMA_API_KEY` — Bearer token for remote HTTPS Ollama (default: empty)
- `OLLAMA_MODEL` — model name for Ollama (default: `qwen3:8b`)
- `LLM_STRUCTURED_OUTPUT` — JSON schema structured output for all providers (`0` or `1`, default: `1`; backward-compat: reads `OLLAMA_STRUCTURED_OUTPUT` as fallback)
- `LLM_EMPTY_RETRIES` — extra attempts when free-text LLM path returns empty/unparseable results (default: `2`, `0` = no retries)
- `LLM_MAX_CONSECUTIVE_FAILURES` - failed LLM requests in a row (timeout, connection error, HTTP 5xx/429; logged at WARNING) after which the request raises `LLMUnavailable` and the pipelines and `scripts/build_and_backup_cache.py` stop instead of storing empty results (default: `3`, `services/llm/health.py`)
- `LLM_PROBE_TIMEOUT` - seconds for the tiny real completion `is_available()` sends instead of listing models (default: `60`)
- `LLM_CONCURRENCY` - parallel LLM requests per pipeline stage (briefs, classification, version diffs, vote steno, amendment summaries) via `llm.llm_map` (default: `4`)
- `LLM_ENABLE_THINKING` - Qwen3 thinking switch sent as `chat_template_kwargs` on the OpenAI-compatible path: `0` off, `1` on, unset = not sent (default: unset; set `0` for `qwen3_8_flash`, whose thinking runs away on tisk prompts)
- `LLM_MAX_OUTPUT_TOKENS` - reply token cap sent with every OpenAI-compatible request (default: `8192`; without it the hlidacstatu proxy reserves 131072 reply tokens and halves the usable 262k context)
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
- `ADMIN_ALLOWED_IPS` — comma-separated IP/CIDR whitelist for admin access (default: `127.0.0.1,::1` — loopback only; the old default included `172.16.0.0/12`)
- `ADMIN_TRUSTED_PROXIES` — comma-separated IP/CIDR of reverse proxies whose `X-Forwarded-For` is trusted to identify the client (default: `127.0.0.1,::1`; XFF from any other peer is ignored)
- `ADMIN_COOKIE_SECURE` — send the admin session cookie only over HTTPS (default: `1` outside dev mode)
- `RATE_LIMIT_TRUSTED_PROXIES` — comma-separated IP/CIDR of proxies whose `X-Forwarded-For` may key rate-limit buckets (default: empty — buckets key on the TCP peer, spoof-proof; set to the proxy's address behind a reverse proxy)
- `STENO_NEGATIVE_CACHE_TTL` — seconds a missing steno page (HTTP 404) stays negatively cached (default: `604800` = 7 days; only 404s are cached — timeouts/5xx always retry)
- `STENO_RECHECK_UNAUTHORIZED_DAYS` / `STENO_RECHECK_AUTHORIZED_DAYS` / `STENO_RECHECK_FINAL_DAYS` - days until a cached steno sub-page is fetched again, by its psp.cz state: unauthorized, authorized but not proofread, final (defaults: `1` / `14` / `90`)
- `COMPUTE_POOL_WORKERS` — thread-pool size behind `run_with_timeout` (default: `0` = Python's `min(32, cpu_count + 4)`; previously hard-coded to 2)
- `ANALYSIS_CACHE_MAX_ENTRIES` — max analysis cache entries before LRU eviction (default: `512`)

## Architecture

### Data Pipeline

1. **`data/downloader.py`** — Downloads ZIP files from `psp.cz/eknih/cdrom/opendata` (voting, MPs, sessions, prints) via httpx
2. **`data/parser.py`** — Parses UNL files (pipe-delimited, Windows-1250 encoded, no headers, trailing pipe) into Polars DataFrames
3. **`data/cache.py`** — Parquet caching layer; re-parses only when source files are newer than cached parquet

All pipeline writes go through **`fileio.py`** — atomic (same-dir temp + `os.replace`) helpers `write_parquet_atomic` / `write_json_atomic` / `stream_to_atomic` plus `assert_zip_intact`, so the frontend file watcher can never read a torn file and a crashed/interrupted download never leaves a partial archive.

Column definitions for all UNL tables live in `models/schemas.py`. Column names are Czech (matching psp.cz docs) for traceability.

### Dual-Process Architecture

The app runs as two separate FastAPI processes:
- **`main_frontend.py`** — Public web app (port 8000). Uses `DataReader` for read-only data access with file-watcher for hot-reloading pipeline outputs.
- **`main_backend.py`** — Admin dashboard (port 8001). Uses `DataService` (extends `DataReader`) with pipeline orchestration, daily refresh, and admin auth.

### DataReader / DataService (`services/data_reader.py`, `services/data_service.py`)

Two-tier data service:
- **`DataReader`** — Read-only base class. Loads shared tables, per-period data, watches filesystem for pipeline outputs. Used by the frontend.
- **`DataService`** — Extends `DataReader` with pipeline orchestration (tisk, amendment, daily refresh). Used by the backend. The daily refresh (`refresh_all_data`) re-downloads shared tables + current-period voting, then re-runs the current-period tisk + amendment pipelines in **refresh-active mode**: brand-new tisky are processed and any bill whose status is non-terminal (not in `TERMINAL_TISK_STATUSES`) is re-scraped and re-analyzed (histories, law changes, sub-tisk versions, AI summaries/diffs, amendments). Terminal bills stay served from cache. The `refresh_active` flag threads through `start_tisk_pipeline`/`start_amendment_pipeline` → the pipeline stages → `scrape_histories_sync`/`scrape_law_changes_sync`/`classify_and_save`/`analyze_version_diffs_sync` and the amendment `identifier`.

Both are initialized at startup via FastAPI lifespan, stored on `app.state.data`. Manages:
- **Shared tables** (persons, MPs, organs, memberships) — loaded once across all periods
- **Per-period data** (`PeriodData` dataclass) — voting records, MP votes, void votes, MP info, tisk (print) lookups
- Periods are loaded on demand; `config.py` maps period numbers to years, organ IDs, and labels

### Admin corrections (`admin/corrections.py`, `services/overrides.py`)

`/admin/corrections` edits `overrides/{period}.json` in the cache dir (`{kind: {key: {...fields, by, at}}}`; kinds `vote_question`, `brief` (review `ok`/`hidden`/`edited`), `amendment_summary` (`"ct:LETTER"`), `homepage_pick`). `DataReader._refresh_tisk_data` applies it on every `get_period` on top of the briefs / vote steno / amendment data and clears `analysis_cache` when the overrides file or the briefs / vote-steno folders change. Pipelines never write this file, so corrections survive reruns. Edited texts are labelled "zkontroloval člověk". A `vote_question` correction may also carry `pro`, `pro_en` and `subkind`. `/admin/quality` lists the single votes with an uncertain or unknown subject and the amendment letters with an uncertain or undetermined fate, to correct here, counts steno pages per authorization state, and lists corrections whose steno text changed after them (old and new AI wording, `admin/steno_review.py`) until the correction is saved again; the correction stays on the site meanwhile.

### Unvoted amendments (`services/amendments/pending.py`, #112)

Bills with a consolidated amendment document (history "zpracovány jako tisk N/X") but no 3rd reading get a `BillAmendmentData` keyed `(PENDING_SCHUZE=0, ct)` (`is_pending`), letters from the PDF, fate `pending` ("Zatím se nehlasovalo"). The parse stage rebuilds the whole list, so the entry disappears once the bill is voted.

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
- **`i18n/glossary.py`** - the guide's glossary (14 terms plus readings and the lead committee), the one source of term texts (#118/#106): the guide renders it and the `term(slug)` macro in `macros.html` shows an entry as an explanation on any page (Jinja globals `gloss`, `gloss_entry`, `gloss_order`)

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
- **`io/`** — Low-level I/O subpackage: `scraper.py` (tisk page scraping), `downloader.py` (PDF download), `extractor.py` (PDF text extraction), `history_scraper.py` (legislative history; stages are typed by (section, mark), and a "předložil Senátu" entry of a Senate bill or treaty is `predloženo Senátu`, not the Chamber's hand-over; bump `HISTORY_PARSER_VERSION` to re-scrape all), `law_changes_scraper.py` (proposed law changes)

### Amendment Pipeline (`services/amendments/`)

Background pipeline for third-reading amendment voting analysis. Runs as asyncio tasks, coordinated by `AmendmentPipelineService`:
- **`pipeline.py`** — Orchestrator: identify → download PDFs → scrape steno → merge → resolve votes → resolve submitters → summarize → cache
- **`pdf_parser.py`** — Downloads and parses amendment PDFs, extracts amendment number, proposed changes, justification
- **`steno_scraper.py`** — Scrapes stenographic record pages from psp.cz for third-reading sessions
- **`steno_parser.py`** — Parses steno HTML into structured data: speakers, speech text, amendment references
- **`submitter_resolver.py`** — Resolves amendment submitter names to MP records, handles name variations
- **`coalition_service.py`** — Analyzes voting coalitions: which parties voted together per amendment
- **`cache_manager.py`** — Loads/saves amendment pipeline outputs as Parquet and JSON files

### Pipeline Cancellation (`services/cancellation.py`)

Shared cooperative cancellation for both pipeline services. Pipeline bodies run in worker threads (`asyncio.to_thread`), where `asyncio.Task.cancel()` cannot interrupt them — so each running period owns a `CancellationFlag` that `cancel_period()`/`cancel_all()` flip instantly; pipeline stages call `check()` at every stage boundary and loop top, exiting promptly with `PipelineCancelled` (caught by the service → `CANCELLED` status). Both services also expose `async wait_stopped(timeout)` to drain tasks after `cancel_all()` (hard-cancelling whatever is still running after the timeout); used by the admin stop endpoint, daily refresh, and backend lifespan shutdown.

### AI brief per bill (`services/tisk/brief.py`, #109/#97)

Pipeline stage `BRIEFS` (after topic consolidation) writes `tisky_meta/{period}/briefs/{ct}.json` for bills (`id_druh` 1-3): plain title, one/two-sentence summary, who it concerns, amounts, proposed start date, cs + en, with model, date and source text hash. `brief_issues`/`text_issues` flag non-Latin script, English in Czech fields (two common English words or one English-only form such as "-tion"), stray signs ("×" outside "3 × 2"), an unfinished summary sentence and numbers missing from the source (a dictionary check was tried and dropped: half the briefs failed it); flagged output is retried, then stored with `status: warning` and shown with a warning. Regenerated when `BRIEF_VERSION` or the text changes. The laws list/detail/homepage/search use the brief; the old critical `summary` is no longer shown. Amendment letter summaries use the same check (`AmendmentVote.summary_warning`).

### Vote questions from steno (`services/vote_steno.py`, #102)

Pipeline stage `VOTE_STENO` crawls (`services/vote_steno_pages.py`) every sitting's steno sub-pages (whole numeric range between the first and last page linked from the day pages, plus up to 2 day pages after the last linked one, which the index sometimes omits; index/day pages of sittings from the last 21 days, or with votes missing from the cached pages, are re-fetched). `services/vote_steno_text.py` (no LLM) anchors each vote at its announcement (`<a href="/sqw/hlasy.sqw?G=<id>" id="hN">` followed by "přihlášeno"); links to older votes inside speeches neither cut nor overwrite windows. The same anchoring feeds the amendment parser (`normalize_vote_anchors` in `find_steno_for_bod`). The LLM (prompt in `llm/prompts.py`) gives kind (`final`/`amendment`/`reject`/`return`/`committee`/`procedural` + sub-kind), question, what "Pro" meant, proposer (kept only when in the text) and a verbatim quote; no quote in the steno, a result formula or a bare "o mém návrhu" quote = `status: unknown`. A failed LLM call (timeout, transport error, empty or unreadable reply) raises `NoAnswer`: the vote is not stored (an existing record is kept), so the next run asks again; only an answer that fails the quote checks is stored as `unknown`. Referential lead-ins get earlier proposals of the sitting and, on a second pass, the questions of the votes just before; an answer quoted only from that earlier context is `status: uncertain`. `link_repeats` marks challenged votes (`invalid_by`, `repeated_as`, `repeats`; the repeat takes over a sure subject of the challenged vote) and chair-repeated ones (`chair_repeated_as`, not called void). The proposer is the speaker of a first-person proposal (`quote_speaker`), else the MPs the chair names in the quote (`quote_named`), else the model's answer; challengers are the MPs who said "zpochybňuji hlasování". `vote_context.proposer_view` shows them with their club on the vote day and the verb form (Navrhl / Navrhla / Navrhli, from `mp_info.pohlavi`). Steno authorization (`services/steno_page_state.py`): every sub-page's psp.cz note (`<p class="status">` after `<a name="_h">`: "Neprošlo jazykovou korekturou, neautorizováno!", "Neautorizováno!", "Autorizováno, neprošlo jazykovou korekturou!", none = final) and a hash of its speech text (between `<!-- ex -->` and `<!-- sy -->`, quotes/dashes/spaces unified) are kept in `steno/{period}/state_{schuze}.json`; `steno_scraper._download_subpage_tracked` re-fetches a cached page when it has no entry yet or its `next_check` came (intervals by state, see `STENO_RECHECK_*`), for both the vote steno and the amendment pipeline. A first hash is a baseline; a different one sets `changed_at` and `amend_stale`. Vote records also store `body_sha` (window hash on the speech text, `services/vote_steno_changes.py`): a changed `sha` with an unchanged `body_sha` reuses the record (no LLM call), a changed speech re-extracts and keeps the replaced record under `previous`; `steno_status` is the state of the quote's page (vote detail shows a note while it is unauthorized). The amendment parse stage does not carry summaries of bills whose pages are `amend_stale` (`amendments/steno_changes.py`) and lists their corrected letters in `amendments/{period}/steno_changes.json`. Index and day pages are still re-fetched by age (`RECENT_DAYS`). Per agenda item, anchored vs `hl_hlasovani` counts are logged and written to `vote_steno/{period}/checks/{schuze}.json` (admin data quality page). Output `vote_steno/{period}/{schuze}.json`, loaded into `PeriodData.vote_steno`. Speech counts per item (incl. merged debates, counted for every merged item) live in `services/speeches.py`. `services/vote_context.py` turns a record into the vote kind, the six-way kind filter of the votes list, the item result and the vote detail story (consequence, what Pro meant, invalid/repeat links).

### Law Service (`services/law_service.py`)

Provides data for the laws browser. Loads tisk metadata and legislative histories, returns paginated/filterable bill lists. Each law row carries its linked amendment count and side tally, `(schuze, bod)` link, the phase label and the last step as a sentence. `services/step_text.py` turns history steps into sentences with a verb ("Zákon dostal k podpisu prezident. Rozhodnout musí do 14. 11.") and phase labels (`phl.*`); `services/law_detail.py` builds the law detail parts: timeline by Chamber / Senate / President with the dashed next step, the President's deadline, votes by reading, document versions from the history, amended laws and e-Sbírka links. Tisky on no approved agenda are in `tisk_lookup` under `(UNSCHEDULED_SCHUZE=-1, ct)` (`lookup_builder.add_unscheduled`) so the list and detail show them.

### Amendment Service (`services/amendment_service.py`)

High-level service for web routes. Provides `get_amendment_bills()` (list bills with amendments) and `get_amendment_detail()` (full amendment data for a session/agenda point). Each amendment/bill dict is tagged with a `driver` (`government`/`opposition`/`mixed`/`unknown`) via `affiliation`. The `driver_badge` macro (macros.html) renders per-amendment and per-bill badges and is **deliberately silent for `unknown`** (e.g. periods where the coalition cannot be derived from the confidence vote) — a neutral badge on every row would be noise; the `.driver-unknown` style and `driver.unknown` key exist for the rare cases a badge is wanted anyway.

### Affiliation Service (`services/affiliation.py`)

Classifies a parliamentary club as government-coalition vs opposition (`classify_party`) and derives who drives an amendment from its submitters' clubs (`amendment_driver`). Both take the period's coalition set explicitly (`PeriodData.coalition_parties`); an empty set means "unknown".

### Coalition Detector (`services/coalition_detector.py`)

Auto-derives the governing coalition per period from the chamber's investiture **confidence vote** — no manual config. `detect_coalition_parties(votes, mp_votes, mp_info)` finds passed "vyslovení důvěry" votes (excludes "nedůvěr" no-confidence motions), takes the most recent by `id_hlasovani`, and returns the clubs whose MPs voted YES — but only if the vote is genuinely contested (a polarization guard: clear yes/no blocs, a coalition seat-majority, and overall YES share ≤ `CONFIDENCE_MAX_YES_SHARE` to reject near-unanimous procedural votes). Otherwise returns an empty set ("unknown"). Computed in `DataReader._load_period` and stored on `PeriodData.coalition_parties` (recomputed on daily refresh). Exact for recent periods (10 = ANO+SPD+MS, 8 = ANO+ČSSD+KSČM); conservatively "unknown" when the confidence vote is ambiguous (e.g. period 9).

### Legislation Overview (`services/legislation_overview.py`)

Powers the homepage (#108): "Teď se o tom mluví" (most speeches in 14 days, at least `MIN_SPEECHES`=10, admin pin / hide / expiry via `homepage_pick`), bills of the latest agenda with a bill still in the Chamber, recently passed laws, newest bills, and the void vote count.

### About the data (`/o-datech`, #118)

`templates/partials/about_data_{cs,en}.html` describe sources and refresh, AI summaries and their check, statistics and vote matching, and list `services/coalition_table.py`. Keep them in sync when the pipelines change.

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
- **`routes/tisk.py`** — HTMX partials for tisk text, evolution, related bills; `GET /api/tisk-pdf/{period}/{ct}` streams the cached bill PDF (`tisky_pdf/{period}/{ct}.pdf`) inline (`ct1` query for sub-tisk versions), embedded in an iframe on the law detail page. `SecurityHeadersMiddleware` relaxes `X-Frame-Options`/`frame-ancestors` to same-origin for this route only so the embed renders.
- **`routes/feedback.py`** — Feedback submission endpoint (POST /api/feedback)
- **`routes/health.py`** — Health check, LLM health, LLM smoke test
- **`routes/utils.py`** — Shared utilities (`validate_period`, `_safe_url`)
- **`routes/charts.py`** — Seaborn/matplotlib chart endpoints returning PNG via `StreamingResponse`
- Templates in `templates/`, partials in `templates/partials/`
- All user-visible strings use `{{ _("key") }}` Jinja2 i18n calls

### Configuration (`config.py`)

Loads `.env` via `python-dotenv`. Contains psp.cz URLs, cache paths, period-to-year mappings, UNL format constants, and LLM settings (provider-agnostic `LLM_*` constants plus per-provider `OLLAMA_*` / `OPENAI_*` env vars). The `PERIOD_ORGAN_IDS` map is critical — `id_obdobi` in the psp.cz database uses organ IDs, not period numbers. Also defines `TERMINAL_TISK_STATUSES` (finished bill statuses the daily refresh skips re-scraping) and the confidence-vote detection thresholds (`CONFIDENCE_YES_BLOC`, `CONFIDENCE_NO_BLOC`, `CONFIDENCE_MAX_YES_SHARE`) used by `coalition_detector` to auto-derive the government coalition (no manual per-period list).

### Domain Enums (`models/enums.py`)

Vote result codes: `A`=YES, `B`=NO, `C`=ABSTAINED, `F`=DID_NOT_VOTE, `@`=ABSENT, `M`=EXCUSED. Void votes (zmatecne) are always filtered out before analysis.

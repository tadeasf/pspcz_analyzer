# PSP.cz Analyzer — Project Overview

## Purpose
Czech Parliamentary Voting Analyzer — OSINT tool for downloading, parsing, and visualizing open voting data from psp.cz (Czech Parliament).

## Tech Stack
- **Python 3.14** (pinned in .python-version, requires >=3.12)
- **Package manager**: uv
- **Web**: FastAPI + Uvicorn + Jinja2 + HTMX
- **CSS**: Pico CSS v2 (CDN, institutional light theme)
- **Data**: Polars (DataFrames, Parquet caching)
- **Charts**: Matplotlib + Seaborn (server-rendered PNG)
- **HTTP client**: httpx
- **Logging**: loguru
- **PDF parsing**: PyMuPDF
- **HTML parsing**: BeautifulSoup4
- **LLM**: Ollama (optional, for topic classification)

## Key Commands
- Install: `uv sync`
- Frontend: `uv run python -m pspcz_analyzer.main_frontend` (0.0.0.0:8000, reload)
- Backend (admin): `uv run python -m pspcz_analyzer.main_backend` (0.0.0.0:8001)
- Add dep: `uv add <package>`
- Tests: `uv run pytest -m "not integration" --cov -q`

## Project Structure
```
pspcz_analyzer/
├── main.py          — FastAPI app, lifespan, router mounting
├── config.py        — URLs, cache paths, period mappings, constants
├── logging_config.py
├── models/
│   ├── schemas.py   — UNL column definitions
│   └── enums.py     — Vote result codes (A/B/C/F/@/M)
├── data/
│   ├── downloader.py     — ZIP download from psp.cz
│   ├── parser.py         — UNL parsing (pipe-delimited, Win-1250)
│   ├── cache.py          — Parquet caching layer
│   ├── tisk_downloader.py
│   ├── tisk_extractor.py
│   ├── tisk_scraper.py
│   └── history_scraper.py
├── services/
│   ├── data_service.py        — Central orchestrator (PeriodData)
│   ├── loyalty_service.py     — Rebellion rates
│   ├── attendance_service.py  — Participation rates + vote breakdown + activity ranking
│   ├── similarity_service.py  — Cosine similarity + PCA
│   ├── votes_service.py       — Vote search/detail
│   ├── llm_service.py      — LLM integration
│   ├── topic_service.py       — Keyword-based classification
│   ├── tisk_pipeline_service.py
│   └── tisk_text_service.py
├── routes/
│   ├── pages.py     — Full HTML page routes
│   ├── api.py       — HTMX partial endpoints
│   └── charts.py    — PNG chart endpoints
├── templates/       — Jinja2 templates (base.html + 7 pages)
│   └── partials/    — HTMX partial fragments (5)
├── static/          — Currently empty
└── utils/
    └── text.py
```

## Style & Conventions
- Czech column names (matching psp.cz docs) for traceability
- No type annotations on route handlers (FastAPI convention)
- loguru for logging
- Analysis services: `function(PeriodData) -> list[dict]`
- Test suite: pytest (66 unit/API tests, 26 integration tests)
- Pre-commit hooks: ruff lint + format, pyright, trailing whitespace, etc.
- CI/CD: GitHub Actions (ci.yml for lint+unit, integration.yml for psp.cz tests)

## Coding Style Rules (MANDATORY)
1. **Imports always at top of the file** — no lazy imports, never, no exceptions
2. **No `except: pass`** — always handle or log exceptions explicitly
3. **Pattern matching** (`match/case`) over multiple `if/elif/else` when comparing a variable against literal values
4. **Function decomposition** — long complex functions must be split into a main function with `_helper()` private functions defined above it; helpers are called from the main function
5. **Type annotations** — use types on all function signatures (params + return)
6. **Google docstring format** — use Google-style docstrings
7. **After finishing each task**: run `uv run ruff format .` and `uv run ruff check --fix .`
8. **After finishing each task**: run `uv run pytest -m "not integration" --cov -q`
9. **After implementing a new feature**: update docs and README

## Project Structure (post-reorganization)
```
pspcz_analyzer/
├── main_frontend.py          — Public web app (port 8000, DataReader)
├── main_backend.py           — Admin dashboard (port 8001, DataService)
├── config.py
├── logging_config.py
├── middleware.py
├── rate_limit.py
├── models/
│   ├── schemas.py            — UNL column definitions
│   ├── enums.py              — Vote result codes
│   ├── tisk_models.py        — TiskInfo + PeriodData dataclasses
│   └── amendment_models.py
├── data/                     — Core I/O only
│   ├── downloader.py         — ZIP download from psp.cz
│   ├── parser.py             — UNL parsing
│   └── cache.py              — Parquet caching
├── services/
│   ├── data_reader.py        — Read-only data service (frontend)
│   ├── data_service.py       — Full data service (extends DataReader)
│   ├── llm/                  — LLM integration package
│   │   ├── __init__.py       — Public API re-exports
│   │   ├── prompts.py        — Prompt templates + JSON schemas
│   │   └── client.py         — LLMClient + factory + helpers
│   ├── tisk/                 — Tisk (parliamentary print) subpackage
│   │   ├── __init__.py
│   │   ├── io/               — Tisk I/O subpackage
│   │   │   ├── scraper.py
│   │   │   ├── downloader.py
│   │   │   ├── extractor.py
│   │   │   ├── history_scraper.py
│   │   │   └── law_changes_scraper.py
│   │   ├── pipeline.py
│   │   ├── classifier.py
│   │   ├── downloader_pipeline.py
│   │   ├── metadata_scraper.py
│   │   ├── version_service.py
│   │   ├── cache_manager.py
│   │   ├── lookup_builder.py
│   │   └── text_service.py
│   ├── amendments/           — Amendment analysis subpackage
│   │   ├── pipeline.py
│   │   ├── steno_scraper.py
│   │   ├── steno_parser.py
│   │   └── ...
│   ├── loyalty_service.py
│   ├── attendance_service.py
│   ├── similarity_service.py
│   ├── votes_service.py
│   └── topic_service.py
├── routes/
│   ├── pages.py              — Full HTML page routes
│   ├── voting.py             — Loyalty, attendance, similarity, votes
│   ├── amendments.py         — Amendment bills, coalitions
│   ├── tisk.py               — Tisk text, evolution, related bills
│   ├── feedback.py           — User feedback
│   ├── health.py             — Health checks, LLM diagnostics
│   ├── utils.py              — Shared utilities
│   └── charts.py             — PNG chart endpoints
├── admin/                    — Admin dashboard
│   ├── routes.py
│   ├── auth.py
│   └── log_stream.py
├── i18n/
│   ├── __init__.py
│   ├── translations.py
│   └── middleware.py
├── templates/
│   └── partials/
└── static/
```
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
- Dev server: `uv run python -m pspcz_analyzer.main` (0.0.0.0:8000, reload)
- Add dep: `uv add <package>`
- No test suite yet

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
│   ├── ollama_service.py      — LLM integration
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

## Project Structure (post-refactoring)
```
pspcz_analyzer/
├── main.py
├── config.py
├── logging_config.py
├── models/
│   ├── schemas.py        — UNL column definitions
│   ├── enums.py          — Vote result codes
│   └── tisk_models.py    — TiskInfo + PeriodData dataclasses
├── data/
│   ├── downloader.py
│   ├── parser.py
│   ├── cache.py
│   ├── tisk_downloader.py
│   ├── tisk_extractor.py
│   ├── tisk_scraper.py
│   ├── history_scraper.py
│   └── law_changes_scraper.py
├── services/
│   ├── data_service.py              — Central orchestrator (~420 lines)
│   ├── mp_builder.py                — MP info builder
│   ├── tisk_lookup_builder.py       — Tisk lookup table builder
│   ├── tisk_cache_manager.py        — TiskCacheManager class
│   ├── tisk_pipeline_service.py     — Pipeline orchestrator (~170 lines)
│   ├── tisk_downloader_pipeline.py  — PDF download + text extraction
│   ├── tisk_classifier.py           — Topic classification + consolidation
│   ├── tisk_metadata_scraper.py     — History + law changes scraping
│   ├── tisk_version_service.py      — Sub-tisk versions + diffs
│   ├── loyalty_service.py
│   ├── attendance_service.py
│   ├── similarity_service.py
│   ├── votes_service.py
│   ├── ollama_service.py
│   ├── topic_service.py
│   └── tisk_text_service.py
├── routes/
│   ├── pages.py
│   ├── api.py
│   └── charts.py
├── templates/
│   └── partials/
├── static/
└── utils/
    └── text.py
```

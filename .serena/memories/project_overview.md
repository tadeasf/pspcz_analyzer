# PSP.cz Analyzer — Project Overview

## Purpose
Czech Parliamentary Voting Analyzer — OSINT tool for downloading, parsing, and visualizing open voting data from psp.cz (Czech Parliament).

## Tech Stack
- **Python 3.14** (pinned in .python-version, requires >=3.12)
- **Package manager**: uv
- **Web**: FastAPI + Uvicorn + Jinja2 + HTMX
- **CSS**: Pico CSS v2 (CDN, dark theme)
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
│   ├── attendance_service.py  — Participation rates
│   ├── activity_service.py    — Vote volume ranking
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
- No docstrings on most functions (lean codebase)
- Test suite: pytest (71 unit/API tests, 26 integration tests)
- Pre-commit hooks: ruff lint + format, pyright, trailing whitespace, etc.
- CI/CD: GitHub Actions (ci.yml for lint+unit, integration.yml for psp.cz tests)

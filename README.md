# PSP.cz Analyzer

Czech Parliamentary Voting Analyzer — an OSINT tool that downloads, parses, and visualizes open voting data from the [Czech Chamber of Deputies](https://www.psp.cz/). Built with FastAPI, Polars, and HTMX.

## Features

- **Party Loyalty** — rebellion rates: how often each MP votes against their party's majority
- **Attendance** — participation rates with breakdowns (active, passive, absent, excused)
- **Voting Similarity** — cross-party alliances via cosine similarity + PCA visualization
- **Votes Browser** — searchable, paginated list of all parliamentary votes with detail views
- **Chart Endpoints** — server-rendered PNG charts (seaborn/matplotlib)
- **Multi-period Support** — covers all 10 electoral periods (1993 to present)
- **Tisk Pipeline** — background processing that downloads parliamentary print PDFs, extracts text, and classifies topics
- **AI Summaries** — optional LLM-based bilingual (Czech + English) summarization and topic classification via Ollama
- **i18n** — full Czech/English UI localization with a header language switcher
- **Feedback** — user feedback form on vote detail pages, submitted as GitHub Issues
- **Rate Limiting & Security** — per-endpoint rate limits (slowapi), CSP/HSTS/Permissions-Policy headers, CSRF protection, and XSS sanitization (nh3)
- **Legislative Evolution** — bill version diffs, law changes, and related bills discovery
- **Docker** — containerized deployment with docker-compose
- **Documentation** — project docs on [GitHub](https://github.com/tadeasf/pspcz_analyzer)

See detailed docs: [Routes](docs/routes.md) | [Services](docs/services.md) | [Templates](docs/templates.md) | [Data Model](docs/data-model.md) | [Testing & CI/CD](docs/testing.md)

## Quick Start

Requires Python >= 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync

# (Optional) Copy and edit environment variables
cp .env.example .env

# Run the dev server (with hot reload)
uv run python -m pspcz_analyzer.main
```

The app starts on `http://localhost:8000`. On first launch it downloads ~50 MB of open data from psp.cz and caches it locally as Parquet files.

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` for local development — `python-dotenv` loads it automatically.

| Variable | Default | Description |
|----------|---------|-------------|
| `PSPCZ_CACHE_DIR` | `~/.cache/pspcz-analyzer/psp` | Data cache directory |
| `PSPCZ_DEV` | `1` | Set to `1` for hot reload, `0` for production |
| `PORT` | `8000` | Server port (used by both local dev and Docker) |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_API_KEY` | *(empty)* | Bearer token for remote HTTPS Ollama |
| `OLLAMA_MODEL` | `qwen3:8b` | Model for topic classification and summarization |
| `DAILY_REFRESH_ENABLED` | `1` | Enable daily re-download of psp.cz data |
| `DAILY_REFRESH_HOUR` | `3` | Hour (CET, 0-23) for daily data refresh |
| `GITHUB_FEEDBACK_ENABLED` | `0` | Enable user feedback via GitHub Issues |
| `GITHUB_FEEDBACK_TOKEN` | *(empty)* | GitHub PAT with `public_repo` scope |
| `GITHUB_FEEDBACK_REPO` | `tadeasf/pspcz_analyzer` | Repository for feedback issues |
| `GITHUB_FEEDBACK_LABELS` | `user-feedback` | Labels applied to feedback issues |
| `TISK_SHORTENER` | `0` | Truncate tisk text for LLM (`0` = full text) |

## Docker

```bash
# Copy .env and configure Ollama connection
cp .env.example .env
# Edit .env to set OLLAMA_BASE_URL and OLLAMA_API_KEY

# Build and start
docker compose up --build
```

The app is available at `http://localhost:8000` (or the port set by `PORT`). Data cache is persisted via a bind mount at `./cache-data/`. Ollama runs separately on the local network — configure its address via `OLLAMA_BASE_URL` in `.env`.

To use a custom port:

```bash
PORT=9000 docker compose up --build
```

## Reverse Proxy

### Caddy

```
yourdomain.cz {
    reverse_proxy localhost:8000
}
```

### nginx

```nginx
server {
    server_name yourdomain.cz;
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Development

```bash
# Install with dev tools (pytest, ruff, pyright, pre-commit)
uv sync --extra dev

# Run unit + API tests
uv run pytest -m "not integration" --cov

# Run integration tests (downloads from real psp.cz)
uv run pytest -m integration -v

# Lint and format
uv run ruff check . && uv run ruff format .

# Install pre-commit hooks
uv run pre-commit install
```

See [Testing & CI/CD](docs/testing.md) for full details on the test suite, CI pipelines, and contributing guidelines.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Web framework | FastAPI + Uvicorn |
| Templating | Jinja2 + i18n extension |
| Frontend interactivity | HTMX |
| CSS | Pico CSS (institutional light theme) |
| Localization | Dict-based i18n (Czech + English) |
| Data processing | Polars |
| Charts | Seaborn + Matplotlib |
| PDF extraction | PyMuPDF |
| HTML scraping | BeautifulSoup4 |
| LLM integration | Ollama (optional, bilingual) |
| Documentation | GitHub + MkDocs |
| HTTP client | httpx |
| Configuration | python-dotenv |
| Testing | pytest + pytest-cov |
| Linting & formatting | Ruff |
| Type checking | Pyright |
| CI/CD | GitHub Actions |
| Containerization | Docker + docker-compose |
| Package manager | uv |

## Data Source

All data comes from the [psp.cz open data portal](https://www.psp.cz/eknih/cdrom/opendata). Files are pipe-delimited UNL format, Windows-1250 encoded. The app downloads and caches them automatically on first access.

Cached data is stored at `~/.cache/pspcz-analyzer/psp/` (raw ZIPs, extracted UNL files, Parquet caches, PDF texts, and topic classifications). Override with `PSPCZ_CACHE_DIR`.

## Tisk Pipeline

On startup, the app launches a background pipeline that enriches parliamentary prints (tisky):

1. **Download** PDFs from psp.cz for each print
2. **Extract** plain text using PyMuPDF
3. **Classify** topics using Ollama LLM (falls back to keyword matching if Ollama is unavailable)
4. **Summarize** each print in both Czech and English (bilingual AI summaries)
5. **Scrape** legislative process histories from psp.cz HTML pages
6. **Discover** related bills via zakon.cz cross-references
7. **Track** law changes (affected existing laws) from legislative process pages

This data powers the vote detail pages (topic tags, AI summaries, legislative timelines, and tisk transcriptions).

## Documentation

| Document | Contents |
|----------|----------|
| [Routes](docs/routes.md) | All HTTP endpoints — pages, API partials, chart images, health check, OpenAPI |
| [Services](docs/services.md) | Data pipeline, analysis services, tisk pipeline, Ollama integration |
| [Templates](docs/templates.md) | Frontend structure, HTMX patterns, i18n, vote detail, skeleton loading, styling |
| [Data Model](docs/data-model.md) | Electoral periods, UNL format, table schemas, vote codes, tisk data, Ollama/env config |
| [Testing & CI/CD](docs/testing.md) | Test suite structure, fixtures, linting config, GitHub Actions workflows, contributing |

## License

Educational / OSINT project. Parliamentary data is public domain per Czech law.

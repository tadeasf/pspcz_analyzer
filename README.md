# PSP.cz Analyzer

Czech Parliamentary Voting Analyzer — an OSINT tool that downloads, parses, and visualizes open voting data from the [Czech Chamber of Deputies](https://www.psp.cz/). Built with FastAPI, Polars, and HTMX.

## Features

- **Party Loyalty** — rebellion rates: how often each MP votes against their party's majority
- **Attendance** — participation rates with breakdowns (active, passive, absent, excused)
- **Most Active MPs** — ranked by raw vote count (YES + NO + ABSTAIN)
- **Voting Similarity** — cross-party alliances via cosine similarity + PCA visualization
- **Votes Browser** — searchable, paginated list of all parliamentary votes with detail views
- **Chart Endpoints** — server-rendered PNG charts (seaborn/matplotlib)
- **Multi-period Support** — covers all 10 electoral periods (1993 to present)
- **Tisk Pipeline** — background processing that downloads parliamentary print PDFs, extracts text, and classifies topics
- **AI Summaries** — optional LLM-based summarization and topic classification via local Ollama
- **API Documentation** — interactive Scalar UI at `/docs` with full OpenAPI schema

See detailed docs: [Routes](docs/routes.md) | [Services](docs/services.md) | [Templates](docs/templates.md) | [Data Model](docs/data-model.md) | [Testing & CI/CD](docs/testing.md)

## Quick Start

Requires Python >= 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync

# Run the dev server (with hot reload)
uv run python -m pspcz_analyzer.main
```

The app starts on `http://localhost:8000`. On first launch it downloads ~50 MB of open data from psp.cz and caches it locally as Parquet files.

## VPS Deployment

### Using `fastapi run`

The `fastapi[standard]` dependency includes the FastAPI CLI. Run in production mode:

```bash
uv run fastapi run pspcz_analyzer/main.py --host 0.0.0.0 --port 8000
```

This starts uvicorn without `--reload` and with production defaults.

### systemd Service

Create `/etc/systemd/system/pspcz-analyzer.service`:

```ini
[Unit]
Description=PSP.cz Analyzer
After=network.target

[Service]
Type=simple
User=deploy
WorkingDirectory=/opt/pspcz-analyzer
ExecStart=/opt/pspcz-analyzer/.venv/bin/fastapi run pspcz_analyzer/main.py --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
# On the VPS
cd /opt/pspcz-analyzer
uv sync
sudo systemctl enable --now pspcz-analyzer
```

### Reverse Proxy (Caddy)

```
yourdomain.cz {
    reverse_proxy localhost:8000
}
```

Or with nginx:

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
| Templating | Jinja2 |
| Frontend interactivity | HTMX |
| CSS | Pico CSS (dark theme) |
| Data processing | Polars |
| Charts | Seaborn + Matplotlib |
| PDF extraction | PyMuPDF |
| HTML scraping | BeautifulSoup4 |
| LLM integration | Ollama (optional) |
| API documentation | Scalar |
| HTTP client | httpx |
| Testing | pytest + pytest-cov |
| Linting & formatting | Ruff |
| Type checking | Pyright |
| CI/CD | GitHub Actions |
| Package manager | uv |

## Data Source

All data comes from the [psp.cz open data portal](https://www.psp.cz/eknih/cdrom/opendata). Files are pipe-delimited UNL format, Windows-1250 encoded. The app downloads and caches them automatically on first access.

Cached data is stored at `~/.cache/pspcz-analyzer/psp/` (raw ZIPs, extracted UNL files, Parquet caches, PDF texts, and topic classifications).

## Tisk Pipeline

On startup, the app launches a background pipeline that enriches parliamentary prints (tisky):

1. **Download** PDFs from psp.cz for each print
2. **Extract** plain text using PyMuPDF
3. **Classify** topics using Ollama LLM (falls back to keyword matching if Ollama is unavailable)
4. **Scrape** legislative process histories from psp.cz HTML pages

This data powers the vote detail pages (topic tags, AI summaries, legislative timelines, and tisk transcriptions).

## Documentation

| Document | Contents |
|----------|----------|
| [Routes](docs/routes.md) | All HTTP endpoints — pages, API partials, chart images, health check, OpenAPI |
| [Services](docs/services.md) | Data pipeline, analysis services, tisk pipeline, Ollama integration |
| [Templates](docs/templates.md) | Frontend structure, HTMX patterns, vote detail, skeleton loading, styling |
| [Data Model](docs/data-model.md) | Electoral periods, UNL format, table schemas, vote codes, tisk data, Ollama config |
| [Testing & CI/CD](docs/testing.md) | Test suite structure, fixtures, linting config, GitHub Actions workflows, contributing |

## License

Educational / OSINT project. Parliamentary data is public domain per Czech law.

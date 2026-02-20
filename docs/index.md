# PSP.cz Analyzer

**Czech Parliamentary Voting Analyzer** — an OSINT tool that downloads, parses, and visualizes open voting data from the [Czech Chamber of Deputies](https://www.psp.cz/).

Built with FastAPI, Polars, and HTMX.

## Features

- **Party Loyalty** — rebellion rates: how often each MP votes against their party's majority
- **Attendance** — participation rates with breakdowns (active, passive, absent, excused)
- **Most Active MPs** — ranked by raw vote count (YES + NO + ABSTAIN)
- **Voting Similarity** — cross-party alliances via cosine similarity + PCA visualization
- **Votes Browser** — searchable, paginated list of all parliamentary votes with detail views
- **Chart Endpoints** — server-rendered PNG charts (seaborn/matplotlib)
- **Multi-period Support** — covers all 10 electoral periods (1993 to present)
- **Tisk Pipeline** — background processing that downloads parliamentary print PDFs, extracts text, and classifies topics
- **AI Summaries** — optional LLM-based bilingual (Czech + English) summarization and topic classification via Ollama
- **i18n** — full Czech/English UI localization with a header language switcher
- **Docker** — containerized deployment with docker-compose
- **Documentation** — project docs on [GitHub](https://tadeasf.github.io/pspcz_analyzer/)

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

All configuration is via environment variables loaded from `.env` by `python-dotenv`:

| Variable          | Default                       | Description                                     |
| ----------------- | ----------------------------- | ----------------------------------------------- |
| `PSPCZ_CACHE_DIR` | `~/.cache/pspcz-analyzer/psp` | Data cache directory                            |
| `PSPCZ_DEV`       | `1`                           | `1` for hot reload, `0` for production          |
| `PORT`            | `8000`                        | Server port (used by both local dev and Docker) |
| `OLLAMA_BASE_URL` | `http://localhost:11434`      | Ollama API endpoint                             |
| `OLLAMA_API_KEY`  | _(empty)_                     | Bearer token for remote HTTPS Ollama            |
| `OLLAMA_MODEL`    | `qwen3:8b`                    | Model for classification and summarization      |

See `.env.example` for a documented template.

## Docker

```bash
docker compose up --build
```

The app runs at `http://localhost:8000` (or the port set by `PORT`). Data cache is persisted via a bind mount at `./cache-data/`. Configure `OLLAMA_BASE_URL` and `OLLAMA_API_KEY` in `.env` to connect to your Ollama instance on the local network.

## Tech Stack

| Layer                  | Technology                           |
| ---------------------- | ------------------------------------ |
| Web framework          | FastAPI + Uvicorn                    |
| Templating             | Jinja2 + i18n extension              |
| Frontend interactivity | HTMX                                 |
| CSS                    | Pico CSS (institutional light theme) |
| Localization           | Dict-based i18n (Czech + English)    |
| Data processing        | Polars                               |
| Charts                 | Seaborn + Matplotlib                 |
| PDF extraction         | PyMuPDF                              |
| HTML scraping          | BeautifulSoup4                       |
| LLM integration        | Ollama (optional, bilingual)         |
| Documentation          | GitHub + MkDocs                      |
| HTTP client            | httpx                                |
| Configuration          | python-dotenv                        |
| Testing                | pytest + pytest-cov                  |
| Linting & formatting   | Ruff                                 |
| Type checking          | Pyright                              |
| Containerization       | Docker + docker-compose              |
| CI/CD                  | GitHub Actions                       |
| Package manager        | uv                                   |

## Data Source

All data comes from the [psp.cz open data portal](https://www.psp.cz/eknih/cdrom/opendata). Files are pipe-delimited UNL format, Windows-1250 encoded. The app downloads and caches them automatically on first access.

Cached data is stored at `~/.cache/pspcz-analyzer/psp/` (override with `PSPCZ_CACHE_DIR`).

## License

Educational / OSINT project. Parliamentary data is public domain per Czech law.

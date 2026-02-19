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
- **AI Summaries** — optional LLM-based summarization and topic classification via local Ollama
- **API Documentation** — interactive Scalar UI at `/docs` with full OpenAPI schema

## Quick Start

Requires Python >= 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync

# Run the dev server (with hot reload)
uv run python -m pspcz_analyzer.main
```

The app starts on `http://localhost:8000`. On first launch it downloads ~50 MB of open data from psp.cz and caches it locally as Parquet files.

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

Cached data is stored at `~/.cache/pspcz-analyzer/psp/`.

## License

Educational / OSINT project. Parliamentary data is public domain per Czech law.

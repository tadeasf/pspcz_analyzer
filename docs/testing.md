# Testing & CI/CD

## Quick Reference

```bash
# Install dev dependencies
uv sync --extra dev

# Run unit + API tests (no network required)
uv run pytest -m "not integration" --cov

# Run integration tests (hits real psp.cz)
uv run pytest -m integration -v

# Lint + format
uv run ruff check .
uv run ruff format .

# Type check
uv run pyright

# Pre-commit hooks (all files)
uv run pre-commit run --all-files
```

## Test Suite

Tests live in `tests/` and are organized into three layers:

### Unit Tests (`tests/unit/`)

Pure logic tests with synthetic data — no network, no disk I/O beyond `tmp_path`.

**Data layer:**

| File | Tests | What it covers |
|------|-------|----------------|
| `test_parser.py` | 8 | UNL parsing: encoding (Windows-1250 → UTF-8), trailing pipe handling, dtype casting, `quote_char=None`, empty files, Czech diacritics |
| `test_cache.py` | 3 | Parquet round-trip, staleness detection (mtime comparison), missing source fallback |

**Analysis services** (`tests/unit/services/`):

| File | Tests | What it covers |
|------|-------|----------------|
| `test_loyalty.py` | 10 | Rebellion rate computation, party filter (case-insensitive), empty data edge case, result sorting, rebellion vote details |
| `test_attendance.py` | 6 | Attendance formula (`active / (total - excused) * 100`), sort modes (`best` vs `worst`), field validation |
| `test_similarity.py` | 9 | PCA produces 2D coords per MP, cross-party pairs exclude same-party, cosine similarity in [-1, 1] range |
| `test_activity.py` | 5 | Vote breakdown fields, party filter, `most_active` sort mode, active count verification |
| `test_votes.py` | 11 | Vote search by description text, pagination (page size, page navigation), vote detail with party breakdown, nonexistent vote returns None |

### API Tests (`tests/api/`)

Use FastAPI's `TestClient` with a mocked `DataService` — no real downloads.

| File | Tests | What it covers |
|------|-------|----------------|
| `test_pages.py` | 5 | All page routes (`/`, `/loyalty`, `/attendance`, `/similarity`, `/votes`) return 200 + HTML |
| `test_api_endpoints.py` | 6 | HTMX partials (`/api/loyalty`, `/api/attendance`, etc.) return 200 + HTML, `/api/health` returns JSON `{"status": "ok"}`, invalid period returns 404 |
| `test_charts.py` | 3 | Chart endpoints (`/charts/loyalty.png`, etc.) return `image/png` with valid PNG magic bytes |

### Integration Tests (`tests/integration/`)

Hit real psp.cz infrastructure — marked with `@pytest.mark.integration` and excluded from default `pytest` runs.

| File | Tests | What it covers |
|------|-------|----------------|
| `test_download.py` | 6 | ZIP downloads succeed, contain expected UNL file patterns |
| `test_parsing.py` | 7 | Real UNL files parse with our schema definitions, column counts match, ID columns non-null, schema drift canary (no all-null typed columns) |
| `test_pipeline.py` | 9 | Full end-to-end: download → parse → DataService → all analysis services produce non-empty results |

**Design decisions:**
- Uses **period 1 (1993)** — smallest dataset, fastest downloads
- Session-scoped fixtures download data once per test run, shared across all integration tests
- Uses the real cache directory (`~/.cache/pspcz-analyzer/psp/`) so downloads persist across runs

## Test Fixtures

### `tests/fixtures/sample_data.py`

Factory functions that create synthetic Polars DataFrames matching real schemas:

- `make_votes(n=5)` — voting summary with all `HL_HLASOVANI` columns
- `make_mp_votes()` — 6 MPs with known patterns:
  - MPs 1-2 (ANO): always YES (loyal)
  - MP 3 (ODS): NO on 3/5 votes (rebel, 60% rebellion rate)
  - MPs 4, 6 (ODS): always YES (establishes party majority)
  - MP 5 (STAN): mixed results (YES, ABSENT, EXCUSED, DID_NOT_VOTE, ABSTAINED) — tests attendance formula
- `make_mp_info()` — 6 MPs across 3 parties (ANO, ODS, STAN)
- `make_void_votes()` — empty DataFrame (no void votes in test data)
- `make_period_data(period=1)` — assembles all above into a `PeriodData` dataclass

### `tests/conftest.py`

Shared fixtures:

- `mock_period_data` — calls `make_period_data()`
- `mock_data_service` — `MagicMock(spec=DataService)` that returns the synthetic data
- `client` — `TestClient(app)` with a custom lifespan that injects the mock service (no real downloads)
- `test_cache_dir` — `tmp_path`-based isolated cache directory

## Linting & Formatting

### Ruff

Configuration in `pyproject.toml`:

```toml
[tool.ruff]
target-version = "py312"
line-length = 100

[tool.ruff.lint]
select = ["E", "W", "F", "I", "UP", "B", "C4", "SIM"]
```

Key rule choices:
- **E, W, F** — pycodestyle errors/warnings + pyflakes (standard)
- **I** — isort import ordering
- **UP** — pyupgrade (modernize syntax for py312+)
- **B** — flake8-bugbear (common gotchas)
- **C4** — flake8-comprehensions
- **SIM** — simplify (with SIM102/SIM108/SIM117/C408 suppressed — readability preference)

Suppressed rules:
- `E501` — line length handled by the formatter
- `B008` — FastAPI `Depends()` pattern uses function calls as defaults
- `SIM102/108/117`, `C408` — style preferences that don't warrant rewriting existing code

### Pyright

Basic mode — the codebase isn't fully annotated yet. Reports missing imports but tolerates missing type stubs.

### Pre-commit Hooks

`.pre-commit-config.yaml` runs on every commit:

1. **pre-commit-hooks** — trailing whitespace, end-of-file fixer, YAML/TOML syntax, large file check, debug statement detection
2. **ruff** — lint with `--fix` + format check
3. **pyright** — type checking

Install hooks: `uv run pre-commit install`

## CI/CD (GitHub Actions)

### `ci.yml` — Every PR + Push to main

Two jobs:

1. **Lint** — `ruff check` + `ruff format --check` + `pyright` (pyright is `continue-on-error` since the codebase isn't fully typed)
2. **Unit Tests** — `pytest -m "not integration" --cov` on Python 3.14

Uses `astral-sh/setup-uv@v5` with caching for fast dependency installation.

### `integration.yml` — Selective Runs

Triggers:
- PRs targeting `main` only (not every feature branch)
- Weekly cron (Monday 2:00 AM UTC) — catches upstream psp.cz format changes
- Manual `workflow_dispatch`

Single job: `pytest -m integration --timeout=300 -v` with a 30-minute timeout. Uploads test artifacts on failure for debugging.

**Why real integration tests?** psp.cz is government infrastructure — stable, but when upstream format changes happen, our parsing breaks silently. The weekly cron job is our early warning system.

## Version Management

`bump-my-version` is configured to update the version in `pyproject.toml`:

```bash
uv run bump-my-version bump patch   # 0.1.0 → 0.1.1
uv run bump-my-version bump minor   # 0.1.0 → 0.2.0
uv run bump-my-version bump major   # 0.1.0 → 1.0.0
```

## Adding New Tests

When adding a new analysis service:

1. Add a factory function in `tests/fixtures/sample_data.py` if the service needs specific data patterns
2. Create `tests/unit/services/test_<service>.py` with synthetic data tests
3. Add an integration test in `tests/integration/test_pipeline.py` to verify it works on real data
4. If the service has an API endpoint, add a test in `tests/api/test_api_endpoints.py`

When adding a new route:

1. Add a page test in `tests/api/test_pages.py` (returns 200 + HTML)
2. If it's an HTMX partial, add to `tests/api/test_api_endpoints.py`
3. If it's a chart, add to `tests/api/test_charts.py` (returns PNG)

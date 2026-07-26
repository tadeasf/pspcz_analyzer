# Contributing to pspcz_analyzer

Thanks for your interest in contributing! This document explains how to set up a
development environment, the project's mandatory code style rules, and the
checklist every pull request must pass.

## Development setup

Requires Python >= 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
# Clone and install with dev tools (pytest, ruff, pyright, pre-commit)
git clone https://github.com/tadeasf/pspcz_analyzer.git
cd pspcz_analyzer
uv sync --extra dev

# Copy and edit environment variables
cp .env.example .env

# (Optional) install pre-commit hooks
uv run pre-commit install
```

## Running the app

The app runs as two separate FastAPI processes:

```bash
# Public web app on 0.0.0.0:8000 (hot reload in dev)
uv run python -m pspcz_analyzer.main_frontend

# Admin dashboard on 0.0.0.0:8001
uv run python -m pspcz_analyzer.main_backend
```

On first launch the frontend downloads ~50 MB of open data from psp.cz and
caches it locally as Parquet files.

## Testing

```bash
# Unit + API tests (no network required)
uv run pytest -m "not integration" --cov

# Integration tests (requires network — hits real psp.cz)
uv run pytest -m integration -v
```

Coverage must stay at or above the configured threshold (`fail_under` in
`pyproject.toml`).

## Mandatory code style rules

These rules are enforced in review. Please follow them in every change:

1. **Imports always at the top of the file** — no lazy imports, never, no
   exceptions.
2. **No `except: pass`** — always handle or log exceptions explicitly.
3. **Prefer `match/case`** over `if/elif` when comparing a variable against
   literal values.
4. **Decompose long functions** into a main function plus private `_helper()`
   functions defined *above* it.
5. **Type annotations on all function signatures** — both parameters and return
   types.
6. **Google-style docstrings** on public functions and classes.
7. **Keep files focused** — files over ~400 lines should be split into focused
   modules.

## Documentation and UI strings

- Project documentation lives in [`docs/`](docs/) and is built with
  [MkDocs Material](https://squidfunk.github.io/mkdocs-material/). Update the
  relevant pages when you change behavior they describe.
- All user-visible UI strings must go through the i18n system: add keys to
  [`i18n/translations.py`](pspcz_analyzer/i18n/translations.py) in **both** the
  `cs` (Czech) and `en` (English) dictionaries, and reference them in templates
  with `{{ _("key") }}`.

## Pre-PR checklist

Run all of these before opening a pull request:

```bash
uv run ruff format .
uv run ruff check --fix .
uv run pytest -m "not integration" --cov -q
uv run pyright
```

- [ ] `ruff format` and `ruff check` pass cleanly
- [ ] Unit tests pass and coverage stays above the threshold
- [ ] `pyright` reports no new errors
- [ ] Docs and README updated if user-visible behavior changed
- [ ] New UI strings added to `i18n/translations.py` in both `cs` and `en`

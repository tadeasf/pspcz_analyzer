# Suggested Commands

## Development
- `uv sync` — Install/sync dependencies
- `uv run python -m pspcz_analyzer.main` — Run dev server (hot reload, 0.0.0.0:8000)
- `uv add <package>` — Add a dependency
- `uv run fastapi run pspcz_analyzer/main.py --host 0.0.0.0 --port 8000` — Production mode

## Quality & Testing
- `uv run ruff check .` — Lint (ruff)
- `uv run ruff format .` — Format (ruff)
- `uv run pytest -m "not integration" --cov` — Unit + API tests with coverage
- `uv run pytest -m integration -v` — Integration tests (requires network, hits psp.cz)
- `uv run pyright` — Type checking (basic mode)
- `uv run pre-commit run --all-files` — All pre-commit hooks
- `uv run bump-my-version bump patch|minor|major` — Version bumping

## System Tools
- `git` — Version control
- `ls`, `find`, `grep` — Standard Linux utilities

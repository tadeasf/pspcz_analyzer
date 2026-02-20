# Routes

All routes accept a `period` query parameter (default: `10`, the current electoral period). Changing the period loads data for that period on demand.

All page and API routes respect the current UI language (set via the `lang` cookie). Chart labels and vote outcome labels are localized.

## Page Routes

Full HTML pages rendered with Jinja2. Defined in `pspcz_analyzer/routes/pages.py`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard — overview stats for the selected period |
| GET | `/loyalty` | Party loyalty analysis page |
| GET | `/attendance` | Attendance analysis page (includes vote breakdown + activity ranking) |
| GET | `/similarity` | Cross-party voting similarity page |
| GET | `/votes` | Votes browser (searchable, paginated) |
| GET | `/votes/{vote_id}` | Single vote detail — per-party and per-MP breakdown |
| GET | `/set-lang/{lang}` | Set UI language (`cs` or `en`) via cookie and redirect back |
| GET | `/docs` | Scalar API documentation UI (not included in OpenAPI schema) |

## API Routes (HTMX Partials)

Return HTML fragments for dynamic table updates. Defined in `pspcz_analyzer/routes/api.py`. Mounted under `/api`.

### GET /api/loyalty

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `top` | int | 30 | Number of MPs to show |
| `party` | string | `""` | Filter by party code (e.g. `ODS`, `ANO`) |

### GET /api/attendance

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `top` | int | 30 | Number of MPs to show |
| `sort` | string | `worst` | Sort order: `worst`, `best`, `most_active`, `least_active`, `most_yes`, `most_no`, `most_abstained`, `most_passive`, `most_absent`, `most_excused` |
| `party` | string | `""` | Filter by party code (e.g. `ODS`, `ANO`) |

### GET /api/similarity

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `top` | int | 20 | Number of cross-party pairs |

### GET /api/votes

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `search` | string | `""` | Full-text search on vote descriptions |
| `outcome` | string | `""` | Filter: `A` (passed), `R` (rejected), or empty for all |
| `topic` | string | `""` | Filter by topic label (from keyword or LLM classification) |
| `page` | int | 1 | Page number |

### GET /api/tisk-text

Returns extracted PDF text for a parliamentary print as an HTML fragment (for lazy-loading via HTMX on vote detail pages).

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `ct` | int | 0 | Print number (cislo tisku) |

### GET /api/tisk-evolution

Returns the legislative evolution view for a parliamentary print, including sub-versions and LLM-generated diff summaries (bilingual — displays English when `lang=en`).

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `ct` | int | *(required)* | Print number |

### GET /api/related-bills

Returns related bills for a parliamentary print, discovered via zakon.cz cross-references.

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `idsb` | int | *(required)* | Bill identifier from zakon.cz |

Rate limit: 5/minute.

### POST /api/feedback

Submit user feedback as a GitHub Issue (requires `GITHUB_FEEDBACK_ENABLED=1`).

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `title` | string | *(required)* | Feedback title |
| `body` | string | *(required)* | Feedback body text |
| `vote_id` | string | `""` | Related vote ID |
| `period` | string | `""` | Related electoral period |
| `page_url` | string | `""` | URL of the page where feedback was submitted |

Rate limit: 3/hour.

### GET /api/ollama/health

Returns Ollama availability status as JSON.

**Response:**
```json
{"available": true, "base_url": "http://localhost:11434", "model": "qwen3:8b"}
```

Rate limit: 10/minute.

### GET /api/ollama/smoke-test

Concurrent bilingual generation test. Fires two parallel LLM calls and measures wall-clock time.

**Response:**
```json
{"success": true, "model": "qwen3:8b", "duration_seconds": 4.2, "summary_cs": "...", "summary_en": "..."}
```

Returns 503 if Ollama is down, 502 on generation failure.

Rate limit: 2/minute.

### GET /api/health

Health check endpoint returning JSON.

**Response:**
```json
{"status": "ok", "periods_loaded": [10, 9]}
```

## Chart Routes

Return PNG images via `StreamingResponse`. Defined in `pspcz_analyzer/routes/charts.py`. Mounted under `/charts`.

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/charts/loyalty.png` | `period`, `top=20` | Horizontal bar chart — rebellion rates (coolwarm palette) |
| GET | `/charts/attendance.png` | `period`, `top=20` | Horizontal bar chart — worst attendance (RdYlGn palette) |
| GET | `/charts/similarity.png` | `period` | PCA scatter plot — MPs colored by party (husl palette) |

All charts render at 150 DPI with a light background (`#FFFFFF` with `#F7F7F7` plot area). Labels and titles are localized based on the current UI language.

## OpenAPI

The full OpenAPI schema is available at `/openapi.json`. The interactive API documentation (Scalar UI) is at `/docs`. Default Swagger UI and ReDoc are disabled in favor of Scalar.

## Rate Limiting

Per-endpoint rate limits (via slowapi/limits):

| Endpoint | Limit |
|----------|-------|
| Page routes | 60/minute |
| Analysis APIs (`/api/loyalty`, etc.) | 15/minute |
| `/api/related-bills` | 5/minute |
| `/api/feedback` | 3/hour |
| `/api/ollama/health` | 10/minute |
| `/api/ollama/smoke-test` | 2/minute |
| Chart routes | 30/minute |

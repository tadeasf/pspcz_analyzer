# Routes

All routes accept a `period` query parameter (default: `10`, the current electoral period). Changing the period loads data for that period on demand.

All page and API routes respect the current UI language (set via the `lang` cookie). Chart labels and vote outcome labels are localized.

## Page Routes

Full HTML pages rendered with Jinja2. Defined in `pspcz_analyzer/routes/pages.py`.

| Method | Path               | Description                                                           |
| ------ | ------------------ | --------------------------------------------------------------------- |
| GET    | `/`                | Dashboard — overview stats for the selected period                    |
| GET    | `/loyalty`         | Party loyalty analysis page                                           |
| GET    | `/attendance`      | Attendance analysis page (includes vote breakdown + activity ranking) |
| GET    | `/similarity`      | Cross-party voting similarity page                                    |
| GET    | `/votes`           | Votes browser (searchable, paginated)                                 |
| GET    | `/votes/{vote_id}` | Single vote detail — per-party and per-MP breakdown                   |
| GET    | `/laws`                      | Laws browser — filterable list of parliamentary bills                 |
| GET    | `/laws/{ct}`                 | Law detail — bill sponsors, status, legislative history               |
| GET    | `/amendments`                | Amendment voting — third-reading amendment analysis overview          |
| GET    | `/amendments/{schuze}/{bod}` | Amendment detail — per-amendment votes, coalitions, AI summary        |
| GET    | `/set-lang/{lang}` | Set UI language (`cs` or `en`) via cookie and redirect back           |

## API Routes (HTMX Partials)

Return HTML fragments for dynamic table updates. Split across domain modules mounted under `/api`:
- `routes/voting.py` — loyalty, attendance, similarity, votes
- `routes/amendments.py` — amendment bills, coalitions
- `routes/tisk.py` — tisk text, evolution, related bills
- `routes/feedback.py` — user feedback
- `routes/health.py` — health checks, LLM diagnostics
- `routes/utils.py` — shared utilities (`validate_period`, `_safe_url`)

### GET /api/loyalty

| Param    | Type   | Default | Description                              |
| -------- | ------ | ------- | ---------------------------------------- |
| `period` | int    | 10      | Electoral period                         |
| `top`    | int    | 30      | Number of MPs to show                    |
| `party`  | string | `""`    | Filter by party code (e.g. `ODS`, `ANO`) |

### GET /api/attendance

| Param    | Type   | Default | Description                                                                                                                                        |
| -------- | ------ | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `period` | int    | 10      | Electoral period                                                                                                                                   |
| `top`    | int    | 30      | Number of MPs to show                                                                                                                              |
| `sort`   | string | `worst` | Sort order: `worst`, `best`, `most_active`, `least_active`, `most_yes`, `most_no`, `most_abstained`, `most_passive`, `most_absent`, `most_excused` |
| `party`  | string | `""`    | Filter by party code (e.g. `ODS`, `ANO`)                                                                                                           |

### GET /api/similarity

| Param    | Type | Default | Description                 |
| -------- | ---- | ------- | --------------------------- |
| `period` | int  | 10      | Electoral period            |
| `top`    | int  | 20      | Number of cross-party pairs |

### GET /api/votes

| Param     | Type   | Default | Description                                                |
| --------- | ------ | ------- | ---------------------------------------------------------- |
| `period`  | int    | 10      | Electoral period                                           |
| `search`  | string | `""`    | Full-text search on vote descriptions                      |
| `outcome` | string | `""`    | Filter: `A` (passed), `R` (rejected), or empty for all     |
| `topic`   | string | `""`    | Filter by topic label (from keyword or LLM classification) |
| `page`    | int    | 1       | Page number                                                |

### GET /api/tisk-text

Returns extracted PDF text for a parliamentary print as an HTML fragment (for lazy-loading via HTMX on vote detail pages).

| Param    | Type | Default | Description                |
| -------- | ---- | ------- | -------------------------- |
| `period` | int  | 10      | Electoral period           |
| `ct`     | int  | 0       | Print number (cislo tisku) |

### GET /api/tisk-evolution

Returns the legislative evolution view for a parliamentary print, including sub-versions and LLM-generated diff summaries (bilingual — displays English when `lang=en`).

| Param    | Type | Default      | Description      |
| -------- | ---- | ------------ | ---------------- |
| `period` | int  | 10           | Electoral period |
| `ct`     | int  | _(required)_ | Print number     |

### GET /api/related-bills

Returns related bills for a parliamentary print, discovered via zakon.cz cross-references.

| Param  | Type | Default      | Description                   |
| ------ | ---- | ------------ | ----------------------------- |
| `idsb` | int  | _(required)_ | Bill identifier from zakon.cz |

Rate limit: 5/minute.

### POST /api/feedback

Submit user feedback as a GitHub Issue (requires `GITHUB_FEEDBACK_ENABLED=1`).

| Param      | Type   | Default      | Description                                  |
| ---------- | ------ | ------------ | -------------------------------------------- |
| `title`    | string | _(required)_ | Feedback title                               |
| `body`     | string | _(required)_ | Feedback body text                           |
| `vote_id`  | string | `""`         | Related vote ID                              |
| `period`   | string | `""`         | Related electoral period                     |
| `page_url` | string | `""`         | URL of the page where feedback was submitted |

Rate limit: 3/hour.

### GET /api/llm/health

Returns LLM availability status as JSON.

**Response:**

```json
{ "available": true, "provider": "ollama", "base_url": "http://localhost:11434", "model": "qwen3:8b" }
```

Rate limit: 10/minute.

### GET /api/llm/smoke-test

Concurrent bilingual generation test. Fires two parallel LLM calls and measures wall-clock time.

**Response:**

```json
{
  "success": true,
  "provider": "ollama",
  "model": "qwen3:8b",
  "duration_seconds": 4.2,
  "summary_cs": "...",
  "summary_en": "..."
}
```

Returns 503 if LLM is down, 502 on generation failure.

Rate limit: 2/minute.

### GET /api/health

Health check endpoint returning JSON.

**Response:**

```json
{ "status": "ok", "periods_loaded": [10, 9] }
```

### GET /api/laws

Returns an HTMX partial with filtered laws list.

| Parameter     | Type   | Default | Description                                 |
| ------------- | ------ | ------- | ------------------------------------------- |
| `period`      | int    | latest  | Electoral period                             |
| `page`        | int    | 1       | Page number                                 |
| `q`           | str    | —       | Full-text search query                       |
| `topic`       | str    | —       | Filter by topic classification               |
| `status`      | str    | —       | Filter by legislative status                 |

### GET /api/amendments

Returns an HTMX partial with amendment bills for a period.

| Parameter     | Type   | Default | Description                                 |
| ------------- | ------ | ------- | ------------------------------------------- |
| `period`      | int    | latest  | Electoral period                             |
| `page`        | int    | 1       | Page number                                 |

### GET /api/amendment-coalitions

Returns coalition analysis for a specific amendment vote.

| Parameter     | Type   | Default | Description                                 |
| ------------- | ------ | ------- | ------------------------------------------- |
| `period`      | int    | required| Electoral period                             |
| `schuze`      | int    | required| Session number                               |
| `bod`         | int    | required| Agenda point number                          |

## Admin Routes (Port 8001)

The admin backend runs on a separate port (default 8001) with IP whitelist + password authentication.

### Authentication

| Route                 | Method | Description                              |
| --------------------- | ------ | ---------------------------------------- |
| `/admin/login`        | GET    | Login page                               |
| `/admin/login`        | POST   | Authenticate (username + bcrypt password) |
| `/admin/logout`       | POST   | Clear session cookie                     |

### Admin Pages

| Route                 | Description                                       |
| --------------------- | ------------------------------------------------- |
| `/admin/`             | Dashboard — system overview, cache size, LLM info |
| `/admin/pipelines`    | Pipeline management — start, stop, monitor        |
| `/admin/config`       | Runtime config editor — LLM settings, toggles     |

### Admin API

| Route                                  | Method | Description                                 |
| -------------------------------------- | ------ | ------------------------------------------- |
| `/admin/api/pipeline/start`            | POST   | Start a pipeline (type + period via form)   |
| `/admin/api/pipeline/stop`             | POST   | Stop the currently running pipeline         |
| `/admin/api/pipeline/cancel/{period}`  | POST   | Cancel a single period's pipeline           |
| `/admin/api/pipeline/remove/{period}`  | POST   | Remove a pending period from the queue      |
| `/admin/api/pipeline/status`           | GET    | Current pipeline status as JSON             |
| `/admin/api/pipeline/history`          | GET    | Recent pipeline run history                 |
| `/admin/api/pipeline/logs`             | GET    | SSE endpoint for real-time log streaming    |
| `/admin/api/config`                    | GET    | Read runtime config (secrets masked)        |
| `/admin/api/config`                    | POST   | Update runtime config                       |
| `/admin/api/refresh`                   | POST   | Trigger manual data refresh                 |
| `/admin/api/health`                    | GET    | Admin backend health check                  |
| `/admin/partials/pipeline-status`      | GET    | HTMX partial for pipeline progress polling  |

## Chart Routes

Return PNG images via `StreamingResponse`. Defined in `pspcz_analyzer/routes/charts.py`. Mounted under `/charts`.

| Method | Path                     | Params             | Description                                               |
| ------ | ------------------------ | ------------------ | --------------------------------------------------------- |
| GET    | `/charts/loyalty.png`    | `period`, `top=20` | Horizontal bar chart — rebellion rates (coolwarm palette) |
| GET    | `/charts/attendance.png` | `period`, `top=20` | Horizontal bar chart — worst attendance (RdYlGn palette)  |
| GET    | `/charts/similarity.png` | `period`           | PCA scatter plot — MPs colored by party (husl palette)    |

All charts render at 150 DPI with a light background (`#FFFFFF` with `#F7F7F7` plot area). Labels and titles are localized based on the current UI language.

## OpenAPI

The full OpenAPI schema is available at `/openapi.json`. Default Swagger UI and ReDoc are disabled. The nav bar links to the [GitHub repository](https://tadeasf.github.io/pspcz_analyzer/) for documentation.

## Rate Limiting

Per-endpoint rate limits (via slowapi/limits):

| Endpoint                             | Limit                              |
| ------------------------------------ | ---------------------------------- |
| Page routes (incl. laws, amendments) | 60/minute                          |
| Analysis APIs (`/api/loyalty`, etc.) | 15/minute                          |
| `/api/related-bills`                 | 5/minute                           |
| `/api/feedback`                      | 3/hour                             |
| `/api/llm/health`                    | 10/minute                          |
| `/api/llm/smoke-test`               | 2/minute                           |
| Chart routes                         | 30/minute                          |
| Admin API routes                     | No limit (auth-protected)          |

# Routes

All routes accept a `period` query parameter (default: `10`, the current electoral period). Changing the period loads data for that period on demand.

## Page Routes

Full HTML pages rendered with Jinja2. Defined in `pspcz_analyzer/routes/pages.py`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard — overview stats for the selected period |
| GET | `/loyalty` | Party loyalty analysis page |
| GET | `/attendance` | Attendance analysis page |
| GET | `/active` | Most active MPs page |
| GET | `/similarity` | Cross-party voting similarity page |
| GET | `/votes` | Votes browser (searchable, paginated) |
| GET | `/votes/{vote_id}` | Single vote detail — per-party and per-MP breakdown |

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
| `sort` | string | `worst` | Sort order: `worst` (lowest first) or `best` |

### GET /api/similarity

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `top` | int | 20 | Number of cross-party pairs |

### GET /api/active

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `top` | int | 50 | Number of MPs to show |
| `party` | string | `""` | Filter by party code |

### GET /api/votes

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `period` | int | 10 | Electoral period |
| `search` | string | `""` | Full-text search on vote descriptions |
| `outcome` | string | `""` | Filter: `A` (passed), `R` (rejected), or empty for all |
| `page` | int | 1 | Page number |

## Chart Routes

Return PNG images via `StreamingResponse`. Defined in `pspcz_analyzer/routes/charts.py`. Mounted under `/charts`.

| Method | Path | Params | Description |
|--------|------|--------|-------------|
| GET | `/charts/loyalty.png` | `period`, `top=20` | Horizontal bar chart — rebellion rates (coolwarm palette) |
| GET | `/charts/attendance.png` | `period`, `top=20` | Horizontal bar chart — worst attendance (RdYlGn palette) |
| GET | `/charts/active.png` | `period`, `top=25` | Horizontal bar chart — most active MPs (viridis palette) |
| GET | `/charts/similarity.png` | `period` | PCA scatter plot — MPs colored by party (husl palette) |

All charts render at 150 DPI with a dark background (`#1a1a2e`).

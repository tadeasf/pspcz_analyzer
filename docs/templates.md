# Templates

Frontend is server-rendered HTML with HTMX for dynamic updates. No JavaScript framework — just Jinja2 templates + HTMX attributes.

## Base Layout (`templates/base.html`)

All pages extend `base.html`, which provides:

- **Head**: Pico CSS v2 (CDN), HTMX v2.0.4 (CDN), custom inline styles
- **Header**: "PSP.cz Analyzer" title
- **Navigation**: Dashboard, Party Loyalty, Attendance, Most Active, Similarity, Votes
- **Period selector**: dropdown that reloads the current page with `?period={n}`
- **Footer**: data source attribution + educational project disclaimer
- **Theme**: Pico CSS dark mode (`data-theme="dark"`)

Active navigation item is highlighted via `aria-current="page"`, controlled by the `active_page` template variable.

## Page Templates

Each page template renders a full page with controls and a `#results` div that HTMX populates:

| Template | Page | Controls |
|----------|------|----------|
| `index.html` | Dashboard | None (static stats + feature cards) |
| `loyalty.html` | Party Loyalty | Top N slider, party filter dropdown |
| `attendance.html` | Attendance | Top N slider, sort toggle (worst/best) |
| `active.html` | Most Active | Top N slider, party filter dropdown |
| `similarity.html` | Similarity | Top N slider |
| `votes.html` | Votes Browser | Search input, outcome filter, pagination |
| `vote_detail.html` | Vote Detail | None (static detail view) |

## Partials (`templates/partials/`)

HTML fragments returned by `/api/*` endpoints for HTMX swaps:

| Partial | Endpoint | Content |
|---------|----------|---------|
| `loyalty_table.html` | `/api/loyalty` | Ranked table of MPs by rebellion rate |
| `attendance_table.html` | `/api/attendance` | Ranked table of MPs by attendance % |
| `active_table.html` | `/api/active` | Ranked table of MPs by active vote count |
| `similarity_table.html` | `/api/similarity` | Table of cross-party MP pairs by similarity |
| `votes_list.html` | `/api/votes` | Paginated vote rows with tisk links |

## HTMX Pattern

Each analysis page follows the same pattern:

```html
<!-- Form with HTMX attributes -->
<form hx-get="/api/loyalty"
      hx-target="#results"
      hx-trigger="load, submit"
      hx-indicator="#loading">
    <!-- Controls (sliders, filters) -->
</form>

<!-- Loading spinner -->
<div id="loading" class="htmx-indicator">Loading...</div>

<!-- Results container (swapped by HTMX) -->
<div id="results"></div>
```

- `hx-trigger="load"` fires on page load for initial data fetch
- `hx-trigger="submit"` fires when the user changes controls
- `hx-indicator` shows a spinner during requests
- The API returns an HTML partial that replaces `#results`

## Charts

Chart images are embedded as `<img>` tags pointing to `/charts/*.png` endpoints. They render server-side via matplotlib/seaborn with a dark theme matching the UI (`#1a1a2e` background).

## Methodology Sections

Each analysis page includes a collapsible `<details>` element explaining the calculation methodology — this serves as inline documentation for users.

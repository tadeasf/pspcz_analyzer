# Templates

Frontend is server-rendered HTML with HTMX for dynamic updates. No JavaScript framework — just Jinja2 templates + HTMX attributes. All user-visible strings are localized via the Jinja2 i18n extension.

## Localization (i18n)

All templates use `{{ _("key") }}` for translatable strings. The Jinja2 i18n extension is installed on all template environments at startup via `setup_jinja2_i18n()`.

The `lang` variable (from `request.state.lang`, set by `LocaleMiddleware`) is available in all template contexts and controls:

- Which translation strings are displayed
- Which AI summary variant to show (Czech or English, with fallback)
- The `<html lang="...">` attribute

## Base Layout (`templates/base.html`)

All pages extend `base.html`, which provides:

- **Head**: Pico CSS v2 (CDN), HTMX v2.0.4 (CDN), external stylesheet (`/static/style.css`)
- **Favicon**: SVG favicon (`/static/favicon.svg`) — Czech lion motif in institutional blue
- **Meta tags**: description, theme-color for mobile browsers
- **Navigation**: Logo + "PSP.cz Analyzer" branding, page links, API Docs link, period selector
- **Language switcher**: CZ/EN toggle links in the header (active language highlighted)
- **Footer**: data source attribution + educational project disclaimer (separated by CSS border)
- **Theme**: Pico CSS dark mode (`data-theme="dark"`)

Active navigation item is highlighted via `aria-current="page"`, controlled by the `active_page` template variable.

## Page Templates

Each page template renders a full page with controls and a `#results` div that HTMX populates:

| Template | Page | Controls |
|----------|------|----------|
| `index.html` | Dashboard | None (static stats + feature cards) |
| `loyalty.html` | Party Loyalty | Top N slider, party filter dropdown |
| `attendance.html` | Attendance | Top N slider, sort toggle (worst/best/most active), party filter |
| `similarity.html` | Similarity | Top N slider |
| `votes.html` | Votes Browser | Search input, outcome filter, topic filter, pagination |
| `vote_detail.html` | Vote Detail | None (static detail view) |

### Vote Detail Template (`vote_detail.html`)

The most complex template. Displays:

1. **Vote metadata** — ID, date/time, session/vote numbers, outcome badge
2. **Topic tags** — colored badges from keyword or LLM classification
3. **AI summary** — bilingual summary card (shows English when `lang == "en"` and English summary exists, otherwise Czech)
4. **Current stage** — badge showing which legislative stage this vote belongs to
5. **Legislative timeline** — visual timeline of the bill's progress through parliament (CSS timeline with dots, active stage highlighted with glow effect)
6. **External links** — to psp.cz vote page and source document
7. **Tisk transcription** — lazy-loaded via HTMX (`hx-trigger="intersect once"`) from `/api/tisk-text`
8. **Tisk evolution** — lazy-loaded sub-version comparison with bilingual LLM diff summaries
9. **Vote counts** — stat cards (For/Against/Abstained/Did not vote)
10. **Party breakdown** — table with per-party vote counts
11. **Individual MPs** — collapsible table of all MP votes with color-coded results

## Partials (`templates/partials/`)

HTML fragments returned by `/api/*` endpoints for HTMX swaps:

| Partial | Endpoint | Content |
|---------|----------|---------|
| `loyalty_table.html` | `/api/loyalty` | Ranked table of MPs by rebellion rate |
| `attendance_table.html` | `/api/attendance` | Ranked table of MPs by attendance % (with vote breakdown) |
| `similarity_table.html` | `/api/similarity` | Table of cross-party MP pairs by similarity |
| `votes_list.html` | `/api/votes` | Paginated vote rows with tisk links |
| `tisk_evolution.html` | `/api/tisk-evolution` | Sub-version comparison with bilingual diff summaries |

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

## Skeleton Loading States

When switching periods or navigating between pages, JavaScript replaces `<main>` content with animated skeleton placeholders:

- `skeleton-pulse` — base class with pulsing animation (0.4→0.8 opacity)
- `skeleton-heading` — placeholder for headings (2rem height, 50% width)
- `skeleton-line` — placeholder for text lines (variants: short/medium/long)
- `skeleton-table-row` — placeholder for table rows
- `skeleton-card` — placeholder for stat cards

The skeleton is shown immediately on period change (`switchPeriod()`) and on nav link clicks, before the full page reload completes. This prevents stale data from being visible during loading.

## Charts

Chart images are embedded as `<img>` tags pointing to `/charts/*.png` endpoints. They render server-side via matplotlib/seaborn with a dark theme matching the UI (`#1a1a2e` background). Chart labels and titles are localized via `gettext()`.

## Styling

All custom CSS lives in `/static/style.css` (external file). Pico CSS v2 dark theme is the base. Key customizations:

- `--pico-border-radius: 0.25rem` — sharper corners for a more formal/institutional feel
- Legislative timeline styles (dots, lines, active glow)
- Skeleton loading animation
- Stat grid layout
- Navigation period selector styling
- Language switcher (`.lang-switcher`) — flex layout with active state highlight
- Footer with CSS border separator (no `<hr>`)

## Methodology Sections

Each analysis page includes a collapsible `<details>` element explaining the calculation methodology — this serves as inline documentation for users. Methodology text is fully localized.

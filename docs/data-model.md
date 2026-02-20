# Data Model

## Electoral Periods

The Czech Chamber of Deputies operates in electoral periods. Each period has a number, a year identifier (used in psp.cz ZIP filenames), and an organ ID (used in the database).

| Period | Years | Label | ZIP Year | Organ ID |
|--------|-------|-------|----------|----------|
| 10 | 2025–present | Current | 2025 | 174 |
| 9 | 2021–2025 | | 2021 | 173 |
| 8 | 2017–2021 | | 2017 | 172 |
| 7 | 2013–2017 | | 2013 | 171 |
| 6 | 2010–2013 | | 2010 | 170 |
| 5 | 2006–2010 | | 2006 | 169 |
| 4 | 2002–2006 | | 2002 | 168 |
| 3 | 1998–2002 | | 1998 | 167 |
| 2 | 1996–1998 | | 1996 | 166 |
| 1 | 1993–1996 | | 1993 | 165 |

The organ ID mapping is critical — `id_obdobi` in the `poslanec` table uses organ IDs (165–174), not period numbers (1–10).

## UNL File Format

psp.cz distributes data as UNL files inside ZIP archives:

- **Encoding**: Windows-1250 (Czech)
- **Delimiter**: pipe `|`
- **Headers**: none — column order defined in `models/schemas.py`
- **Trailing pipe**: every line ends with `|`, producing an extra empty column (dropped during parsing)
- **Quoting**: some files contain unescaped double-quotes — parsed with `quote_char=None`

## Data Sources (ZIP Archives)

| Archive | URL Pattern | Contents |
|---------|-------------|----------|
| `hl-{year}ps.zip` | `/opendata/hl-{year}ps.zip` | Voting data for one period |
| `poslanci.zip` | `/opendata/poslanci.zip` | MPs, persons, organs, memberships |
| `schuze.zip` | `/opendata/schuze.zip` | Sessions and agenda items |
| `tisky.zip` | `/opendata/tisky.zip` | Parliamentary prints (bills) |

Base URL: `https://www.psp.cz/eknih/cdrom/opendata`

## Key Tables

### Voting Data (per-period)

**hl_hlasovani** — vote summaries (`hl{year}s.unl`):

| Column | Type | Description |
|--------|------|-------------|
| `id_hlasovani` | Int64 | Unique vote ID |
| `id_organ` | Int32 | Organ (chamber) ID |
| `schuze` | Int32 | Session number |
| `cislo` | Int32 | Vote number within session |
| `bod` | Int32 | Agenda item number |
| `datum` | Utf8 | Date (string) |
| `cas` | Utf8 | Time (string) |
| `pro` / `proti` / `zdrzel` / `nehlasoval` | Int32 | Vote counts |
| `prihlaseno` | Int32 | MPs registered |
| `kvorum` | Int32 | Quorum required |
| `vysledek` | Utf8 | Outcome code (see below) |
| `nazev_dlouhy` / `nazev_kratky` | Utf8 | Vote description (long/short) |

**hl_poslanec** — individual MP votes (`hl{year}hN.unl`, multiple files per period):

| Column | Type | Description |
|--------|------|-------------|
| `id_poslanec` | Int64 | MP identifier |
| `id_hlasovani` | Int64 | Vote ID (FK to hl_hlasovani) |
| `vysledek` | Utf8 | Vote result code (see below) |

**zmatecne** — void vote IDs (`hl{year}z.unl`):

| Column | Type | Description |
|--------|------|-------------|
| `id_hlasovani` | Int64 | ID of a void vote |

### Shared Tables

**osoby** — persons:
`id_osoba`, `pred` (title before), `prijmeni` (surname), `jmeno` (first name), `za` (title after), `narozeni`, `pohlavi`, `zmena`, `umrti`

**poslanec** — MP records:
`id_poslanec`, `id_osoba` (FK), `id_kraj`, `id_kandidatka`, `id_obdobi` (organ ID, not period number), `web`, contact fields, `foto`, `facebook`

**organy** — organs/organizations:
`id_organ`, `organ_id_organ` (parent), `id_typ_organu` (1 = parliamentary club), `zkratka` (abbreviation), `nazev_organu_cz/en`, date range, `priorita`

**zarazeni** — memberships:
`id_osoba` (FK), `id_of` (organ FK), `cl_funkce`, `od_o`/`do_o` (membership dates), `od_f`/`do_f` (function dates)

**schuze** — sessions:
`id_schuze`, `id_org`, `schuze` (session number), `od_schuze`/`do_schuze`, `aktualizace`

**bod_schuze** — agenda items:
`id_bod`, `id_schuze` (FK), `id_tisk` (FK to tisky), `bod` (item number), `uplny_naz` (full name)

**tisky** — parliamentary prints:
`id_tisk`, `ct` (print number), `nazev_tisku`, `datum_doruceni`, `id_obdobi`, and more

## Tisk Enrichment Data

Data produced by the background tisk pipeline, stored in the cache directory.

### TiskInfo Model

The `TiskInfo` dataclass (`models/tisk_models.py`) holds enriched data for each parliamentary print:

| Field | Type | Description |
|-------|------|-------------|
| `ct` | int | Print number |
| `nazev` | str | Print name |
| `url` | str | psp.cz URL |
| `topics` | list[str] | Topic labels (from LLM or keyword classification) |
| `summary` | str | Czech AI summary |
| `summary_en` | str | English AI summary |
| `has_text` | bool | Whether extracted PDF text exists |
| `sub_versions` | list[dict] | Sub-tisk versions with diff summaries |
| `law_changes` | list[str] | Laws changed by this print |
| `history` | TiskHistory | Legislative process timeline |

### PDF Text Cache

Extracted plain text from parliamentary print PDFs:

```
~/.cache/pspcz-analyzer/psp/tisky_text/{period}/{ct}.txt
```

### Topic Classifications

Per-period topic classification stored as Parquet files:

```
~/.cache/pspcz-analyzer/psp/tisky_meta/{period}/topic_classifications.parquet
```

Columns: `ct` (print number), `topic` (serialized topic labels), `summary` (Czech), `summary_en` (English), `source` (classification method).

Topics are assigned either by keyword matching (`topic_service.py`) or by LLM classification (`ollama_service.py`). The LLM results take priority when available.

### AI Summaries

Per-tisk summaries generated by Ollama in both Czech and English, stored in the topic classifications Parquet cache alongside topic data.

### Version Diff Summaries

LLM-generated comparison summaries between sub-versions of a parliamentary print:

```
~/.cache/pspcz-analyzer/psp/tisky_version_diffs/{period}/{ct}_{sub_ct}.txt      # Czech
~/.cache/pspcz-analyzer/psp/tisky_version_diffs/{period}/{ct}_{sub_ct}_en.txt   # English
```

### Legislative Histories

Scraped from psp.cz HTML, stored as JSON:

```
~/.cache/pspcz-analyzer/psp/tisky_historie/{period}/{ct}.json
```

Contains the full legislative process timeline (readings, committee reports, Senate, President).

## Configuration

All configuration is via environment variables, loaded from `.env` by `python-dotenv`. Constants are defined in `config.py`.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PSPCZ_CACHE_DIR` | `~/.cache/pspcz-analyzer/psp` | Root cache directory for all data |
| `PSPCZ_DEV` | `1` | `1` for hot reload (dev), `0` for production |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API endpoint |
| `OLLAMA_API_KEY` | *(empty)* | Bearer token for remote HTTPS Ollama |
| `OLLAMA_MODEL` | `qwen3:8b` | Model for inference |
| `DAILY_REFRESH_ENABLED` | `1` | `1` to enable daily data refresh, `0` to disable |
| `DAILY_REFRESH_HOUR` | `3` | Hour (CET, 0-23) at which the daily refresh runs |

### Ollama Configuration

Additional constants in `config.py` (not overridable via env var):

| Constant | Default | Description |
|----------|---------|-------------|
| `OLLAMA_TIMEOUT` | `300.0` | Per-request timeout in seconds |
| `OLLAMA_HEALTH_TIMEOUT` | `5.0` | Health check timeout |
| `OLLAMA_MAX_TEXT_CHARS` | `50000` | Max text length sent to LLM |
| `OLLAMA_VERBATIM_CHARS` | `40000` | Chars included verbatim (rest truncated) |

If Ollama is not running or unreachable, the system silently falls back to keyword-based classification.

## Vote Result Codes

### Individual MP Votes (`hl_poslanec.vysledek`)

| Code | Enum | Meaning |
|------|------|---------|
| `A` | YES | Voted yes |
| `B` | NO | Voted no |
| `C` | ABSTAINED | Abstained |
| `F` | DID_NOT_VOTE | Registered but didn't press button |
| `@` | ABSENT | Not registered in the chamber |
| `M` | EXCUSED | Formally excused |
| `W` | BEFORE_OATH | Before taking oath |
| `K` | ABSTAIN_ALT | Alternative abstain code |

### Vote Outcomes (`hl_hlasovani.vysledek`)

| Code | Meaning |
|------|---------|
| `A` | Passed |
| `R` | Rejected |
| `X` | Invalid |
| `Q` | Invalid (variant) |
| `K` | Invalid (variant) |

Votes in the `zmatecne` table are void and are always filtered out before any analysis.

## Caching Strategy

```
~/.cache/pspcz-analyzer/psp/          (or $PSPCZ_CACHE_DIR)
    raw/              # Downloaded ZIP files
    extracted/        # Extracted UNL files
    parquet/          # Parsed DataFrames cached as Parquet
    tisky_pdf/        # Downloaded parliamentary print PDFs
    tisky_text/       # Extracted plain text from PDFs
    tisky_meta/       # Topic classification + summary Parquet caches
    tisky_historie/   # Legislative history JSON files
    tisky_version_diffs/  # LLM diff summaries (Czech + English)
```

The Parquet cache uses file modification times: if the Parquet file is newer than the source UNL directory, it's loaded directly. Otherwise the UNL files are re-parsed and the Parquet is regenerated.

Column schemas are defined in `pspcz_analyzer/models/schemas.py` — each table has a `*_COLUMNS` list (column order) and a `*_DTYPES` dict (type casts).

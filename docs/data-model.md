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
~/.cache/pspcz-analyzer/psp/
    raw/          # Downloaded ZIP files
    extracted/    # Extracted UNL files
    parquet/      # Parsed DataFrames cached as Parquet
```

The Parquet cache uses file modification times: if the Parquet file is newer than the source UNL directory, it's loaded directly. Otherwise the UNL files are re-parsed and the Parquet is regenerated.

Column schemas are defined in `pspcz_analyzer/models/schemas.py` — each table has a `*_COLUMNS` list (column order) and a `*_DTYPES` dict (type casts).

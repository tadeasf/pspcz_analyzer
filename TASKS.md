# TASKS: data fixes + UX rework (Nov 2026)

Working plan for the feedback round from Hlídač státu (Martin) and Veronika Šmídová (PR/marketing, author of the redesign).
The issues were filed on GitHub from the `michalblaha` account. Their texts were drafted with an AI by non-developers. Each one was checked against the code (main @ `34f3e1d`) on 2026-11-05. Corrections to their diagnoses are noted per task below.

- Overview issues: [#99] (set 1 + 2a: data correctness), [#121] (set 3: look, texts, structure).
- Redesign mockup: [`docs/ux-rework/zakony-pod-lupou.html`](docs/ux-rework/zakony-pod-lupou.html). It is a static single-file page; open it in a browser. Hash routes: `#/prehled`, `#/zakony`, `#/zakon-246`, `#/pozmenovaci-navrhy`, `#/pn-246`, `#/hlasovani`, `#/hlasovani-88540`, `#/loajalita`, `#/loajalita-poslanci`, `#/ucast`, `#/jak`, `#/nahlasit-chybu`.
  - It is a visual and text reference, not code to copy. Values in `[brackets]` are placeholders, and the chart numbers are illustrative.
  - Real data appears only for tisk 246 (dálniční známka) and vote 88540.
  - The author line was changed to "Tadeáš Fořt & Hlídač státu".
- More source material is in `snemovna-sada-3-podklady.zip`, attached to the first comment of [#121]:
  - `texty-podle-stranek.md`: all mockup texts per page.
  - `jak-se-v-tom-vyznat.md`: the verified text for the new guide page, needed for [#118].
  - Not downloaded yet.
- Line numbers below refer to `34f3e1d` and may drift.

Effort: **S** = up to half a day, **M** = 1-2 days, **L** = 3+ days (estimates from code size, not measured).
Timeline promised to the team: roughly 1-2 months of weekend work, pushed continuously.

---

Done items are ticked here; details, commits and remaining gaps per item are in [STATUS.md](STATUS.md).

## Decisions

Moved to [DECISIONS.md](DECISIONS.md). Progress is in [STATUS.md](STATUS.md).


## Phase 0: deployment

- [x] **[#84] Which code version runs on snemovna.hlidacstatu.cz** (P0, S). Code done 2026-11-10 (`version` in both health endpoints + footer, see `docs/deployment.md`); the server checks below are still open.
  - Findings:
    - No git tag contains `66e5b9f`.
    - `release.yml` tags images with the pyproject version (`0.1.0`) plus `:latest`, which never matches the git tags (1.2.x).
    - `.dockerignore` excludes `.git`.
    - Compose builds frontend and backend separately (`build: .`), so the backend (pipelines) may run an older build than the frontend.
  - Fix: pass a `GIT_SHA` build arg into the image and show it in `/health` (`routes/health.py:18-21`) and in the footer.
  - Server checks: dropped 2026-11-19. Work happens locally on `feat/zakony-pod-lupou`; Martin deploys when it is ready.
  - Former server checks:
    - `git -C <deploy dir> log -1` and `git status`.
    - `docker ps` creation times for both containers.
    - `docker exec <c> grep -c normalize_czech /app/pspcz_analyzer/services/law_service.py`. 0 means the build predates `66e5b9f`.
    - `grep '"VL": ("sbirka"' .../history_scraper.py` in the backend container. A match means it predates `dcfd756`.
  - The team was told that pushing continuously and deploying from main should fix the mismatch.

## Phase 1: data correctness (set 1, [#99])

Recommended order from [#99]: #84 → #85 + #92 → #86 + #87 → #91 → #88, #89, #90 → #93, #94 → #95, #97, #98 → #96.
Unit tests must run on saved snapshots (HTML history pages, opendata files); the repo has none yet (`tests/unit/services/test_history_status.py` uses synthetic HTML). Comparing a whole period against live psp.cz is a separate integration script, recommended in [#99] as the regression guard.

### 1A. Votes

- [x] **[#85] Votes linked to the wrong tisk (proposed vs approved agenda)** (P0, M). Diagnosis correct. Done 2026-11-10.
  - Result on local data: lookup entries 819 → 765 (period 10), 6054 → 5597 (9), 8553 → 8333 (8). Also drops undocumented `pozvanka` 0 rows (written interpellations colliding with real items, periods up to 9).
  - Not fixed: 274 approved (schuze, bod) pairs across all periods carry more than one tisk; the last row still wins.
  - `lookup_builder.py:88-92` never filters on `pozvanka`. It also drops rows without a tisk before the overwrite at `:128`, so an approved item with no tisk inherits the proposed item's tisk.
  - Reproduced locally on 86822, 86479 and 87746. 584 votes differ between the two agendas.
  - Fix: filter `bod_schuze` (and `schuze`) to `pozvanka` null before the tisk filter, and skip sessions marked unapproved in `schuze_stav.unl`.
  - Side effects:
    - `/laws` (`law_service.py:86,159`) and the amendment candidates (`identifier.py:53`) are built from `tisk_lookup.values()`, so tisky that appear only on proposed agendas drop out of `/laws` (touches [#98]).
    - The lookup rebuilds on restart.
    - Amendment outputs are cached by (schuze, bod), so the **amendment pipeline must be re-run** (LLM time).
- [x] **[#92] Void votes (zmatečná) not loaded** (P0, S). Done 2026-11-10: void votes are 16 / 114 / 123 for periods 10 / 9 / 8 on local data; the cache key was renamed to `void_votes_{period}`, so no VPS cleanup is needed. Correct for periods 9 and 10 (`data_reader.py:350`); neither zip has `hl{year}z.unl`.
  - **Worse for period 8**: `hl2017z.unl` exists but is a different table. It contains 263 valid vote IDs and none of the 123 real void votes.
  - Fix: load the shared `zmatecne.unl`, log at error level when it is missing, and mark void votes on the detail page.
  - Migration: `get_or_parse` (`data/cache.py:35-38`) checks freshness against the directory mtime, so **delete the wrong `zmatecne_8.parquet` on the VPS** or rename the cache key.
  - Periods 1-7 are not checked.
  - The fix changes attendance, loyalty and similarity numbers for periods 8-10.
- [x] **[#91] Vote code `K` not counted, excuses (omluvy) not loaded** (P0, M). Done 2026-11-19 in `services/vote_codes.py`.
  - Result:
    - One code-to-category map (yes / no / abstained = C,F,K / absent = @,W / excused = M / other), used by vote detail, attendance, amendments and labels.
    - On load, `@` becomes `M` when the vote time is inside an `omluvy.unl` interval, which is the psp.cz rule. `N` is folded into `B`.
    - A load-time check compares MP records with the `hl_hlasovani` summary. It flags 1 vote in period 10 (86480) and 3 in period 8, and all of them are inconsistencies in the psp.cz source.
  - Verified on 88876 against the live psp.cz page: 1 not logged in and 22 excused, which matches exactly. Babiš's attendance is now 91.4 % (it was 29.2 %).
  - Left over:
    - Babiš lands in "Nezařazení" instead of ANO, which is the club bug in [#101].
    - The night-sitting question is answered: `omluvy.unl` splits night sittings into 09:00-23:59 and 00:00-09:00 rows, so matching on the calendar date is correct.
    - The separate "Pasivní" column and sort are removed, since they were always 0 after 1995.
  - Original diagnosis: Correct (`votes_service.py:266-280`, `attendance_service.py:29-39`; `omluvy.unl` is never read).
  - Local period-10 codes are only A/B/@/K. Across 2030 votes, K equals zdržel + nehlasoval in all but one, and A+B+K equals přihlášeno in all.
  - Fix: one code-to-category map used by both services (`ABSTAIN_ALT` already exists in `models/enums.py:16`).
  - Join `omluvy.unl` (`id_organ`, `id_poslanec`, `den`, `od`, `do`) to the vote's `datum`/`cas` to split `@` into absent and excused.
  - Caveats:
    - psp.cz back-fills omluvy, so exact parity is only "as of the day".
    - The night-sitting rule is unknown; verify on a sample of `hlasy.sqw` pages.
    - Omluvy refresh daily for free, since they are in the current-period zip.

### 1B. Tisk history and status

All of these need a **full re-scrape of history JSON** after [#87]. It is cheap: no LLM, about 300 pages for period 10.

- [x] **[#87] History parser keys stages on the mark only, not the section** (P0, M). Correct: `_MARK_MAP` at `history_scraper.py:28` ignores the section, and `_parse_stages` (`:190-201`) reads only `section-content`.
  - Done `b2766d1`: stages typed by (section, mark), publication stage and number from `p.status`; snapshot tests 82, 167, 246.
  - Fix: pass the section heading into `_build_stage` and key on (section, mark). Parse `p.status` for "Zákon vyhlášen ... N/RRRR Sb.".
  - The `p.status` placement is the reporter's claim. It is consistent with the cache, but unverified until there is an HTML snapshot; save snapshots as fixtures.
- [x] **[#86] Wrong statuses and "106/1999 Sb." frozen in the cache** (P0, S, after #87/#88/#90). Correct (`config.py:106`, `metadata_scraper.py:70-76`).
  - Done `cd49500`: `parser_version` (missing = 0), outdated histories re-scraped even when terminal; a pipeline run is still needed to replace the cache.
  - The local cache (current parser) has no 106/1999, so production data comes from an older parser.
  - Fix: add `parser_version` to `TiskHistory` (`history_from_dict` defaults to 0) and re-scrape whenever it is below the current version, terminal or not.
- [x] **[#88] "vyhlášeno" set already when the law is sent for publication** (P1, S). Correct (`:253`).
  - Done `2579ec6`: "vyhlášeno" only from the publication sentence, Sbírka entry alone = "odesláno k vyhlášení"; snapshot 246.
  - Also, "Schválený" substring-matches "schválen" (`:50`, `:106`), so all 10 "vyhlášeno" items in the cache have outcome "schválen" and no law number.
  - Fix: set "vyhlášeno" only from the `p.status` sentence, and add a non-terminal "odesláno k vyhlášení" status.
- [x] **[#89] Stage date is the first date in the paragraph** (P1, S/M). Correct (`:119` and `:122` work independently). Reproduced on tisk 82.
  - Done `39253c2` (S fix): date from the outcome sentence or the nearest earlier dated one; 82 signature 16. 7. 2026. Per-event split (M) not done.
  - Fix: split into sentences and take the date from the sentence that matched the outcome (S). Full per-event splitting changes the stage model and timeline template (M).
- [x] **[#90] Missing statuses: Senate, President, rejected after return** (P1, M). Correct (`:246-271`). "podepsáno" is unreachable because the text says "podepsal".
  - Done `511fa2b`: status from the last decisive stage, new statuses and terminal set, opinion from the Vláda section (the whole-page read gave 82, a government bill, "souhlas"); "zamítnutí"/"stažen z pořadu" guarded by whole-word patterns, not seen in the snapshots.
  - Fix: status follows the last stage typed by section (needs #87). Extend `TERMINAL_TISK_STATUSES`, the status filter and the translations.
  - Unverified suspicions to check on real snapshots:
    - `"zamítnut"`/`"stažen"` also match "zamítnutí" and "stažen z pořadu".
    - `_extract_government_opinion` (`:234-243`) reads the whole page text, the same boilerplate risk as the old 106/1999 bug.
- [x] **[#98] Laws list contains interpellations/reports; "Předkladatel" holds the document type** (P1, S-M). Correct.
  - Data part done `ef56d0d`: `id_druh` in `TiskInfo` and law rows, submitter fixed. Default druh filter + toggle in the list is still open (UI).
  - Period-10 counts by `id_druh`: 1: 55, 2: 86, 3: 2, 4: 19, 5: 58, 6: 39.
  - Fix:
    - Carry `id_druh` into `TiskInfo` (`lookup_builder.py:109-120, 170`) and default the filter to druh 1/2/3 with a toggle.
    - In `_extract_submitter` (`history_scraper.py:206-231`), handle "... vládou předložen", and return empty when the subject is a document type (or set Vláda when `id_druh` is 1, 3 or 4).
  - Cached bad values: "Mezinárodní smlouva" ×19, "Návrh zákona o státním rozpočtu vládou" ×2 (ct 94), "Senát PS" ×8 (odd).
  - `predkladatel.unl` columns are not in `models/schemas.py`; check the psp.cz docs before relying on them.
  - The list only contains tisky that appear on an agenda.

### 1C. Amendments (labels and counts)

- [ ] **[#93] Amendment counts are bod occurrences, not amendments** (P1, M, after #85). Correct.
  - Root cause is deeper: `_identify_third_reading_bods` (`identifier.py:102-148`) makes any (schuze, bod) with more than one vote a candidate if the tisk ever had a 3rd reading.
  - `amendment_count` is `len(amendments)` (`amendment_models.py:103-105`) and includes amendments without a result, so a "not determined" bucket is needed for accepted + rejected + other = count.
  - Fix: group by `ct` in `law_service._build_law_row` / `law_detail` and `amendment_service.list_amendment_bills`, show the sum in `law_detail.html:14`, rename `amendments.bills_found`, and tighten candidate selection.
  - Definition pending (see Decisions: count letters).
- [x] **[#94] "Předkladatel: Opoziční" on a government bill** (P1, S). Correct (`law_service.py:28-31`, `amendment_service.py:49-52`, `translations.py:82`, `amendments_list.html:11`).
  - It is a label fix: relabel to something like "Pozměňovací návrhy podali". The bill's real submitter comes from history (#98).
  - `driver` uses only `submitter_parties`, so committee amendments probably land as "unknown" (not verified).

### 1D. Search and URLs

- [x] **[#95] Search: tisk number, no diacritics, inflected forms** (P1, S-M). Mostly correct, but it misses a **schema bug**: tisky column 15 is named `id_stav` and typed Int32 in `models/schemas.py`, but it holds the full official title. `data/parser.py:58` casts with `strict=False`, so the title silently becomes null.
  - Fix:
    - Rename the column, drop the cast, and search it.
    - A numeric, "tisk N" or "ST N" query matches `t.ct`, and the votes search matches `ct` through `tisk_lookup`.
    - Inflection: prefix match per token on top of `normalize_czech` (cut tokens to max(4, len-2)). It covers poplatky/poplatcích, televize/televizi and česká/české.
  - Full Czech morphology (MorphoDiTa) is not worth it: CC BY-NC-SA models and a heavy dependency.
- [x] **[#96] Search/filters not in the URL** (P2, S). Correct (`laws.html:10`, `votes.html:19`, `amendments.html:19`, pagination partials).
  - Fix: API routes return an `HX-Push-Url` header pointing at `/laws?...`. Page routes accept status/topic/outcome/page and pass them into the initial `hx-get`; today only `laws_page` takes `search` (`pages.py:166`). Back sends `HX-History-Restore-Request`.
  - Also fix: the pagination partials put `search={{ search }}` into the URL without `urlencode`.

### 1E. AI summaries

- [ ] **[#97] Corrupted AI summaries** (P1, M; provenance and citations are L). **Cause found**: the model writes it.
  - All 57 period-10 summaries are `llm:gpt-oss-120b`. ct 21 has "shodu ke **стр**ážitelných regulacím" and ct 34 has "politic**ского** fungování". The source texts contain no Cyrillic, so this is token-level language drift, not encoding.
  - `summarize` (`services/llm/client.py:505`) uses JSON-schema constrained decoding, sets no temperature, and does no output validation.
  - Fix:
    - Post-generation validator: non-Latin script, common English words, stray symbols. On failure, retry, then show with a warning (decided).
    - Store model, date and source version next to `summary`.
    - Regenerate flagged summaries (VPS LLM time, every period).
  - Folds into [#109], which regenerates summaries anyway.

## Phase 2: data features (set 2a, [#99])

Order from [#99]: #100 → #101 → #102 → #103 → #104.

- [ ] **[#100] When the subject of a vote or amendment content can't be determined, say so** (P1, L). Correct.
  - Confidence is stored once per agenda item (`amendment_models.py:93`) and shown to readers (`amendments_list.html:43`, `amendment_detail.html:25,43-47`).
  - A letter with no text of its own is still sent to the LLM with the whole bill's text (`summarizer.py:144-148`), so guessing is possible.
  - Fix:
    - Add source + status (certain/uncertain/undetermined) per `AmendmentVote`.
    - Manual corrections via the admin (see 2X) keyed by (period, schuze, bod, letter), merged by the pipelines but never overwritten.
    - Move the parser score and warnings to the admin.
- [x] **[#101] Club and government/opposition by date of vote; period 9 has none** (P1, S + L). The reporter's cause for period 9 is **wrong**.
  - Confidence vote 77501 is **not in psp.cz open data** (checked in the live `hl-2021ps.zip`). The only period-9 matches are 77497 (173:0) and 77500 (186:0), both near-unanimous, so the guard rejects them (`coalition_detector.py:82`) and the result is "unknown" (`:140`).
  - **Decided fix**: a hand-kept coalition table (period, club, from, to, source), with the confidence vote as a fallback. It also covers the Pirates leaving (exact date still to be checked).
  - The acceptance criterion "77501 by club on the web" cannot come from open data. Options: manual seed, scraping `hlasy.sqw`, or reporting the gap to psp.cz.
  - Correct part: `mp_builder.py:27-33` takes the latest club across all periods. Club organs carry `organ_id_organ`, so a per-period filter is trivial (S).
  - Membership dates `od_o`/`do_o` exist. The club at the date of each vote in every service is L, because `mp_info.party` is one value per MP.
  - The Fiala government is organ 1628, type 5 (17.12.2021-15.12.2025). Ministers who are not MPs have no club.
  - **Extra bug**: `cl_funkce` is never filtered; 112 function rows with an `id_of` matching a club organ id join by mistake.
- [ ] **[#102] What each vote was about, from steno** (P1, L). Correct: steno is used only for 3rd readings (`identifier.py:102-136`), and `_VOTE_NUMBER_RE` (`steno_parser.py:86`) misses "hlasování 278" and "(Číslo 273".
  - Key finding (verified on psp.cz): the vote page `hlasy.sqw?g=<id>` links to the exact steno page and anchor, e.g. `.../stenprot/031schuz/s031088.htm#h10` for vote 10 of sitting 31.
    - The link's period path is broken (`1993ps`); the real page is under `/eknih/2025ps/`.
    - Alternatively, crawl all steno pages of a sitting and collect the `hN` anchors (N = vote number within the sitting). That takes fewer requests.
  - Open data does not help: `nazev_dlouhy` is the agenda item title, `nazev_kratky` is empty (period 10), and there is no vote-to-steno table in the hl zips.
  - The text before each vote is regular. Tisk 246, sitting 31:
    - Vote 8: "Nyní budeme hlasovat pozměňovací návrh hospodářského výboru, který se týká změny jednodenního kuponu z jednoho dne na 24 hodin."
    - Vote 9: "Nyní návrh B1 poslance Papajanovského, který se týká rozšíření daňových příjmů krajů o 6 procent ..." (this also answers the open question in [#99] about who submitted B1).
    - Vote 10: the chair reads the full resolution on the bill as a whole.
  - Agreed approach:
    1. Window = steno text from the end of the previous vote's result to "Zahajuji hlasování".
    2. Rules classify the kind: amendment letter, bill as a whole, rejection, return, procedural, objection, challenge.
    3. The LLM writes the question plus a verbatim quote. Validate that the quote exists in the steno and that letters and numbers match; otherwise "zatím neznáme".
    4. The web shows the question, "podle stenozáznamu", the quote and a link. Errors go through "Nahlásit chybu" into admin corrections.
    5. Before the steno is published, show the agenda item name and a note that the question will be added.
    6. Measure on a random sample of about 100 votes and publish the result.
  - Expected accuracy (estimates): anchoring about 95 %, kind about 85-90 %, free-text question about 70-80 % (bills and amendments almost always right, procedural votes mostly).
  - Risk: more network load and pipeline time from fetching steno for every sitting.
- [ ] **[#103] Who submitted each amendment letter (opposition/government/cross/committee); submitted vs adopted** (P1, M, after #101 and #93). Correct (`amendment_service.py:49-52`; the model has no committee field).
  - Grouped votes, withdrawals, re-votes and technical fixes are already flagged (`amendment_models.py:47-52`).
  - Committee detection relies on parsing PDF headers.
- [x] **[#104] Similarity: real agreement instead of cosine; gov × opposition pairs** (P1, S + S). Correct: only A=+1 and B=-1, everything else 0, `fill_null(0)` (`similarity_service.py:21-35`), and no minimum overlap (`:75-108`). The methodology text in `translations.py:228` is misleading.
  - Fix: agreement = sum over codes c of M_c·M_cᵀ divided by P·Pᵀ (P = MP took part). Minimum shared votes goes in config; show "X z Y".
  - What counts as "voted" depends on [#91]. Gov × opposition needs [#101].

## Phase 2X: shared infrastructure (not a single issue)

- [ ] **Steno crawler for all sittings** (feeds [#102] and [#108]). Extend `services/amendments/steno_scraper.py` beyond 3rd readings: all pages per sitting, cached, incremental in the daily refresh, with negative-cache rules already in place (`STENO_NEGATIVE_CACHE_TTL`).
- [ ] **Speech counts per agenda item** ([#108] "Teď se o tom mluví").
  - From the same pages: speaker turns per bod, excluding the chair's procedural lines.
  - Caveats: steno publication lag (the last few days of the 14-day window will be missing; actual lag not yet checked) and merged debates (sloučená rozprava) that span several items.
- [ ] **Admin extensions** ([#100], [#108], [#109]).
  - Corrections (vote question, amendment fields, uncertain data), homepage pick, AI summary review status and edited text.
  - The backend writes an overrides file into the cache dir with who/when for each change. The frontend reads it through the existing file watcher. Pipelines merge overrides and never overwrite them.
  - See the "Admin users" decision about accounts and access.
- [ ] **Unvoted amendments** ([#112] filter "Zatím se nehlasovalo"). Today the amendment pipeline starts from 3rd-reading votes, so bills between the 2nd and 3rd reading are missing. Parse the consolidated amendment document as soon as it appears (we already download sub-tisk versions).
- [ ] **RSS/Atom feed per tisk** ([#111] "Sledovat tento návrh"). Pending confirmation.
- [x] **Version in footer** (part of [#84]).

## Phase 3: UX rework (set 3, [#121])

Rules from [#121]:
- Czech text from the mockup is the source; English is AI-translated (decided).
- A value appears only once it is computed correctly; until then, "zatím neznáme".

Checkpoints from [#121]:
- **Checkpoint 1**: after the site frame (#105-#108) is deployed.
- **Checkpoint 2**: after Zákony, law detail, Hlasování and vote detail.
- **Final check**: the whole site against the mockup, on desktop and phone.

Total for set 3 alone: about 33 dev-days (estimate), before the extra items from the Decisions table.

### 3A. Site frame (checkpoint 1)

- [x] **[#105] Header, menu, footer, AI notice** (P1, S). Template + i18n. `/similarity` redirects to the loyalty tab. Footer dates come from the max vote `datum` and the history cache file mtime.
- [x] **[#106] Text rules: numbers, dates, states, terms** (P1, M). Only a `thousands` filter exists (`i18n/__init__.py:57`), and templates hard-code `"{:,}"` (e.g. `partials/attendance_table.html:24-28`).
  - Needs Czech number, percent and date filters plus one glossary source, which also feeds the [#118] tooltips.
- [x] **[#107] Icons, illustrations, alternating backgrounds, club colours** (P2, S). CSS plus inline SVG from the mockup. Club colours go in config, per period.
- [x] **[#108] Homepage** (P1, L).
  - Blocks 1, 2 and 4-6 use existing data (`legislation_overview.py`, `bod_schuze`).
  - "Teď se o tom mluví" uses speech counts (2X), with an admin override for the pick.
  - Depends on #85, #87-#90, #95, #102 and #109.
- [ ] **[#109] Short AI summary and descriptive title per tisk** (P1, L).
  - New LLM pass. The current prompt asks for critical analysis and risks (`services/llm/prompts.py:50-58`), which breaks the "descriptive only" rule.
  - New fields: title, one-sentence summary, who/how much/from when, source version. `TiskInfo` has only `summary`/`summary_en` (`models/tisk_models.py:43`).
  - Cheap check that the numbers appear in the tisk text. Review status and manual edits that survive reruns live in the admin (2X).
  - Scope: návrhy zákonů, periods 10 and 9 first (decided). Includes the [#97] validator.

### 3B. Section pages (checkpoint 2 after #110, #111, #113, #114)

- [x] **[#110] Laws: tabs by tisk type, cards, sorting, pagination** (P1, M).
  - `id_druh` is in the schema (`models/schemas.py:211`), but `druh_tisku.unl` is not loaded.
  - Treaties and reports show the last history step as text (proposed). Interpellations come later.
  - Depends on #109, #87-#90, #95, #96, #98 and #103.
- [x] **[#111] Law detail** (P1, L).
  - The restructure itself is M.
  - Senate/President timeline needs the fixed parser. Deadlines (15 days for the President, 30 for the Senate) are cheap to compute.
  - "Decisive vote on top" needs the vote kind from #102.
  - The Senate print number and e-Sbírka links are new extraction (URL scheme not checked).
  - Amendment links go to the consolidated document (decided). "Sledovat" is the RSS feed (pending).
- [ ] **[#112] Amendments: section landing page and per-law detail** (P1, L).
  - Grouping, filters and name search are mostly template work (`submitter_names` exists).
  - The per-amendment link goes to the consolidated document (decided).
  - The "Zatím se nehlasovalo" filter needs the unvoted-amendments pipeline (2X).
  - Depends on #93, #94, #100, #102 and #103.
- [x] **[#113] Votes: list grouped by agenda item, with the question and votes needed** (P1, M).
  - Grouping by (schuze, bod) is trivial.
  - `kvorum` is already loaded (`models/schemas.py:31`) but unused, so the quorum bar is cheap.
  - The question and kind stay "zatím neznáme" until #102.
- [x] **[#114] Vote detail: question, result, why it passed, clubs, MPs by club** (P1, M).
  - The club table and MP grid exist (`services/votes_service.py:266-295`).
  - The question, "Co znamenal hlas" and the steno link come from #102.
  - Challenged/repeated votes: the `hl-2021ps.zip` has `hl2021x.unl` and `hl2021v.unl`, which are not identified yet. Check the psp.cz docs; one may be zpochybnění.
  - Depends on #91, #92, #100, #101 and #102.
- [x] **[#115] Party (dis)loyalty: explanation, rebellion chart, full MP list** (P2, M).
  - Rebel vote rows and `active_votes` are already computed (`services/loyalty_service.py:58-90`), so "X z Y" and the drill-down are cheap.
  - "Nejčastěji u bodu" is one group_by. Add a `/loyalty/all` page and a CSS bar chart instead of the PNG.
  - The "Jen o celých zákonech" toggle needs #102.
- [x] **[#116] Similarity: club-coloured map, top 5 pairs, gov × opposition pairs** (P2, M).
  - `compute_pca_coords` already returns name/party/x/y (`similarity_service.py:47-70`). Render a server-side SVG map with hover titles and links; the "map technology" question in #121 is answered by the mockup itself.
  - Pair tables need #104 and #101.
- [x] **[#117] Attendance: definition, table, chart view** (P2, M). Table plus two server-side SVG charts. The 50-vote threshold and mandate start (from memberships) are cheap. Depends on #91 and #92.

### 3C. Content, states, mobile

- [x] **[#118] "Jak se v tom vyznat" guide replaces the docs link** (P1, M). Static page from `jak-se-v-tom-vyznat.md` (in the [#121] zip). The "O datech" pages must be written from the code.
- [ ] **[#119] Empty and error states, "Nahlásit chybu" form** (P2, S).
  - Feedback is tied to `vote_id` today (`routes/feedback.py:77`, `services/feedback_service.py:20`); make it generic.
  - Detecting a failed PDF iframe load is unreliable, so always show fallback links (the server already checks `_tisk_pdf_exists`).
  - [#99] also notes an intermittent "Failed to load PDF document" on tisky 231 and 246 (cause unknown).
- [ ] **[#120] Mobile and accessibility** (P2, M). CSS plus keyboard/ARIA. axe needs a browser test dependency.
- [ ] **English version**: AI-translate the new Czech texts into `i18n/translations.py` (decided).

---

## Data migration checklist (VPS)

- [x] ~~Delete `zmatecne_8.parquet`~~ Not needed: the cache key was renamed. The old `zmatecne_*.parquet` files are unused and can be deleted any time.
- [ ] Re-scrape all history JSON after [#87] (+ #88-#90) via `parser_version` ([#86]).
- [ ] Re-run the amendment pipeline after [#85] / [#93].
- [ ] Regenerate AI summaries ([#97] validator + [#109] new prompt), periods 10 and 9 first.
- [ ] First full steno crawl after 2X is in place.

## Unverified items to check while working

- Periods 1-7 vote codes checked 2026-11-19: all load, no unknown codes, 0-4 flagged votes per period (source inconsistencies). Periods 1-2 have no omluvy rows, so excused MPs show as not logged in there.
- LLM: Martin switched the model to "Qwen3.8-Flash-Next" (2026-11-06). On 2026-11-19 `ll.hlidacstatu.cz/v1/models` returned 502. Once it is up, read the model id from `/models` and set `OPENAI_MODEL` in `.env` (today `gpt-oss-120b`).
- Actual steno publication lag on psp.cz.
- Whether period 10 needs `hl2025x/v` files (the local July zip had only `s`, `h1`, `h2`) and what `hl2021x.unl` / `hl2021v.unl` contain.
- Source of interpellations (not in our pipeline).
- What Hlídač's "hlídání" can consume (for the RSS decision).
- e-Sbírka URL scheme.
- Exact date the Pirates left the period-9 coalition.
- The local opendata is from July 2026, so counts differ from the issues (e.g. 16 vs 28 void votes in period 10).

[#84]: https://github.com/tadeasf/pspcz_analyzer/issues/84
[#85]: https://github.com/tadeasf/pspcz_analyzer/issues/85
[#86]: https://github.com/tadeasf/pspcz_analyzer/issues/86
[#87]: https://github.com/tadeasf/pspcz_analyzer/issues/87
[#88]: https://github.com/tadeasf/pspcz_analyzer/issues/88
[#89]: https://github.com/tadeasf/pspcz_analyzer/issues/89
[#90]: https://github.com/tadeasf/pspcz_analyzer/issues/90
[#91]: https://github.com/tadeasf/pspcz_analyzer/issues/91
[#92]: https://github.com/tadeasf/pspcz_analyzer/issues/92
[#93]: https://github.com/tadeasf/pspcz_analyzer/issues/93
[#94]: https://github.com/tadeasf/pspcz_analyzer/issues/94
[#95]: https://github.com/tadeasf/pspcz_analyzer/issues/95
[#96]: https://github.com/tadeasf/pspcz_analyzer/issues/96
[#97]: https://github.com/tadeasf/pspcz_analyzer/issues/97
[#98]: https://github.com/tadeasf/pspcz_analyzer/issues/98
[#99]: https://github.com/tadeasf/pspcz_analyzer/issues/99
[#100]: https://github.com/tadeasf/pspcz_analyzer/issues/100
[#101]: https://github.com/tadeasf/pspcz_analyzer/issues/101
[#102]: https://github.com/tadeasf/pspcz_analyzer/issues/102
[#103]: https://github.com/tadeasf/pspcz_analyzer/issues/103
[#104]: https://github.com/tadeasf/pspcz_analyzer/issues/104
[#105]: https://github.com/tadeasf/pspcz_analyzer/issues/105
[#106]: https://github.com/tadeasf/pspcz_analyzer/issues/106
[#107]: https://github.com/tadeasf/pspcz_analyzer/issues/107
[#108]: https://github.com/tadeasf/pspcz_analyzer/issues/108
[#109]: https://github.com/tadeasf/pspcz_analyzer/issues/109
[#110]: https://github.com/tadeasf/pspcz_analyzer/issues/110
[#111]: https://github.com/tadeasf/pspcz_analyzer/issues/111
[#112]: https://github.com/tadeasf/pspcz_analyzer/issues/112
[#113]: https://github.com/tadeasf/pspcz_analyzer/issues/113
[#114]: https://github.com/tadeasf/pspcz_analyzer/issues/114
[#115]: https://github.com/tadeasf/pspcz_analyzer/issues/115
[#116]: https://github.com/tadeasf/pspcz_analyzer/issues/116
[#117]: https://github.com/tadeasf/pspcz_analyzer/issues/117
[#118]: https://github.com/tadeasf/pspcz_analyzer/issues/118
[#119]: https://github.com/tadeasf/pspcz_analyzer/issues/119
[#120]: https://github.com/tadeasf/pspcz_analyzer/issues/120
[#121]: https://github.com/tadeasf/pspcz_analyzer/issues/121

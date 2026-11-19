# DECISIONS

Decisions for the data fixes and the "Zákony pod lupou" redesign (Nov 2026). Agreed with Martin (Hlídač státu) and Veronika Šmídová, or made by Tadeáš where the team left it open. Do not re-litigate; add a new row when something changes.

Plan and per-issue notes: [TASKS.md](TASKS.md). What is done: [STATUS.md](STATUS.md).

| Topic | Decision | Status |
|---|---|---|
| Stack | Build the redesign incrementally on the current Jinja2/HTMX app. No SPA rewrite. The mockup's charts and similarity map are plain inline SVG (no JS libs), so render them server-side with Jinja. The matplotlib endpoints in `routes/charts.py` can be dropped once replaced. | decided |
| URLs | Existing URLs stay (`/laws`, `/votes`, `/amendments`, `/loyalty`, `/attendance`, `/similarity`). Merged sections redirect to the new page with the right tab (e.g. `/similarity` opens the similarity tab of the loyalty page). From [#121]. | decided |
| Missing data | Never guess. Show "zatím neznáme" until a value is computed correctly ([#100] is the rule for everything). | decided |
| Vote question ([#102]) | Build it now. 100 % accuracy is not required. Show the source quote plus a link so readers can verify it, and fall back to "zatím neznáme". Measure accuracy two ways (see the accuracy row). | decided (team left it to me) |
| "Teď se o tom mluví" ([#108]) | Do it properly from the start, ranked by the number of speeches per agenda point in the last 14 days, counted from steno. It shares the steno crawl with [#102]. | decided (my call) |
| Manual interventions | Extend the **existing admin** (`admin/`): homepage pick, AI summary review status, corrections of uncertain data. No separate overrides-file workflow. | decided (my call) |
| AI title + short summary ([#109]) | Regenerate summaries with a new, descriptive-only prompt. Start with návrhy zákonů in periods 10 and 9. | decided |
| Amendment document numbers (č. 1731) ([#111], [#112]) | v1 links to the consolidated amendment document (sub-tisk) that holds the full text of each letter. Pairing letters with written documents is not reliable. | decided ("jj") |
| Amendments not voted yet ([#112]) | Process the consolidated amendment document as soon as it is published, before the 3rd reading, so the "Zatím se nehlasovalo" filter works. | decided (my call) |
| AI summary that fails validation ([#97]) | Show it with a warning, if there is any content (Martin; Veronika preferred hiding it). | decided 2026-11-19 |
| Accuracy check for votes ([#102]) | Both: keep the 30 hard votes of tisk 231 as a fixed regression test (revocations, challenges, references hours back), plus a random sample of about 100 votes across sittings and readings for the overall rate. Publish the result. | decided (Veronika) |
| English | Machine-translate with AI. Do not wait for texts from the team. | decided |
| "Sledovat tento návrh" ([#111]) | RSS/Atom feed per tisk, with no user accounts. If email alerts are wanted later, Hlídač státu already has users and "hlídání" and could consume our feed. Veronika: fine to drop it if it is hard. | decided: RSS, lowest priority, last |
| Amendment count ([#93]) | Count letters, not written proposals (Martin: written ones get re-filed, not all are read in, and one can split into several letters). Count every letter in the consolidated document, including ones never voted on because another letter excluded them or they were withdrawn (Veronika), and show those as "nehlasovalo se". | decided |
| Treaties / reports ([#110]) | Proposal: v1 shows the last history step as text (e.g. "Sněmovna vzala zprávu na vědomí 12. 10. 2026") instead of a status badge. Interpellations are not needed now (Veronika listed them only because the old web showed some). | decided (Veronika, Martin "patrně ano") |
| Site name / domain | "Zákony pod lupou" is a working title. The team decides. | open (not mine) |
| Admin users | The admin is single-user (`ADMIN_USERNAME`) and loopback-only by default (`ADMIN_ALLOWED_IPS=127.0.0.1,::1`). If anyone besides me edits, it needs more accounts and external access (VPN or an IP whitelist). | open, ask when it comes up |
| Work order | 1) site frame from the mockup (#105-#107), 2) #101 club by vote date + regular excuse refresh, 3) remaining non-LLM data fixes, alternating with redesign pages. LLM work (#97, #109, #102 question text) waits until the LLM is back (Sun 2026-11-22). | decided 2026-11-19 |
| Branch and deploy | All work goes to `feat/zakony-pod-lupou`, committed per issue. Tuned locally; Martin deploys when it is ready. No checks against snemovna.hlidacstatu.cz. | decided 2026-11-19 |
| Serena | Removed from the repo, not used. | decided 2026-11-19 |

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

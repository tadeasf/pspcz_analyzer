# STATUS

What is done on `feat/zakony-pod-lupou`, newest first. Plan: [TASKS.md](TASKS.md). Decisions: [DECISIONS.md](DECISIONS.md).

| Date | Item | Commit | Verified |
|---|---|---|---|
| 2026-11-19 | Excuses refresh daily: the daily refresh re-parses `omluvy` with the current-period zip (it holds all periods), and the frontend watcher reloads the period when it changes | `880f1d4` | Unit test for the watcher |
| 2026-11-19 | [#101] Club at the vote date (per-vote `party` from membership dates, only this period's clubs, function rows dropped); hand-kept government table with sources (period 9: Babiš until 16. 12. 2021, Fiala five-party until 30. 9. 2024, then without Piráti); gov/opposition labels use the government on the vote date | `41e93a8` | Vote 88876 matches psp.cz: ANO 80 (69 no, 1 not logged in, 10 excused). David Pražák (left ANO 1. 11. 2026) is counted in ANO for October votes. |
| 2026-11-19 | [#105] Site frame from the mockup (header, menu, AI notice, footer with real dates); [#107] section headers with icons; [#118] guide page (Czech text from the mockup); [#119] generic 'Nahlásit chybu' form | `915d06f` | Screenshots at 1280 px and 390 px, no horizontal scroll; 604 tests |
| 2026-11-19 | [#91] Vote code `K` counted as "zdržel se / nehlasoval"; excuses from `omluvy.unl` turn `@` into `M`; load-time check against the vote summary | `e610f82` | Vote 88876 matches live psp.cz (1 not logged in, 22 excused). Periods 1-10 load, no unknown codes. |
| 2026-11-19 | Serena removed | `173425d` | - |
| 2026-11-10 | [#92] Void votes loaded from `zmatecne.unl` and marked on the vote page | `733ec3f` | Period 10: 28 void votes, same as the issue. |
| 2026-11-10 | [#85] Votes linked only via the approved agenda | `733ec3f` | Unit tests; 86822, 86479, 87746 reproduced locally. |
| 2026-11-10 | [#84] Version (git SHA) in health endpoints and footer | `733ec3f` | Unit tests. |

## Known gaps

- Redesign: only the frame is done. Page contents (laws list, detail pages, homepage, charts) still use the old layout until #108-#117.
- Guide page is Czech only (English shows a note); the "O datech" sub-pages it lists are not written yet.
- #107 leftovers: alternating backgrounds and illustration come with the homepage (#108); club and vote colours come with the charts (#116, #117).
- `/similarity` still has its own page (highlighted as loyalty in the menu); it becomes a loyalty tab with #115/#116.
- Locally no tisk histories are scraped yet, so the footer says "zatím nenačteno" and the laws list has no statuses.
- Per-MP pages (attendance, similarity) show the MP's latest club in the period; per-vote tables use the club on the vote date.
- The "77501 by club" acceptance item of [#101] cannot come from open data (the vote is missing there).
- Periods 1-2 have no excuse data at psp.cz, so excused MPs show as not logged in.
- LLM endpoint is down until Sun 2026-11-22.

[#84]: https://github.com/tadeasf/pspcz_analyzer/issues/84
[#85]: https://github.com/tadeasf/pspcz_analyzer/issues/85
[#91]: https://github.com/tadeasf/pspcz_analyzer/issues/91
[#92]: https://github.com/tadeasf/pspcz_analyzer/issues/92
[#101]: https://github.com/tadeasf/pspcz_analyzer/issues/101

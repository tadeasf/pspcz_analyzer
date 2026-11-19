# STATUS

What is done on `feat/zakony-pod-lupou`, newest first. Plan: [TASKS.md](TASKS.md). Decisions: [DECISIONS.md](DECISIONS.md).

| Date | Item | Commit | Verified |
|---|---|---|---|
| 2026-11-19 | [#91] Vote code `K` counted as "zdržel se / nehlasoval"; excuses from `omluvy.unl` turn `@` into `M`; load-time check against the vote summary | `e610f82` | Vote 88876 matches live psp.cz (1 not logged in, 22 excused). Periods 1-10 load, no unknown codes. |
| 2026-11-19 | Serena removed | `173425d` | - |
| 2026-11-10 | [#92] Void votes loaded from `zmatecne.unl` and marked on the vote page | `733ec3f` | Period 10: 28 void votes, same as the issue. |
| 2026-11-10 | [#85] Votes linked only via the approved agenda | `733ec3f` | Unit tests; 86822, 86479, 87746 reproduced locally. |
| 2026-11-10 | [#84] Version (git SHA) in health endpoints and footer | `733ec3f` | Unit tests. |

## Known gaps

- Club per MP is the latest club, not the club at the vote date ([#101]). Babiš shows as "Nezařazení" on vote 88876.
- Periods 1-2 have no excuse data at psp.cz, so excused MPs show as not logged in.
- LLM endpoint is down until Sun 2026-11-22.

[#84]: https://github.com/tadeasf/pspcz_analyzer/issues/84
[#85]: https://github.com/tadeasf/pspcz_analyzer/issues/85
[#91]: https://github.com/tadeasf/pspcz_analyzer/issues/91
[#92]: https://github.com/tadeasf/pspcz_analyzer/issues/92
[#101]: https://github.com/tadeasf/pspcz_analyzer/issues/101

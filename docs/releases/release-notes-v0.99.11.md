# sluice v0.99.11

**A small CLI safety fix.** `sync start` now validates mutually-exclusive flags *before* prompting for the `--reset-target-data` destructive confirmation. Drop-in upgrade from v0.99.10 — no behaviour change for valid invocations.

## Fixed

- **`sync start --restart-from-scratch --reset-target-data` no longer prompts to DROP the target tables before reporting that the flags are mutually exclusive.** The `--reset-target-data` typed confirmation ("Type 'reset' to confirm") ran ahead of the flag-combination validation, so an operator combining the two flags was asked to authorize dropping every target table and only *after* typing `reset` learned the combination is rejected (the command then aborted without dropping). **No data was ever lost** — the mutex was always enforced, and `--yes` failed cleanly up front — but a *validation* error must fire before a *destructive-action* confirmation. The three sync-start flag-combination checks now run up front, ahead of the prompt. Pinned by a unit test. (Found by post-v0.99.10 release validation.)

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.10. Valid invocations are unaffected; only the error *ordering* for the rejected `--restart-from-scratch` + `--reset-target-data` combination changed (now loud up front, with no destructive prompt).

## Who needs this

- Operators who script `sync start` recovery flows. The fix ensures an invalid flag combination fails fast and loud rather than after a misleading drop-tables prompt — no behaviour change for anyone using valid flag combinations.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.11`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.11`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

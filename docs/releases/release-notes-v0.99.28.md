# sluice v0.99.28

**A silent value clamp/truncation under `--mysql-sql-mode=''` is now reported loudly (Vector B).** The legacy-data escape hatch made MySQL silently clamp out-of-range values (numeric overflow → MAX, over-long string → cut) on write, and sluice's warning guard skipped its check under relaxed mode — so those coercions passed unannounced. They're now a loud WARN, on every bulk-copy path. **Drop-in from v0.99.27.**

## Fixed

- **Silent clamp/truncation under `--mysql-sql-mode=''` → loud WARN (Vector B).** Passing `--mysql-sql-mode=''` relaxes the MySQL target so it accepts legacy zero-dates — but it also makes MySQL **silently** clamp or truncate any other out-of-range / over-long value on write (a numeric overflow becomes MAX, an over-long string is cut, etc.), and exit 0. The post-write warning guard (added for the strict-mode LOAD DATA case) previously **skipped entirely** under relaxed mode, so these coercions were never surfaced — the Vector B gap. sluice now reports each coercion as a loud **one-time-per-column WARN** (not a refusal — you opted into relaxed mode) that names the offending values and the data-preserving remedy (map the column to a fitting type with `--type-override`, e.g. `=decimal(P,S)`, `=text`/`=varchar`, `=datetime`). The check runs on **all three** bulk-copy write paths — `LOAD DATA`, the batched-INSERT fallback (`local_infile=OFF` / geometry columns), and the idempotent upsert path used on resume / parallel chunked copy (≥100k rows) / cold-start — each on a pinned connection so the session-scoped warning list is read reliably. Under strict `sql_mode` (the default) the value is still **refused**, unchanged. Drop `--mysql-sql-mode=''` to have strict mode refuse instead of coerce.

  Grounded on real MySQL: under `sql_mode=''` an out-of-range write clamps (`300→127`, `'toolong'→'too'`, `99999→999`) **and** still flags MySQL's warning list — the signal was always there; the guard simply wasn't looking under relaxed mode.

- **Range/overflow refusals no longer show an empty `Examples: []`.** The guard read `@@warning_count` before `SHOW WARNINGS`; that first read clears MySQL's per-statement diagnostic list, so the subsequent `SHOW WARNINGS` returned nothing and the strict-mode refusal (and the NaN/±Infinity refusal) listed no values. The guard now reads `SHOW WARNINGS` first, so the refusal names the offending values for faster diagnosis.

## Compatibility

- No breaking changes. Drop-in from v0.99.27. Strict-mode (default) behavior is unchanged except the refusal now includes the offending values. The only new behavior is a WARN on coercions under `--mysql-sql-mode=''`, where previously there was silence.

## Who needs this — action required

- **Anyone running a MySQL-target migration with `--mysql-sql-mode=''`.** You'll now see a WARN if the target silently coerced any value. If you see one, the data was clamped/truncated — re-run with `--type-override` on the named column to preserve it, or drop `--mysql-sql-mode=''` to refuse. (If you used `--mysql-sql-mode=''` only to accept legacy zero-dates, prefer the read-side `--zero-date` flag, which handles those without relaxing the whole target.)

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.28`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.28`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

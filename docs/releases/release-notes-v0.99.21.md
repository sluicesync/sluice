# sluice v0.99.21

**MySQL `TINYINT(1)` columns used as real integers are no longer collapsed silently.** sluice maps `TINYINT(1)` to boolean by the documented MySQL convention — but `TINYINT(1)` is only a display width, so a column used as a status code or small enum can hold `2`, `127`, `-1`, etc. Those values were silently collapsed to `true` (even MySQL→MySQL). This release makes that **loud** and adds an integer override to preserve the value. **Drop-in from v0.99.20.**

## Fixed

- **`TINYINT(1)` values outside `{0,1}` are now flagged loudly instead of silently collapsed (Vector D).** The boolean decode maps every non-zero `TINYINT(1)` value to `true`, so a column storing real integers lost its value with no warning, in every direction including MySQL→MySQL. The bulk-copy / snapshot read path now emits a **one-time-per-column `WARN`** naming the `table.column`, an example offending value, and the data-preserving remedy. The boolean default is unchanged (the overwhelming majority of `TINYINT(1)` columns genuinely mean boolean) — but the lossy case is no longer silent.

## Added

- **`--type-override <table>.<col>=smallint` (also `=int` / `=integer`) preserves a `TINYINT(1)` integer column end-to-end.** The override rewrites the IR type the **reader** decodes with, so the cell is read as an integer rather than collapsed to a bool, and carried faithfully — cross-engine (MySQL→Postgres) and same-engine (MySQL→MySQL). `smallint` is the recommended floor: a `TINYINT(1)` (8-bit) value always fits a 16-bit `SMALLINT`, and unlike a `tinyint` override it won't re-emit a MySQL `TINYINT(1)` target column that would re-trigger the boolean mapping on a round-trip. The new out-of-range `WARN` points operators here.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.20. The `TINYINT(1)`→boolean default is unchanged; the only new behavior is a `WARN` on out-of-range values and three new `--type-override` tokens.
- The out-of-range `WARN` currently fires on the `migrate` / `sync` cold-start (bulk-copy / snapshot) read path. The steady-state CDC tail does not yet warn — a tracked follow-up — but the `--type-override` remedy preserves the value on every path immediately.

## Who needs this — action required

- **Anyone migrating a MySQL schema that uses `TINYINT(1)` as a small integer** (a status code, a small enum, a signed count) rather than a 0/1 boolean. On v0.99.21 such a column now emits a `WARN` during the copy. If you see it, re-run with `--type-override <table>.<col>=smallint` (or `=int`) to preserve the integer instead of collapsing it to a boolean.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.21`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.21`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

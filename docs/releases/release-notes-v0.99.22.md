# sluice v0.99.22

**The `TINYINT(1)` out-of-range WARN now covers the CDC read paths too.** v0.99.21 made a `TINYINT(1)` value outside `{0,1}` loud on the bulk-copy / snapshot path; this extends the same warning to the steady-state CDC tail so it can't slip through during continuous sync. Detection-only — no value changes. **Drop-in from v0.99.21.**

## Fixed

- **`TINYINT(1)`-out-of-range detection now spans every read path (Vector D follow-up).** sluice maps `TINYINT(1)` to boolean by the MySQL convention, which collapses any non-`{0,1}` value to `true`. v0.99.21 added a loud, one-time-per-column WARN for this on the bulk-copy / snapshot reader. This release extends the identical WARN to the **binlog CDC reader** and the **PlanetScale/Vitess VStream reader** (including its cold-start COPY), so a non-`{0,1}` value written live during continuous sync is flagged too — not only during `migrate` / `sync` cold-start. The change is detection-only and side-effect-free: the decoded value is byte-identical to before, and the `--type-override <table>.<col>=smallint` (or `=int`) remedy already preserved the integer end-to-end on all paths. Pinned by per-path unit tests; the WARN is per-reader, warn-once-per-`table.column`.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.21. Behavior change is limited to an additional WARN log line on CDC paths for out-of-range `TINYINT(1)` values; no value or schema behavior changes.

## Who needs this

- **Anyone running continuous sync (`sync start`) from a MySQL or PlanetScale/Vitess source whose schema uses `TINYINT(1)` as a small integer.** You now get the same out-of-range WARN on the CDC tail that v0.99.21 added to the initial copy. If you see it, re-run with `--type-override <table>.<col>=smallint` to preserve the integer.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.22`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.22`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

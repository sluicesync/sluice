# sluice v0.99.167

**`--infer-types` now works against a live Cloudflare D1. Live-D1 testing found that v0.99.166's rich-type inference aborts on a real D1 source — D1's query API rejects the validation patterns (`code 7500 "LIKE or GLOB pattern too complex"`) on the first timestamp/UUID column, regardless of table size. The fix is `migrate --stage-local`: replicate the live D1 into a byte-faithful local SQLite file first, then migrate from that file — which also rounds out a per-query CPU limit on large tables. It auto-engages for `--infer-types` + D1, so the feature just works. No data was ever at risk (the failure was a loud abort), and a plain D1 migrate is unchanged.**

## Fixed

**`--infer-types` against a live Cloudflare D1 source (ADR-0145).** v0.99.166 shipped data-validated rich-type inference, but it was only ever tested against a local SQLite file. Run against a real `--source-driver d1`, the validation aborts on the first temporal (`*_at`) or UUID (`*_uuid`) candidate:

```
pipeline: infer-types validate customers.org_uuid: d1: ... HTTP 400:
{"code":7500,"message":"LIKE or GLOB pattern too complex: SQLITE_ERROR"}
```

The conformance checks use long character-class `GLOB` patterns (the UUID pattern is ~356 characters), and D1's SQLite build caps pattern complexity well below that. This is **size-independent** — it fires on a 1,750-row table with pristine data — and was invisible because the local SQLite driver used in tests has a permissive default limit. A second D1 limit (a per-query CPU ceiling, `code 7429`, on an unbounded full-table validation scan over a multi-GB table) is closed by the same fix.

**No data was ever at risk:** the failure is a loud abort, and the conservative default mapping (`INTEGER`→`bigint`, `TEXT`→`text`) was unaffected. But the feature's headline use case — graduating a clean D1 dataset to native Postgres types — was blocked.

## Added

**`migrate --stage-local` — lossless local staging for live D1 (ADR-0145).** It replicates the live D1 into a local SQLite file, then runs the whole migrate (schema, `--infer-types` validation, bulk copy) against that file. Local SQLite has neither D1 query limit, so everything runs locally at full speed:

```
sluice migrate --source-driver d1 --source "d1://<account>/<database>" \
    --target-driver postgres --target "postgres://…" --infer-types
# (staging auto-engages; equivalently pass --stage-local explicitly)
```

- **Byte-faithful, and lossless** — unlike `wrangler d1 export`, which rounds integers > 2⁵³ through a JavaScript double. The replica recreates each table from D1's verbatim `sqlite_master` DDL and copies every cell at its exact storage class via the same `CAST`/`typeof`/`hex` projection sluice's lossless D1 reader uses: integers > 2⁵³, REALs, BLOBs, and NULLs preserved exactly, generated columns recomputed from the DDL, explicit indexes recreated.
- **Identical inference decisions** — because the staged file carries the original conservative SQLite types, `--infer-types` sees exactly what it would on D1.
- **Auto-engages** for `--infer-types` against a D1 source (with a loud notice — the direct path is structurally broken there). `--no-stage-local` opts out (accepting the direct-path limits); an explicit `--stage-local` stages even without inference (e.g. a faster local bulk read). Both flags are D1-only and mutually exclusive.
- The staged file lives in the system temp dir and is removed when the migrate finishes.

## Compatibility

A plain conservative D1 migrate — no `--infer-types`, no explicit `--stage-local` — is **byte-for-byte unchanged**: it still streams directly from D1 with no staging. Staging is D1-only; `--stage-local` against any other source is refused loudly. The fix is validated live end-to-end on a real D1 (the control reproduces `code 7500`; `--stage-local` runs `--infer-types` to completion and lands every row byte-exact) and pinned byte-faithful on a mock across storage classes, integers > 2⁵³, BLOBs, NULLs, generated columns, WITHOUT ROWID, composite PK, and explicit indexes.

## Who needs this

Anyone running `sluice migrate --infer-types` against a live Cloudflare D1 (`--source-driver d1`) — it now works instead of aborting on the first timestamp/UUID column. If you only do a conservative D1 migrate (no `--infer-types`), nothing changes for you.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.167 · **Container:** ghcr.io/sluicesync/sluice:0.99.167
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

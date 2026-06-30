# sluice v0.99.166

**New `--infer-types` (opt-in): promote a SQLite/D1 source's conservatively-typed columns to native Postgres types — `boolean`, `timestamptz`/`timestamp`, `jsonb`, `uuid` — but only after exhaustively validating the actual data, falling back to the safe type otherwise. The convenience of native types without the data-loss risk of name-only heuristics. Off by default; SQLite/D1 source only. Completes the D1/SQLite migration-ergonomics arc.**

## Added

**`--infer-types`: opt-in, data-validated rich-type inference for SQLite/D1 sources (ADR-0144).** SQLite/D1's dynamic storage classes mean sluice maps a SQLite source conservatively and losslessly — `INTEGER`→`bigint`, `TEXT`→`text`. That's the right default (it never fails on type and never loses data), but clean datasets often want native Postgres types. `--infer-types` provides them, safely:

```
sluice migrate --source-driver=d1 --source="d1://…" --target-driver=postgres --target="…" --infer-types
```

- **Promotions:** `INTEGER`→`boolean`, ISO-8601 `TEXT`→`timestamptz`/`timestamp`, JSON `TEXT`→`jsonb`, UUID `TEXT`→`uuid`.
- **Candidate selection by name-hint** (`is_*`/`has_*`/`*_flag`; `*_at`/`*_time`/`created`/`updated`; `*_json`/`metadata`/`payload`/`settings`/`attributes`; `*_id`/`*_uuid`/`uuid`/`guid`) so unrelated columns aren't over-promoted.
- **Exhaustive data validation is the safety gate.** For each candidate, sluice runs one aggregate pushed down to the source (`COUNT(*) … NOT IN (0,1)`, `json_valid` + `json_type IN ('object','array')`, an anchored hex-UUID `GLOB`, an ISO-8601 `GLOB`) and promotes **only when zero values violate** — otherwise the column keeps its safe type and the report says so.

## Why this is safe (where name-only heuristics aren't)

A `*_id` column holding a non-UUID value like `cus_abc123` fails UUID validation and **stays `text`** — no data loss, ever. (That exact case is a total-data-loss failure under aggressive name-only inference.) The inference adds **no new value-conversion code**: it computes validated `--type-override` entries and rides the existing override decode, which **loud-refuses** any value it can't parse — so even a validation gap can only loud-fail, never silently coerce.

Temporal handling is **tz-aware**: `timestamptz` only when every value carries an explicit offset/`Z`, otherwise naive `timestamp` (sluice never invents a zone). A **mixed** offset/naive column, or a column with **sub-microsecond** fractions, is refused (kept `text`) — so an offset value can't be silently UTC-shifted into a naive column, and a fraction can't be silently rounded under `timestamp(6)`. (`jsonb` normalizes the document — the JSON value is equal, the stored bytes differ; the report calls this out.)

A loud, structured report names every promotion (with the validated row count) and every considered-but-kept-safe column, so you see exactly what happened.

## Compatibility

Opt-in: with no `--infer-types`, behavior is byte-for-byte unchanged conservative mapping. SQLite/D1 source only (other source engines already carry rich static types — using it elsewhere is refused loudly). Scoped to `migrate`. An explicit `--type-override` always wins over an inferred type. Pinned per type-family on a real Postgres target.

## Who needs this

Anyone migrating a clean, well-formed Cloudflare D1 / SQLite dataset into Postgres who wants native `boolean`/`timestamptz`/`jsonb`/`uuid` columns instead of `bigint`/`text` — without the ALTER-everything-afterward chore or the data-loss risk of name-only type guessing.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.166 · **Container:** ghcr.io/sluicesync/sluice:0.99.166
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

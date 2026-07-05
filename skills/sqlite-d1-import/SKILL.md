---
name: sqlite-d1-import
description: Use to import a SQLite file or a Cloudflare D1 database into Postgres or MySQL. Drives `sluice migrate --source-driver sqlite|d1` with `--stage-local`, `--infer-types`, `--sqlite-date-encoding`, and the big-integer fidelity gotchas. Gated — state-changing (writes to the target). Trigger when the user asks to import / migrate a SQLite database or a Cloudflare D1 database.
---

# sqlite-d1-import

Import a SQLite file or a live Cloudflare D1 database into a Postgres/MySQL target via the standard `migrate` pipeline. State-changing (writes the target); the SQLite/D1 source is read-only and migrate-only (for continuous sync, use the `sqlite-trigger` / `d1-trigger` engines instead — see the operator doc).

## When to use
The source is a `.db` SQLite file, a `.sql` dump, or a Cloudflare D1 database. The target is Postgres or MySQL.

## Inputs you need
- Target DSN + driver (env: `SLUICE_TARGET`).
- For SQLite: `--source <path|file:…|sqlite://…>` (a `.db` file or a `.sql` dump — sluice sniffs the header). For D1: `--source d1://<account_id>/<database_id>` (or `d1://<database_id>` with `CLOUDFLARE_ACCOUNT_ID`) and `CLOUDFLARE_API_TOKEN` in the env (env-only — there is no flag, and it is never logged).

## Steps

1. **Pick the D1 path deliberately — big-integer fidelity is the gotcha.** Integers > 2^53 are LOST on both of D1's default export paths (`wrangler d1 export` rounds server-side; the default JSON query API serializes as IEEE-754 double). The **only lossless** D1 read is `--source-driver d1`, the live query-API reader — it reads each value via `CAST(col AS TEXT)` + `typeof()`, so integers > 2^53 (snowflake IDs, ns timestamps, big counters) round-trip exactly. So: D1 **without** big integers → the simple `wrangler d1 export dump.sql` then `migrate --source-driver sqlite --source dump.sql` path is exact and fine; D1 **with** big integers → use `--source-driver d1`.

2. **Preflight (read-only).** `sluice migrate --dry-run --format json --source-driver <sqlite|d1> --source <…> --target-driver <drv> --target "$SLUICE_TARGET"` — or run `migrate-preflight`. Confirm the type translation before moving data.

3. **Set the date/bool encoding.** SQLite/D1 have no native DATE/TIME/BOOLEAN storage. sluice maps a column by its **declared** type and you tell it how the **values** are stored with `--sqlite-date-encoding`: `iso` (default, ISO-8601 TEXT) | `unixepoch` | `unixmillis` | `julian`. A value whose storage class doesn't match the chosen encoding (or a non-0/1 boolean) is **refused loudly, naming the row** — never a silently-wrong date. Carry an outlier column raw with `--type-override <table>.<col>=text`.

4. **(SQLite/D1) promote conservative types with `--infer-types`.** Optionally promote name-hinted columns (`is_*`, `created_at`, `*_json`, `*_id`) to richer PG types (boolean / timestamptz / jsonb / uuid) — but ONLY after exhaustively validating every non-NULL value conforms; a single non-conforming value keeps the column at its safe type and reports it. It is refused against a non-SQLite/D1 source. An explicit `--type-override` always wins.

5. **(Live D1) stage locally when inferring.** On a live D1 source, `--infer-types` **auto-engages `--stage-local`**: sluice first replicates the D1 database byte-faithfully into a local SQLite temp file (big integers included), then runs validation + the bulk read locally at full speed — because D1's HTTP query API enforces a per-query CPU ceiling and a pattern-complexity limit that abort the validation on the direct path (the `--infer-types` validation uses D1-compatible length/substr checks, but staging avoids the round-trip cost entirely). Set `--stage-local` explicitly to stage without inference; `--no-stage-local` opts out (accepting that D1's limits may abort validation). The staged temp file is removed when the migrate finishes.

6. **Run + verify.** Drop `--dry-run`, keep `--format json`, run the migrate, then `fidelity-verify` (count mode is the cross-engine content check).

## What you return
- **Path chosen + why:** SQLite file / `.sql` dump / `--source-driver d1` — and, for D1, whether big-integer fidelity forced the query-API path.
- **Encoding decisions:** the `--sqlite-date-encoding` used and any `--type-override` outliers; any `--infer-types` promotions accepted vs kept-safe-and-reported.
- **Staging note (live D1 + infer):** that local staging engaged and the temp file was cleaned up.
- **Result:** the migrate outcome + the `fidelity-verify` verdict; any loud refusal (storage-class mismatch, non-portable expression) surfaced with the row it named.

## References (canonical — don't duplicate)
`docs/operator/sqlite-d1-import.md` (the full path matrix, big-integer verification, date/bool table) · `AGENTS.md` (`CLOUDFLARE_API_TOKEN` env-only) · `skills/migrate-preflight/SKILL.md` · `skills/fidelity-verify/SKILL.md` · `sluice migrate --help`.

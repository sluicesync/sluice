---
name: migrate-preflight
description: Use BEFORE running a sluice migrate or sync start, to assess whether it will succeed and surface risks. Drives `migrate --dry-run --format json` plus schema preview/diff and the connectivity/ownership/keyless preflights, then returns a go/no-go with every risk named. Read-only — never writes to either database. Trigger when the user asks to migrate/copy/move/sync a database, or asks "will this migration work / what will it do".
---

# migrate-preflight

Assess a planned migration or sync **before** running it, and hand the human a go/no-go decision backed by evidence. This is sluice's "validate end-to-end before you commit" tenet, run as a checklist. It is entirely read-only.

## When to use
The user wants to migrate or continuously-sync a database and you are about to run `sluice migrate` or `sluice sync start`. Always run this first — the dry-run is free and catches the expensive failures (unsupported types, missing extensions, ownership traps, non-empty target) before any data moves.

## Inputs you need
- Source and target DSNs (prefer env: `SLUICE_SOURCE` / `SLUICE_TARGET` — see `AGENTS.md`; never put credentials in argv).
- The source driver if it isn't a plain DSN (`--source-driver planetscale` for PlanetScale VStream, `--source-driver d1` for Cloudflare D1, etc.).
- Whether this is a one-time `migrate` or a continuous `sync`.

## Steps

1. **Confirm engines + connectivity.** `sluice engines` lists registered engines. A dry-run also connects, so a `SLUICE-E-CONNECT-*` code here means fix the DSN/network before anything else (hand to `sluice-error-triage`).

2. **Run the dry-run plan — the table roster.**
   ```sh
   sluice migrate --dry-run --format json --source-driver <drv> --target-driver <drv> --source "$SLUICE_SOURCE" --target "$SLUICE_TARGET"
   ```
   `--target-driver` is **required** — omitting it is a kong usage error (exit **80**), which is a bad invocation, NOT a preflight refusal (the refusal taxonomy is exit 3; don't confuse the two). The JSON is `{"command","dry_run":true,"plan":{...}}`, and each `plan` table is `{name, columns, primary_key, secondary_indexes, foreign_keys, row_count}` — a **roster + shape only**: the table list, the no-PK tables (`primary_key:false`), and row-count estimates. It does **not** contain translated types, index/constraint phases, or warnings — those live in Step 3, and any translation WARN prints to **stderr**, never into this JSON. For a continuous sync, use `sync start --dry-run --format json` (same shape, plus the CDC/slot plan).

3. **Inspect schema translation — the primary risk surface.** This is where the real risk data lives, not the dry-run. `sluice schema preview --format json …` reports how each source type maps to the target — the `translations[]`, `hints[]`, `unsigned_bigint_narrowings[]`, and the full target `ddl`; `sluice schema diff --format json …` (exit 1 = drift found) compares an existing target. **Also capture stderr** — translation WARN lines print there, not in the JSON. Flag every warning and every refusal:
   - unsupported / extension-owned types → `SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED` (needs `--enable-pg-extension <ext>`);
   - **unsigned-bigint narrowing** — MySQL `BIGINT UNSIGNED` maps to PG `bigint`; it shows in `unsigned_bigint_narrowings[]` / stderr. The mapping is visible here, but whether real data exceeds 2^63−1 is **not** (see the caveat box);
   - **binary/blob column DEFAULTs** — scrutinize `DEFAULT` clauses on binary/`BYTEA` columns in the preview `ddl`: a MySQL hex default that its `information_schema` reports truncated (a bare `0x` with no digits — e.g. `BINARY(1) DEFAULT 0x00`) can mistranslate to a **silently wrong** target default. Flag any binary-default column for manual verification;
   - **no-PK / keyless tables** (copied but not CDC-trackable — call out for a sync; the dry-run's `primary_key:false` is the signal);
   - Postgres target **ownership**: if sluice warns the target connects as an ephemeral `pscale_api_*` role, created objects are owned by it — surface the advisory (sluice never auto-`ALTER OWNER`).

   **⚠ What preflight CANNOT see — value-level hazards.** No preflight command scans a single row *value*; `--dry-run` and `schema preview` read schema + row-counts only. So value-level refusals that fire at **copy time** are **structurally invisible** to this skill: a `BIGINT UNSIGNED` holding a value above 2^63−1 (→ `SLUICE-E-BULKCOPY-TABLE-FAILED`, "greater than maximum value for int64"), a MySQL zero/partial date `0000-00-00` (needs `--zero-date=null|epoch`), a NUL byte in text. **Do not report GO on a clean dry-run** for any table carrying **legacy MySQL data** (zero-dates, unsigned bigints at scale, non-UTF8): treat it as an **unquantified NO-GO risk** and recommend a **trial copy of that table** (`migrate --include-table=<t>`) — the only real preflight for value hazards.

4. **Check the target's starting state.** A non-empty target table is `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` at run time. Note whether recovery would need a **destructive** flag (`--reset-target-data --yes` for sync, `--resume` for migrate, `--force-cold-start` to override) — those require explicit human approval (see `AGENTS.md` destructive-flags list); never pre-authorize them.

## What you return — the go/no-go
A short report:
- **Verdict:** GO / GO-WITH-RISKS / NO-GO.
- **Plan summary:** N tables, M rows (estimate), directions, one-time vs continuous.
- **Risks, each named:** the specific type/value/extension/ownership/keyless issue + its `SLUICE-E-*` code + the remedy (from `docs/operator/error-codes.md`). Distinguish *refusals sluice will make* (must be resolved) from *advisories* (proceed with awareness).
- **Destructive steps required (if any):** named explicitly, flagged as needing human approval.
- **Next command:** the exact `migrate`/`sync start` invocation to run once approved (drop `--dry-run`, keep `--format json`), followed by `fidelity-verify`.

Do **not** run the migration itself from this skill — preflight ends at the recommendation. On any `status:"refused"` or exit 3 in the dry-run, stop and surface `error.hint`; do not retry unchanged. **Code-vs-message split:** a value-level copy failure returns the generic top-level `"code":"SLUICE-E-BULKCOPY-TABLE-FAILED"` — the *specific* cause (`SLUICE-E-VALUE-ZERO-DATE`, the int64-overflow text, …) is in the free-text `message`/`hint`, so match those, not just the `code` field.

## References (canonical — don't duplicate)
`AGENTS.md` (workflow, taxonomy, envelope) · `docs/operator/error-codes.md` (codes → remedies) · `docs/type-mapping.md` / `docs/value-types.md` (translation policy) · `sluice migrate --help`.

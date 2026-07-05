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

2. **Run the dry-run plan.**
   ```sh
   sluice migrate --dry-run --format json --source-driver <drv> --source "$SLUICE_SOURCE" --target "$SLUICE_TARGET"
   ```
   The JSON is `{"command","dry_run":true,"plan":{...}}`. Read the `plan`: the tables to copy, the translated target types, the index/constraint phases, and any warnings. For a continuous sync, use `sync start --dry-run --format json` instead (same shape, plus the CDC/slot plan).

3. **Inspect schema translation.** `sluice schema preview --format json …` shows how each source type maps to the target; `sluice schema diff --format json …` (exit 1 = drift found) compares an existing target. Flag every translation **warning** and every **refusal** in the plan — these are the migration's real risks:
   - unsupported / extension-owned types → `SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED` (needs `--enable-pg-extension <ext>`);
   - value hazards the target can't hold (MySQL zero dates → `SLUICE-E-VALUE-ZERO-DATE`; NUL-in-text → `SLUICE-E-VALUE-NUL-BYTE`; non-portable SQLite expressions → `SLUICE-E-EXPR-*`);
   - **no-PK / keyless tables** (copied but not CDC-trackable — call this out for a sync);
   - Postgres target **ownership**: if sluice warns the target connects as an ephemeral `pscale_api_*` role, created objects are owned by it — surface the advisory (sluice never auto-`ALTER OWNER`).

4. **Check the target's starting state.** A non-empty target table is `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` at run time. Note whether recovery would need a **destructive** flag (`--reset-target-data --yes` for sync, `--resume` for migrate, `--force-cold-start` to override) — those require explicit human approval (see `AGENTS.md` destructive-flags list); never pre-authorize them.

## What you return — the go/no-go
A short report:
- **Verdict:** GO / GO-WITH-RISKS / NO-GO.
- **Plan summary:** N tables, M rows (estimate), directions, one-time vs continuous.
- **Risks, each named:** the specific type/value/extension/ownership/keyless issue + its `SLUICE-E-*` code + the remedy (from `docs/operator/error-codes.md`). Distinguish *refusals sluice will make* (must be resolved) from *advisories* (proceed with awareness).
- **Destructive steps required (if any):** named explicitly, flagged as needing human approval.
- **Next command:** the exact `migrate`/`sync start` invocation to run once approved (drop `--dry-run`, keep `--format json`), followed by `fidelity-verify`.

Do **not** run the migration itself from this skill — preflight ends at the recommendation. On any `status:"refused"` or exit 3 in the dry-run, stop and surface `error.hint`; do not retry unchanged.

## References (canonical — don't duplicate)
`AGENTS.md` (workflow, taxonomy, envelope) · `docs/operator/error-codes.md` (codes → remedies) · `docs/type-mapping.md` / `docs/value-types.md` (translation policy) · `sluice migrate --help`.

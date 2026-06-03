# sluice v0.78.4 — PG Row-Level Security preflight (refuse-loudly)

**Headline:** Closes a catalogued silent-loss class operator-flagged from [PlanetScale's "RLS sounds great until it isn't"](https://planetscale.com/blog/rls-sounds-great-until-it-isnt) blog. Prior to v0.78.4, sluice silently proceeded against an RLS-enabled PG source whose connecting role lacked `BYPASSRLS` — policy `USING` expressions filtered the snapshot down, migration "succeeded" with fewer rows than the source, no error surfaced. v0.78.4 refuses loudly with an operator-actionable recovery hint before any data movement starts.

## Added

- **`feat(pipeline, engines/postgres): PG Row-Level Security preflight (#52 sub-deliverable 1)`**

  Sluice now probes every included table for `pg_class.relrowsecurity` + `relforcerowsecurity`, and probes the connecting role for `pg_roles.rolbypassrls`, then refuses to proceed if any included table has RLS enabled AND the connecting role lacks `rolbypassrls=true`. Runs on **both** source-read and target-write sides with appropriately distinct refuse-message wording:

  - **Source side** warns about silent filter: *"rows that fail the USING expression are silently filtered out of the source snapshot (silent data loss; the migration would 'succeed' with fewer rows than the source)."*
  - **Target side** warns about WITH CHECK rejection: *"INSERTs that fail any WITH CHECK expression are rejected with 'new row violates row-level security policy' mid-bulk-copy or mid-CDC-apply."*

  **FORCE ROW LEVEL SECURITY explicitly called out**: when any offending table has `relforcerowsecurity=true`, the message adds: *"At least one table is marked FORCE ROW LEVEL SECURITY — even the table owner is RLS-checked under FORCE; you need BYPASSRLS regardless of table ownership."*

  **Operator-actionable recovery hint** offers three paths:

  > (a) grant BYPASSRLS to the sluice role: `ALTER ROLE sluice_app BYPASSRLS;` [preferred — the documented PG-source / PG-target prep step];
  > (b) re-run sluice with a superuser or table-owner role that already has BYPASSRLS (note: a non-superuser owner still needs BYPASSRLS when the table is FORCE-RLS);
  > (c) explicitly scope the table(s) out of the migration via `--exclude-table` if the data they hold is intentionally tenant-scoped and should not cross to the target.

  **Diagnose-bundle integration**: `sluice diagnose` standard-level bundle now reports per-table RLS state (`enabled` / `forced`) + the connecting role's `rolbypassrls` attribute under `EngineState.rls`. Operators can run diagnose to see the state before attempting a migration.

## Tests

- **Unit (12 sub-tests)** — `internal/pipeline/rls_preflight_test.go` exercises the 4 cells of {RLS on/off} × {role BYPASSRLS yes/no} + the FORCE-RLS variant + source-vs-target-side wording + multiple-offenders-sorted + empty-schema no-op + missing-prober no-op + probe-error propagation.
- **Integration (9 tests)** — `internal/engines/postgres/rls_preflight_integration_test.go` boots a real PG container with a fixture creating `rls_off` / `rls_on` / `rls_force` tables + a non-superuser `sluice_app` role explicitly `NOBYPASSRLS NOSUPERUSER`. Pins the catalog SQL against actual `pg_class` / `pg_roles` values, validates the diagnose bundle's rendered JSON, and **verifies an unprivileged role's INSERT into a FORCE-RLS table is actually refused by PG** (ground-truth pin on the silent-loss class the preflight prevents).
- **Lowercase control** — non-RLS PG → PG migrations continue to work clean; no false-positive refusal on the common case.

## Compatibility

- **Drop-in upgrade from v0.78.3.** Behaviour change: if an operator was previously running sluice against an RLS-enabled PG source/target with a non-BYPASSRLS role, the migration was silently filtering or failing opaquely; v0.78.4 will now refuse loudly with the recovery hint. **This is a deliberate user-visible change** consistent with the loud-failure tenet.
- **Operators on the common case** (no RLS tables, or BYPASSRLS-equipped role) **see no observable change**.
- **No new flag.** Per the loud-failure tenet, no `--allow-rls-without-bypass` opt-out is added. The recovery is operator action (grant BYPASSRLS or `--exclude-table`), not a sluice-side bypass. RLS is a security contract; sluice's job is to honor it, not work around it.

## What this does NOT do (deferred to v0.79.0 + ADR-0058)

Three sub-deliverables of task #52 ship in stages:

1. **v0.78.4 (this release)** — defensive preflight refuse-loudly + diagnose bundle surface.
2. **v0.79.0 (planned)** — `ir.Policy` field on `ir.Table`; PG schema reader captures `pg_policies` into IR; PG schema writer emits `CREATE POLICY` + `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` on target so RLS-as-schema-contract survives the migration. ADR-0058 will document the policy-translation semantics.
3. **v0.79.0 (planned)** — full Bug-74-style integration matrix: {RLS off, RLS on, FORCE-RLS} × {DEFAULT role, BYPASSRLS role} × {snapshot, CDC apply} on both source and target sides.

The v0.78.4 scope closes the *worst* silent-loss path (the source-filter case the PlanetScale blog flags) without conflating it with the policy-translation feature. Operators today can grant BYPASSRLS to the sluice role and proceed; the schema-fidelity gap (policies not copied to target) becomes loud once an operator hits it, because the target's `pg_policies` view will show no policies whereas the source had them.

## Who needs this

- **PG operators with multi-tenant RLS-segregated tables** (the audience PlanetScale's blog speaks to). Most likely to have hit the silent-filter mode and not realized it.
- **Operators migrating from SQL Server / Oracle to PG** with tenant-scoped views — even if the source doesn't have RLS, the target schema design often introduces RLS for compliance, and sluice's writer connection now refuses gracefully if mis-configured.
- **Anyone NOT using RLS** sees no change.

## The loud-failure tenet at work

This release is a clean example of the loud-failure tenet from CLAUDE.md: when a misconfiguration could silently corrupt data (here: filter rows out of the snapshot), the right answer is to refuse with operator-actionable specifics, not proceed with a bypass flag. The cost is one extra step for operators with RLS (grant BYPASSRLS); the win is that operators who didn't realize their schema had RLS will find out at preflight time, not after the data is silently truncated on the target.

## Cross-references

- [PlanetScale: "RLS sounds great until it isn't"](https://planetscale.com/blog/rls-sounds-great-until-it-isnt) — the operator-pain blog that triggered this task
- [v0.78.3 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.78.3) — Bug 88 hotfix (the prior release)
- Task #52 (PG RLS handling, full scope) — three sub-deliverables; this is sub-deliverable 1
- CLAUDE.md § *Tenets* (loud-failure) — this release's design rationale

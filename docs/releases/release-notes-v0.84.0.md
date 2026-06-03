# sluice v0.84.0 — PG Row-Level Security IR capture + emit (ADR-0063)

**Headline:** Minor release closing the **target-arrives-without-policies** silent-security-regression class. Pre-v0.84.0, a PG → PG migration through sluice would translate the schema, copy rows, and apply CDC — but `pg_policies` entries on the source were never captured into the IR, so the target landed wide-open where the source had been multi-tenant-segregated by RLS. v0.84.0 reads `pg_policies` into the IR and re-emits `ALTER TABLE … ENABLE ROW LEVEL SECURITY` + `CREATE POLICY` on the target in load-bearing order. This was the PlanetScale RLS blog's lead concern; v0.78.4 had already shipped the preflight (sub-deliverable 1 — refuse loudly when reader/writer roles can't see policies); v0.84.0 lands sub-deliverables 2 and 3 to complete the failure-mode-3 fix.

Also in this release: a curated docs+marketing bundle (`docs/use-cases.md`, `docs/cutover.md`, `docs/comparison.md`, copywriting-guardrails), the README rewrite around HVR-class positioning, and CI-hardening across the engines/mysql + pipeline integration test infrastructure.

## Added

- **`feat(engines/postgres,engines/mysql,ir): task #52 sub-deliverables 2 + 3 — RLS IR capture + emit (ADR-0063)`**

  ### IR additions

  - `ir.Table.RLSEnabled` (`bool`) and `ir.Table.RLSForced` (`bool`) capture `pg_class.relrowsecurity` / `relforcerowsecurity`.
  - `ir.Table.Policies []*ir.Policy` carries the per-table policy list.
  - `ir.Policy{Name, Command, Permissive, Roles, Using, Check}` mirrors the `pg_policies` columns. `Command` is `ALL`, `SELECT`, `INSERT`, `UPDATE`, or `DELETE`. `Roles` is a `[]string` of role names (or `{"public"}` if the policy is unscoped).

  ### PG SchemaReader

  - New `populateRLS` step joins against `pg_policies` for each table and reads the two `pg_class` flags.
  - Roles are rendered server-side as `array_to_json(roles::text[])` and parsed in Go to avoid a `pgtype` dependency in the schema-reader's hot path.
  - Tables without policies get `Policies = nil` (not empty slice) so the IR-equality semantics stay consistent with the pre-RLS world.

  ### PG SchemaWriter

  - New `internal/engines/postgres/rls_emit.go` produces `ALTER TABLE … ENABLE ROW LEVEL SECURITY` (+ `FORCE` if source had it), then `CREATE POLICY <name> ON <table> [AS PERMISSIVE|RESTRICTIVE] FOR <cmd> TO <roles> USING (...) WITH CHECK (...);` per policy.
  - Emit order is load-bearing: `CREATE TABLE` → `ENABLE/FORCE` → `CREATE POLICY`. Without `ENABLE` the policies are defined but inert (no error, but a subtle silent-policy-not-enforced bug).
  - Wired into both `CreateTablesWithoutConstraints` (the migrate-time path) and `PreviewDDL` (the dry-run path).

  ### Cross-engine semantics

  - **PG → MySQL**: `maybeWarnRLSDrop` in `internal/engines/mysql/schema_writer.go` logs exactly **one** WARN per stream (`sync.Once`-gated) naming every affected table when incoming IR carries RLS state. MySQL has no RLS surface — operators routing PG → MySQL accept the policy-layer drop, but they see the warning so it's not silent.
  - **MySQL → PG**: no-op by construction. MySQL sources never populate the new IR fields.
  - **PG → PG**: full round-trip — source policies land on target verbatim.

## Architecture

- IR shape: `internal/ir/schema.go` — backwards-compatible additions (existing callers see new fields as zero-values).
- Reader: `internal/engines/postgres/schema_reader.go` — `populateRLS` is invoked from `ReadSchema` after the existing schema-shape population.
- Writer: `internal/engines/postgres/rls_emit.go` (new) + `schema_writer.go` phase 1d (wired in).
- Cross-engine WARN: `internal/engines/mysql/schema_writer.go` `maybeWarnRLSDrop` (`sync.Once`).

## Tests

- **Unit tests** (`rls_emit_test.go`, `rls_warn_test.go`, `schema_test.go`): full Command × Permissive × USING/CHECK × ENABLE/FORCE × roles cell coverage. Bug-74 discipline (pin the class, not the representative — verified across every shape variant).
- **Integration tests** (`rls_ir_emit_integration_test.go`, `rls_warn_integration_test.go`): real PG 16 round-trip of 6 policies × 2 tables (+ 1 RLS-disabled control). MySQL writer end-to-end fires exactly one WARN regardless of how many invocations / how many tables hit it.
- **`PreviewDDL` emit-order verified**: the dry-run path emits ENABLE before CREATE POLICY, same as the live path.

## Added — supporting

- **`docs(task #50): use-cases.md + cutover.md + comparison.md + copywriting-guardrails (F20/F21/F15/F22)`** — Marketing/positioning bundle. `docs/use-cases.md` names the four concrete operator scenarios (managed-PG upgrades, cross-cloud migration, MySQL ↔ PG consolidation, logical-CDC backups). `docs/cutover.md` is the operator-oriented companion to ADR-0062 — when to run `sluice cutover`, the refuse-loudly classes, the procedural rollback shapes. `docs/comparison.md` is the longer-form per-row companion to the README's headline matrix. `docs/dev/copywriting-guardrails.md` is the F22 mandate: claim only what the loud-failure machinery enforces.

- **`docs(readme): rewrite around HVR-class positioning + Heroku scope statement (F50)`** — README rewrites the hero section around "Open-source HVR-class CDC for MySQL ↔ Postgres" with concrete pricing wedge. Adds explicit "When NOT to use sluice" section calling out Heroku Postgres, one-off snapshots, logical-decoding-to-applications, and schema-migration tooling as non-fits.

## Fixed — CI hardening

- **`test(engines/mysql): task #60 — retry-with-backoff on shared TestMain boot`** + **`test(pipeline): task #63 — retry-with-backoff on per-test MySQL boots`** + **`test(engines/mysql,pipeline): task #12 — bump retry attempts 3 → 5 + wrap GTID per-test boot`** — Three CI-hardening landings address the MySQL container-boot wait-until-ready flake class that cost 3-5 release-cycle reruns historically.

  ### Pattern

  - 5 boot attempts with 30s / 60s / 120s / 240s backoff between attempts. Worst-case wall time per helper: ~17.5min, under the CI 30-minute shard timeout.
  - Applied at three boot sites: engines/mysql shared TestMain (Option B fixture), engines/mysql `startMySQLGTIDForCDC` (per-test special-case that bypasses shared TestMain by design), pipeline `runMySQLWithRetry` wrapper covering all 7 `startMySQL*` helpers.

  ### Coverage

  PR #54 (task #60) + PR #59 (task #63) + PR #62 (task #12). The session's CI instrumentation captured two cases where 3 attempts wasn't enough under runner load — bumping to 5 buys the tail.

## Compatibility

- **Drop-in upgrade from v0.83.0.** New IR fields are additive; out-of-tree IR consumers (custom engine implementations) see the new fields as zero-values and behave identically to v0.83.0.
- **Behavior change to flag**: PG → PG migrations that touch RLS-enabled tables now emit `ENABLE ROW LEVEL SECURITY` + `CREATE POLICY` on target. **This is the documented fix for failure mode 3** — operators relying on the prior wide-open-target behavior (none, presumably) would see policy enforcement on target post-cutover. The preflight (v0.78.4) and the IR emit (v0.84.0) together make the policy-layer transit silent-free.
- **Behavior change to flag**: PG → MySQL migrations against RLS-enabled sources now log a single WARN naming affected tables. No functional change — the policy layer was always being dropped silently; v0.84.0 makes the drop loud.
- **Minor version bump (v0.84.0)** — new IR fields + new emit behavior + new WARN log line.
- **Severity a** — closes the silent-security-regression class. PG operators migrating multi-tenant schemas with row-level policies should upgrade before their next cutover.

## Who needs this

- **PG operators with RLS-enabled tables** — the canonical v0.84.0 audience. Run `sluice migrate` against a v0.84.0 binary and target schema will carry the policies, not arrive blank.
- **Cross-engine PG → MySQL operators with RLS sources** — see the one-time WARN and decide whether the destination-MySQL system needs application-layer tenant scoping to compensate.
- **MySQL → PG operators** — no observable change.
- **Operators chasing the CI-hardening fixes** — the engines/mysql and pipeline-package boot-flake reruns are quieter on v0.84.0.

## Cross-references

- [ADR-0063 — PG Row-Level Security IR capture + emit](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0063-pg-rls-ir-capture-and-emit.md)
- [docs/use-cases.md — operator scenarios](https://github.com/sluicesync/sluice/blob/main/docs/use-cases.md)
- [docs/cutover.md — `sluice cutover` operator guide](https://github.com/sluicesync/sluice/blob/main/docs/cutover.md)
- [docs/comparison.md — per-row deep dive vs. alternatives](https://github.com/sluicesync/sluice/blob/main/docs/comparison.md)
- [PlanetScale RLS blog](https://planetscale.com/blog/rls-sounds-great-until-it-isnt) — motivation
- Task #52 description in the project backlog — full enumeration of the 5 RLS failure modes; sub-deliverables 2 + 3 in this release close failure mode 3

# ADR-0063 — PG row-level-security IR capture + emit (task #52 sub-deliverables 2 + 3)

* Status: Accepted (2026-05-25)
* Severity: A (silent-security-regression class on PG → PG)
* Task: #52 sub-deliverables 2 + 3 (sub-deliverable 1 shipped in v0.78.4)
* Siblings:
  * [ADR-0057 — hard-delete semantics across engines](adr-0057-hard-delete-semantics-across-engines.md) — adjacent silent-loss-class ADR
  * [ADR-0053 — exclude-constraint verbatim carry](adr-0053-exclude-constraint-verbatim-carry.md) — same IR-capture pattern for a PG-only feature
  * [ADR-0062 — cutover sequence priming](adr-0062-cutover-sequence-priming.md) — adjacent in the cross-engine schema-completeness arc

## Context

Task #52 enumerated five failure modes Postgres row-level security introduces when sluice is run against an RLS-bearing schema:

1. Source snapshot is silently RLS-filtered (silent data loss) — closed by sub-deliverable 1 (RLS preflight, ADR text in the preflight files; shipped v0.78.4).
2. Target INSERTs blocked by `WITH CHECK` (loud but opaque) — same preflight closes this.
3. **Target schema arrives without policies (silent security regression).** A PG → PG migration where the source has `CREATE POLICY` rows lands the target with an identical column shape and zero per-tenant filters — every row visible to every role. From the operator's perspective, the migration "succeeded" but the multi-tenant security boundary the source enforced no longer exists on the target. **This ADR closes failure mode 3.**
4. FORCE RLS on source (owner-bypass surprise) — surface covered by sub-deliverable 1's reporting; emit covered here.
5. CDC carries rows snapshot dropped — out of scope for sub-deliverable 1/2/3 (a separate per-row CDC-side concern, future work).

PlanetScale's "RLS sounds great until it isn't" blog post (2024) flagged failure mode 3 as the operational concern that most surprises operators migrating multi-tenant Postgres workloads. The shape is the worst kind of silent failure: nothing fails, nothing logs, and the only signal is the operator's later audit ("why can `tenant_b` read `tenant_a`'s rows?"). The CLAUDE.md "zero users is the current reality" tenet calls this exact shape out as load-bearing: the first silent corruption ends the project's credibility permanently.

Sluice's RLS preflight (sub-deliverable 1) refuses early when the *connecting role* lacks BYPASSRLS, preventing failure modes 1 and 2. But a properly-configured BYPASSRLS-bearing role passes the preflight cleanly — and then runs into failure mode 3 because the schema reader / writer had no representation for RLS at all.

## Decision

Capture `pg_class.relrowsecurity` / `relforcerowsecurity` and `pg_policies` rows into the IR, and emit them back as `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` (+ `FORCE`) and `CREATE POLICY` on the target.

### IR shape

Three new fields on `ir.Table`:

```go
type Table struct {
    // ... existing fields ...
    RLSEnabled bool      // pg_class.relrowsecurity
    RLSForced  bool      // pg_class.relforcerowsecurity
    Policies   []*Policy // pg_policies rows
}

type Policy struct {
    Name       string   // pg_policies.policyname
    Command    string   // "ALL" / "SELECT" / "INSERT" / "UPDATE" / "DELETE"
    Permissive bool     // pg_policies.permissive == "PERMISSIVE"
    Roles      []string // pg_policies.roles (rendered through array_to_json)
    Using      string   // pg_policies.qual (USING expression text, PG-canonical)
    Check      string   // pg_policies.with_check (WITH CHECK text)
}
```

Each `Policy` field maps directly to a `pg_policies` column. `Permissive bool` is the only non-trivial mapping — PG renders "PERMISSIVE" / "RESTRICTIVE" as text; bool is the more compact IR shape and the writer renders the keyword back on emit.

### Read side

`PG SchemaReader.populateRLS` runs as a new step in the existing read-phases pipeline (between `populateExcludeConstraints` and `populateComments`). Two cheap catalog scans:

1. One query against `pg_class` for `relrowsecurity` / `relforcerowsecurity` (table-level flags).
2. One query against the `pg_policies` view (joins `pg_policy` with role catalog so we get human-readable role names; `qual` and `with_check` come back as `pg_get_expr` canonical PG-dialect text).

The `roles` column is `text[]` in `pg_policies`. Rather than introduce a `pgtype.Array` driver-side dependency for what is ultimately a small list of role names, the query renders `array_to_json(roles)::text` and the Go side parses it with `encoding/json` (`decodeRoleArray`). The "fewer pgx-codec moving parts" trade-off mirrors the existing reader's preference for catalog-side casts (`pg_get_expr`, `pg_get_constraintdef`, `format_type`) over driver-side type machinery.

### Write side

`PG SchemaWriter.CreateTablesWithoutConstraints` gains a new phase 1d (after phase 1c COMMENT ON statements):

1. For each table with `RLSEnabled` true: `ALTER TABLE <qualified> ENABLE ROW LEVEL SECURITY;`
2. For each table with `RLSForced` true: `ALTER TABLE <qualified> FORCE ROW LEVEL SECURITY;`
3. For each policy in `table.Policies`: `CREATE POLICY <name> ON <qualified> [AS RESTRICTIVE] FOR <cmd> TO <roles> [USING (...)] [WITH CHECK (...)];`

**The order is load-bearing.** `CREATE POLICY` against a table whose RLS is OFF defines the policy in `pg_policy` but never enforces it — exactly the silent-security-regression shape failure mode 3 exists for. The writer always emits ENABLE before any CREATE POLICY for the same table.

The defensive emit branch — Policies populated but `RLSEnabled` false (a hand-built shape the reader never produces) — also emits the ENABLE so the policy isn't inert. Documented in `emitRLSStatements`'s comment block and pinned with a unit test.

### Cross-engine semantics

* **PG → MySQL.** MySQL has no RLS surface. The MySQL `SchemaWriter` detects any RLS-bearing table in the incoming IR (`RLSEnabled || RLSForced || len(Policies) > 0`) and logs exactly one WARN per writer lifetime (one per stream) naming the affected tables. Rationale: per-table WARNs would flood logs for any non-trivial multi-tenant schema; the operator-visible fact is "policies are dropped on this run," not "policies are dropped on table X." The WARN is gated by `sync.Once` on the writer.
* **MySQL → PG.** MySQL sources never populate the new IR fields. PG writer's RLS-emit branch short-circuits on `!RLSEnabled && !RLSForced && len(Policies) == 0`. No-op, no warning, by construction.
* **PG → PG.** The full-faithfulness path. ENABLE / FORCE / every CREATE POLICY round-trips.

### `sluice schema preview` integration

`PreviewDDL` mirrors the apply path: phase 1d emits an `ir.DDLStatement` for each RLS row with one of three Kind tags: `ALTER TABLE ENABLE RLS`, `ALTER TABLE FORCE RLS`, `CREATE POLICY`. Operators running `sluice schema preview` see every policy that will land before any DDL fires.

## Trade-offs and alternatives

### Why capture into IR vs. side-channel

A side-channel pass that reads `pg_policies` and applies it as a post-step would couple the orchestrator to PG-specific catalog state, violating the IR-first tenet ("source-specific knowledge lives in readers; target-specific knowledge lives in writers; the IR is the only shared contract"). Capturing into IR keeps every cross-engine consumer — diff, preview, chain restore, the cross-engine refusal in `checkCrossEngineSupportable` — uniformly aware of the policy layer.

### Why a per-table `Policies` slice rather than a top-level `Schema.Policies`

Policies attach to a single table by PG's catalog model (`pg_policy.polrelid`). A top-level slice would need a per-row `Table` field, which is redundant indexing. Per-table `Policies` is also what other PG-only IR fields use (`ExcludeConstraints`).

### Why bool for `Permissive` instead of an enum

Two values, no anticipated third. PG's grammar is `AS PERMISSIVE | AS RESTRICTIVE`; an enum would carry the same bit. Bool keeps the IR compact and reads naturally at call sites.

### Why no role-creation on emit

Operators running PG → PG with non-default roles in `pg_policies.roles` must ensure those roles exist on the target before the schema emit fires. `CREATE POLICY ... TO role_name` against a missing role raises `42704 role "role_name" does not exist`. Sluice does not create roles — role-management lives outside the migration tool's scope (operators manage roles via `ALTER ROLE`, IaC, or their managed-PG console). Documented in the ADR and in the operator-facing schema-preview output: `CREATE POLICY ... TO <role>` rows let the operator preview which roles need to exist.

### Why the WARN-once, not per-table

Per-table WARNs in a 50-table multi-tenant schema flood logs and bury the signal. One WARN per writer lifetime naming all affected tables conveys the same operator-actionable information ("the policy layer was dropped on N tables on this run") without log noise. The CLAUDE.md "Clean, elegant code" tenet prefers the higher-density signal.

### Why no `--strip-policies` opt-out flag

The current contract is "always carry policies when both sides are PG, always WARN-drop when source is PG and target is MySQL." A `--strip-policies` flag would be a third behaviour ("PG → PG migration that drops policies") with no operator-actionable use case the existing primitives don't already cover (operators wanting to ship a target without policies can use `--exclude-table` on the policy-bearing tables, or omit the per-table grant). YAGNI — wait for an operator request, refuse silent-strip behaviour by default.

## Failure modes the emit can't fix

* **Missing roles on target.** `CREATE POLICY ... TO role` against a missing role fails at apply time with PG SQLSTATE 42704. The error is loud (CREATE TABLE phase aborts; the operator sees the role name in the error). Sluice does not pre-flight role existence on the target — operator-managed.
* **USING/CHECK expressions referencing source-only functions.** If a policy's USING expression calls an extension function (`uuid_generate_v4()` from uuid-ossp) and the operator didn't `--enable-pg-extension uuid-ossp`, the CREATE POLICY fails with SQLSTATE 42883. ADR-0044's Tier-3 schema-read gate covers DEFAULT and GENERATED expressions but does NOT yet cover policy expressions. Documented; out of scope for sub-deliverables 2 + 3, future work for a follow-up if the operator-request shows up.
* **RLS in transactional DDL surprise.** PG enforces ENABLE / FORCE / POLICY in a single transaction with CREATE TABLE; sluice's existing CREATE TABLE → COMMENT ON → RLS phase ordering doesn't transactionally bundle them. A partial-failure mid-phase leaves the target with tables but missing policies. The CLAUDE.md "loud-failure" tenet covers this: the next apply attempt re-runs the RLS phase (PG's `ALTER TABLE ... ENABLE` is idempotent, `CREATE POLICY` is not — duplicate-name error is loud and operator-actionable). Wrapping into a single transaction would mask other partial-failure shapes; the current shape is consistent with how `populateComments` etc. work.

## Test strategy — Bug 74 class-pin discipline

The reader and writer both dispatch on Command via a string literal. A test pin on "SELECT" alone would not cover "INSERT" / "UPDATE" / "DELETE" / "ALL" — they ride different catalog paths and different USING/CHECK invariants. Per the CLAUDE.md "Pin the class, not the representative" rule, the test matrix exercises:

* **Command axis**: ALL, SELECT, INSERT, UPDATE, DELETE — five rows in the unit-test and integration matrices.
* **Permissive axis**: PERMISSIVE (default), RESTRICTIVE.
* **USING/CHECK shape axis**:
  * USING only (SELECT / DELETE policies)
  * WITH CHECK only (INSERT policies)
  * USING + WITH CHECK both (UPDATE / ALL policies)
* **Table flags axis**: ENABLE only, ENABLE + FORCE, neither.

The full cross-product is 5 × 2 × 3 × 3 = 90 cells — too many for an explicit matrix, so the unit tests cover the Command × USING/CHECK × Permissive × Flags cells with representative coverage at each axis, and the **integration test exercises the matrix end-to-end** on a real PG container (one policy per cell in the source DDL fixture, then re-queries `pg_policies` on the target to confirm every cell round-tripped).

The Bug 74 anti-pattern: the v0.69.3 array fix tested one element family (`int[][]`), passed CI, and `numeric[][]` silently flattened in production. The RLS-IR analogue would be: test one Command (`SELECT`), pass CI, and `UPDATE`'s combined USING + WITH CHECK silently emits a malformed CREATE POLICY in production. The integration test below exists to make that failure mode visible at CI time.

## Implementation files

* `internal/ir/schema.go` — `RLSEnabled`, `RLSForced`, `Policies` on `Table`; `Policy` type.
* `internal/engines/postgres/schema_reader.go` — `populateRLS` + helpers.
* `internal/engines/postgres/rls_emit.go` — `emitRLSStatements`, `emitCreatePolicy`, `formatPolicyRoles`, `rlsStatementKind`.
* `internal/engines/postgres/schema_writer.go` — phase 1d wiring in `CreateTablesWithoutConstraints` + `PreviewDDL`.
* `internal/engines/mysql/schema_writer.go` — `maybeWarnRLSDrop` + `sync.Once` gate on `SchemaWriter`.

## Tests

* **Unit tests** (`internal/ir/schema_test.go`, `internal/engines/postgres/rls_emit_test.go`, `internal/engines/mysql/rls_warn_test.go`) — Policy round-trip, every emit cell (Command × Permissive × USING/CHECK), WARN-once gate, MySQL-source no-op, defensive empty-Roles / empty-Command fallbacks.
* **Integration tests** (`internal/engines/postgres/rls_ir_emit_integration_test.go`, `internal/engines/mysql/rls_warn_integration_test.go`) — real PG round-trip exercising the matrix; real MySQL writer firing exactly one WARN under PG-shape IR; PreviewDDL surfacing every RLS row in order.

## Roll-out

Patch release. Operators on PG → PG see policies round-trip with no flag changes; operators on PG → MySQL see a new WARN line on first apply; the preflight (sub-deliverable 1, already shipped in v0.78.4) remains in place. CHANGELOG entry calls out the new behaviour under both directions.

## Out of scope

* **Policy emit translation to MySQL-side RLS-like primitives.** MySQL doesn't have row-level security; the closest analogue (`DEFINER` + `SECURITY DEFINER` views) is a different shape with different semantics. Sluice doesn't synthesize one from a PG policy — operators routing PG → MySQL accept the policy-layer drop or stay on PG.
* **CDC-driven policy mutation replay.** CDC events for `pg_catalog` changes (a new policy added on the source mid-stream) are out of scope. Operators redo the schema phase to pick up post-cold-start policy changes.
* **Role provisioning on target.** Sluice does not create or grant on the target; operators manage roles independently.
* **Failure mode 5 (CDC carries rows snapshot dropped).** Per-row CDC concern, not schema-shape concern; future work in a separate ADR.

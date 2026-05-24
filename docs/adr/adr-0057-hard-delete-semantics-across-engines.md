# ADR-0057 — Hard-delete semantics across engines

## Status

Accepted (2026-05-24). Documents an **existing** structural property
of sluice plus the regression-guard test matrix that pins it. No new
production code; this ADR exists to make the policy explicit and to
record the family-pin discipline that protects it from the next
Bug-74-shape codec regression.

Structural property pinned by:
- `internal/engines/mysql/cdc_reader.go:708-724` — binlog reader
  emits `ir.Delete` for `DELETE_ROWS_EVENTv{0,1,2}`.
- `internal/engines/postgres/cdc_reader.go:725-730` — pgoutput reader
  emits `ir.Delete` for both `DeleteMessageV2` and `DeleteMessage`.
- `internal/engines/postgres/cdc_reader.go:988-1032` — `emitDelete`
  refuses loudly on `REPLICA IDENTITY NOTHING` (operator-actionable
  error rather than silent drop).
- `internal/engines/postgres/change_applier.go:1104-1124` /
  `internal/engines/mysql/change_applier.go:929-941` — appliers
  execute `DELETE FROM … WHERE …` against the target.
- `internal/ir/change.go:60-123` — `ir.Delete` is a first-class
  sealed-interface member, not a soft-delete tombstone.

Regression matrix pinned by:
- `internal/pipeline/cdc_delete_matrix_mysql_integration_test.go`
- `internal/pipeline/cdc_delete_matrix_pg_integration_test.go`

## Context

Hard-delete handling is the most-cited operator pain in the F18
Reddit-research dataset (`C:\code\sluice-reddit-research-2026-05-23.md`,
finding F18; triage in
`.claude/worktrees/agent-792c52fcfe42ce6a/TASK40-TRIAGE.md`). Three
independent operator quotes across Fivetran (`u/sloth_king_617`),
Airbyte (`u/minormisgnomer`), and AWS DMS / Estuary describe
competitor CDC tools silently dropping hard deletes — same class as
the v1 `binlog_row_image` hazard but at the application layer. The
silent-drop variant is the worst possible failure mode: the operator
doesn't notice until reconciliation runs (if it runs at all), and by
then the target has drifted.

sluice's IR-first architecture answers this structurally: hard deletes
on the source produce `ir.Delete` events that flow through the
pipeline to the applier, which executes `DELETE FROM` on the target.
There is no silent-drop branch and no tombstone-soft-delete fallback.
The "always emit DELETE" property is the design tenet that lets
sluice make the trust claim competitors cannot.

That property is, however, **family-dispatched**: the wire format of
the BEFORE-image (MySQL binlog) and the OLD tuple (Postgres pgoutput)
varies by source-side configuration. A regression that passed under
one configuration could silently break another. This is exactly the
Bug-74 trap that cost sluice a release banner — a representative
test ("DELETE under `binlog_row_image=FULL` works") would not catch a
regression that only manifests under `MINIMAL` or `NOBLOB`.

The Bug-74 lesson (`CLAUDE.md` § *Pin the class, not the
representative*) applies: pin the **matrix** of family cells, not one
representative cell. For hard deletes the family dispatch is:

- **MySQL** `binlog_row_image` ∈ {`FULL`, `MINIMAL`, `NOBLOB`}
- **Postgres** `REPLICA IDENTITY` ∈ {`DEFAULT`, `FULL`, `USING INDEX`,
  `NOTHING`}

(NOTHING is the explicit refuse-loudly cell — covered separately,
see below.)

## Decision

### 1. Always-emit-DELETE is policy, not heuristic

sluice's CDC readers MUST emit an `ir.Delete` for every source-side
DELETE event they observe, **except** when the source configuration
makes the row unidentifiable (Postgres `REPLICA IDENTITY NOTHING`).
In that one case the reader refuses loudly with an operator-actionable
error naming the misconfiguration. Silent drop is not an option in any
configuration.

The appliers MUST execute `DELETE FROM … WHERE <identity-keys>`
against the target for every `ir.Delete` they receive. Rows-affected
≠ 1 is logged per ADR-0010's idempotency contract; the position
advances. (Silent zero-match is the structural bug Bug 8 fixed via
`filterDeleteBefore` for the composite-PK case; that fix's behaviour
is part of the matrix this ADR pins.)

### 2. The family-matrix pin

The regression-guard pin exercises the cross-product of source-side
configuration × delete-shape:

#### MySQL source (same-engine MySQL→MySQL)

| `binlog_row_image` | plain DELETE | UPDATE-then-DELETE | TOAST'd row DELETE |
|---|:---:|:---:|:---:|
| `FULL`    | yes | yes | n/a  |
| `MINIMAL` | **skip (unsupported, finding)** | **skip (unsupported, finding)** | n/a  |
| `NOBLOB`  | yes (no BLOB col present) | yes (no BLOB col present) | **skip (unsupported, finding)** |

`NOBLOB` × TOAST'd is the load-bearing cell: the BEFORE-image lacks
BLOB columns by definition. The pin would assert that a 100KB
`MEDIUMTEXT` column being absent from the BEFORE-image does not
prevent the DELETE from propagating.

**Task #44 finding — sluice requires `binlog_row_image=FULL`.** Under
`MINIMAL` (and under `NOBLOB` when a BLOB/TEXT non-PK column exists),
the binlog DELETE event carries `nil` for non-PK / BLOB columns. The
applier's `buildWhereClause`
(`internal/engines/mysql/change_applier.go:1240-1248`) emits `col IS
NULL` predicates for those entries; the DELETE matches zero rows on
the target, ADR-0010 idempotency absorbs the miss, and the position
advances — Bug-8-equivalent silent data loss. This is documented in
`docs/dev/notes/prep-change-applier.md:26` as an unstated assumption;
this ADR makes it explicit and lists the three affected matrix cells
as `t.Skip()`-with-reason placeholders. **Fix is out of scope for
Task #44** (test-only chunk) and should be tracked as a follow-up
backlog item — most likely shape: have the MySQL CDC reader's DELETE
emit narrow the Before image to identity-key columns (the same
pattern PG already uses via `filterDeleteBefore`).

The skipped cells remain in the matrix file so when the production
fix lands, removing the skip is the contract — no matrix-discovery
work required.

##### Bug 88 closure (v0.78.3, 2026-05-24)

The Task #44 finding is fixed. The MySQL CDC reader now narrows the
DELETE Before-image to primary-key columns before emit, via a
`filterDeleteBefore` helper mirroring the PG-side helper of the
same name. The fix locus is the binlog reader's `DELETE_ROWS_EVENT`
emit path (`internal/engines/mysql/cdc_reader.go`), not the
applier — same shape as PG's locus, per the Bug-74 family-pin
discipline ("fix the class, not the instance"; here the class is
the wire-format reader's responsibility for delivering a usable
identity, not the applier's responsibility for emitting an
identity-friendly WHERE).

The four previously-`t.Skip()`'d matrix cells are now live and
pass:

- `MINIMAL` × `plain-delete`
- `MINIMAL` × `update-then-delete`
- `NOBLOB` × `toast-delete` (the load-bearing cell — BLOB column
  absent from BEFORE-image, non-BLOB non-PK column `name` nil)
- (The previously-passing `NOBLOB` × `plain-delete` /
  `update-then-delete` cells continue to pass.)

A unit-level pin (`TestFilterDeleteBefore` in
`internal/engines/mysql/cdc_reader_test.go`) covers the helper's
narrowing behaviour across MINIMAL / FULL / NOBLOB / composite-PK
/ PK-less shapes so regressions surface cheaply, without spinning
up a testcontainer.

**Scope note: VStream (PlanetScale) is out of scope for this fix.**
The Bug 88 fix targets the binlog reader (`cdc_reader.go`); the
VStream reader (`cdc_vstream.go`) has its own DELETE emit path
that currently passes the full Before-image through unchanged. If
PlanetScale ever exposes a `binlog_row_image=MINIMAL`-equivalent
on the VStream wire format, the same narrowing would need to land
there too. Pinned by the existing
`cdc_vstream_composite_pk_integration_test.go` matrix expanding
when needed.

#### Postgres source (same-engine PG→PG)

| `REPLICA IDENTITY` | plain DELETE | UPDATE-then-DELETE | TOAST'd row DELETE |
|---|:---:|:---:|:---:|
| `DEFAULT`     | yes | yes | yes |
| `FULL`        | yes | yes | yes |
| `USING INDEX` | yes | yes | yes |
| `NOTHING`     | refuses loudly (see §3) | refuses loudly | refuses loudly |

All three replicable settings are pinned for every shape because the
OLD-tuple content differs across all three — DEFAULT carries the
primary-key only, FULL carries every column, USING INDEX carries the
columns of the named index. A regression that drifted the apply-side
WHERE construction to depend on a particular OLD-tuple shape would
fail one or two cells while leaving the third green.

#### Cross-engine sanity

One representative cell per direction:

- MySQL→Postgres with `binlog_row_image=FULL` × plain DELETE
- Postgres→MySQL with `REPLICA IDENTITY FULL` × plain DELETE

Full matrix on cross-engine is overkill (testcontainer-pairs are
slower); the same-engine matrix exercises the family dispatch, and
the cross-engine sanity verifies the apply path across the engine
boundary still propagates DELETE.

### 3. Refuse-loudly on REPLICA IDENTITY NOTHING

`emitDelete`
(`internal/engines/postgres/cdc_reader.go:988-1013`) refuses with an
operator-actionable error when the OldTuple is `nil`:

> postgres: cdc: delete on `<schema>.<table>` without identity:
> relation has REPLICA IDENTITY NOTHING; configure REPLICA IDENTITY
> DEFAULT, FULL, or USING INDEX before replicating DELETEs

Unit-level coverage exists at
`TestSynthesizeKeyOnlyBeforeRejectsReplicaIdentityNothing`
(`internal/engines/postgres/cdc_reader_test.go:539-555`) for the
UPDATE path. The matrix does not add an integration cell for NOTHING
because the refusal is the explicit policy, not a per-cell behaviour
to verify — it would just confirm "the error is raised" which the
unit test already covers.

### 4. The matrix exists to prevent Bug-74-shape regressions

The matrix's job is to fail loudly when a future change to the
delete-emit or delete-apply path silently breaks the always-propagate
property in one configuration while leaving others green. This is
the exact failure mode that cost sluice a public correction banner
in v0.69.3: the array-element fix passed `int[][]`/`text[][]` cells
but silently flattened `numeric[][]` because pgx dispatched to a
different target-OID codec. The reviewer corollary applies — a
reviewer looking at a delete-path change should re-derive the
matrix above and verify the pin still covers it.

## Alternatives considered

**Tombstone-only emit (soft-delete with `__deleted_at` column).** Some
competitor tools (Airbyte, optionally Fivetran) default to this:
DELETEs propagate as UPDATEs to a tombstone column, and operators are
responsible for downstream cleanup. Rejected because (a) it changes
the target schema in a non-reversible way the operator did not ask
for, (b) it amplifies storage costs on tables with high DELETE
churn, and (c) it gives operators a footgun: the tombstone is invisible
to existing target queries until they remember to add a WHERE clause.
sluice's audience wants the database the way the source has it, not
a derived projection.

**Best-effort delete + reconciliation pass.** Some homegrown tools
emit DELETE but rely on a periodic checksum pass to catch silent
drops. Rejected because the reconciliation pass is the operator pain
the F18 finding documents — operators are already paying for it on
top of their CDC tool, and "reconcile to find silent drops" is the
exact workflow sluice's structural-correctness approach exists to
make unnecessary.

**Skip-on-unidentifiable + warn log.** An alternative to the
refuse-loudly policy for REPLICA IDENTITY NOTHING: emit a WARN log and
skip the DELETE. Rejected because skip-and-warn is functionally
silent drop — operators not watching the log feed will not notice.
Refuse-loudly halts the stream until the operator fixes the source,
which matches the "loud-failure discipline" tenet in CLAUDE.md.

## Consequences

- Hard-delete propagation is permanently part of sluice's positioning
  vs Fivetran/Airbyte/DMS. The `docs/comparison.md` work (F14/F15)
  can cite this ADR as the load-bearing reference.
- Any future change to the emit or apply path for DELETE must keep
  the matrix green. Reviewers re-derive the cell list above when
  approving such a change.
- New engines added to sluice MUST document their own variant of the
  source-side identity-configuration axis in this ADR (or a sibling)
  and add per-engine matrix cells. The IR contract carries `ir.Delete`
  uniformly; the family-dispatch lives in the wire-format readers.
- The matrix runs under the existing `integration` build tag and adds
  ~10 cells; expected runtime is bounded by testcontainer startup
  (each cell boots a fresh container). The cost is small relative
  to the silent-regression-prevention payoff (Bug 8 + Bug 74 history
  in CLAUDE.md).

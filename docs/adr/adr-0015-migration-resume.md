# ADR-0015: Resumable simple-mode migrations via per-target state table

## Status

Accepted. Implemented in v0.3.0 (`internal/pipeline/resume.go`,
`internal/engines/postgres/migration_state.go`,
`internal/engines/mysql/migration_state.go`).

Storage shape evolved by ADR-0082: the single-row `table_progress`
JSON blob became a header row + one `sluice_migrate_table_progress`
row per table (O(1) checkpoint writes; legacy rows upgrade on first
Read). The resume semantics this ADR defines are unchanged.

### Amendment — 2026-09-15 (v0.154.0): resume binds the SOURCE, not just the id

Audit 2026-09-15 A0915-STATE-MEDIUM-1. As specified below, resume is
**adoption by id**: `loadOrInitState` reads the recorded row whose
`migration_id` matches and continues it. A migration id is a *name*, not an
identity, so a `--resume` pointed at a **different source** that shares the id
read a copy it never performed as its own — and exited **0 having copied
nothing**. At phase `complete` it logged "already complete; nothing to do" and
stopped; mid-phase it skipped every table the other run had recorded complete.
Both arms were measured on real servers.

Two doors close it, and they are complementary rather than redundant:

- **The header row now records the source's identity, and a `--resume` refuses
  a live source that disagrees** — `SLUICE-E-RESUME-SOURCE-MISMATCH`, before
  any copy, printing the recorded and the live identity side by side. Identity
  is the source **engine + database + schema** (where the engine scopes by
  one). A new `source_identity` column carries it on both engines, added
  idempotently to control tables an older binary created. It is **set once**,
  at the header INSERT, and deliberately absent from every upsert's SET list:
  were it updated, a resume from a foreign source would overwrite the very
  evidence that refuses it, so the door would hold on the first re-run and open
  silently on the second. This is the door that catches the **no-flag** case
  this ADR documents as a known limitation — two databases on one host
  colliding on the auto-derived id, which hashes the *hosts* and deliberately
  not the database.
- **An operator-typed `--migration-id sync-<x>` is refused outright**, at id
  resolution in `resolveMigrationID` rather than on the kong flag, so a config
  file or a programmatic caller meets it too. That namespace holds the progress
  rows a `sync start` cold start records under its stream id; a `migrate
  --resume` given one reads that sync's finished copy as its own. This arm
  needs no recorded state, which is why it is separate from the identity door.

**The host is deliberately excluded**, and that exclusion is load-bearing
rather than an omission: this ADR frames `--migration-id` as the operator's
assertion of a stable identity across DSN changes, so a DNS move, a failover,
or a resume pointed at a replica of the same database must still resume.
Keying on the host would refuse precisely the case the flag exists to serve.
Neither host nor credentials ever enter the stored value.

State written by a sluice older than the column carries no identity. Such a
resume **proceeds** with a `RESUME-SOURCE-UNRECORDED` warning rather than being
refused — refusing would strand every migration in flight at the moment an
operator upgrades, which is the one thing `--resume` exists to prevent — and
that population can only shrink, since every header row this binary writes
records an identity and a row is never back-filled on resume.

## Context

Before this work, a `sluice migrate` run that failed partway through —
target ran out of disk, transient network blip, operator killed the
process — had to be re-run from scratch after dropping the entire
target. For multi-hour migrations of large tables that's a real
operational pain point: a network glitch ten hours into a fourteen-hour
copy throws away ten hours of work.

Two design axes had to be settled before code:

1. **Where to persist resume state.** Re-using the existing
   `sluice_cdc_state` table from the streamer was tempting (one less
   table, fewer columns to ALTER) but conflated two unrelated
   concepts: continuous-sync streams (long-lived, position-driven) and
   one-shot migrations (single-run, phase-driven). Different lifetimes,
   different recovery semantics.
2. **How granular to make the per-table progress.** The bulk-copy
   phase does most of the work. Per-batch checkpointing would let a
   resume pick up mid-table — but it adds significant complexity:
   per-batch state writes contend with the bulk-load throughput,
   COPY-protocol writes commit all-or-nothing so there is nothing
   useful to checkpoint mid-COPY, and the orchestrator needs to track
   row offsets across reader and writer goroutines.

## Decision

A new per-target `sluice_migrate_state` table parallel to
`sluice_cdc_state`. Schema (engine-neutral):

```
migration_id    TEXT PRIMARY KEY
phase           TEXT NOT NULL
table_progress  TEXT          -- JSON map: { "users": "complete", "orders": "in_progress" }
started_at      TIMESTAMP NOT NULL
updated_at      TIMESTAMP NOT NULL
last_error      TEXT          -- truncated to 1 KiB
```

Phase enum: `pending → tables → bulk_copy → identity_sync → indexes →
constraints → complete`. Failures persist the in-flight phase plus
the truncated error message; resume reads the row, branches by phase,
and re-enters at the recorded point.

Per-table granularity is **whole-table truncate-and-redo**: an
in-progress table is `TRUNCATE`d before re-copy. The trade-off is
deliberate — operators pay the cost of re-copying *one* in-progress
table, not the entire migration. Per-batch checkpointing shipped in
v0.4.x — see [ADR-0018](adr-0018-per-batch-bulk-copy-checkpointing.md) — and
extends the whole-table fallback with a PK-cursor resume that avoids
the truncate when the in-progress table's primary key is monotonic.

`MigrationStateStore` is wired as an *optional* engine surface
(`ir.MigrationStateStoreOpener`), mirroring the `SlotManagerOpener`
shape from ADR-0011. Engines that don't implement it can still be
used as targets — the orchestrator falls back to non-resumable
behaviour and `--resume` errors clearly. Both shipping engines
(MySQL, Postgres) implement it.

`TableTruncator` is a similarly-optional surface on `RowWriter`. Both
shipping row writers implement it via plain `TRUNCATE TABLE`. A
target without TRUNCATE support would refuse to resume an in-progress
table with a clear error.

## Consequences

**Win.** A failed migration can be re-run with `sluice migrate
--resume` and skips work already done. The default behaviour is
unchanged for fresh migrations; the resume flag is opt-in.

**The truncate-and-redo trade-off.** Re-copying one table is
acceptable for most workloads. Tables larger than the network can
re-pump in the operator's downtime window will hit this corner —
and the v1 answer is "split into smaller migrations or wait for the
checkpoint upgrade." A future PR can layer per-batch checkpointing
without changing the state-table shape (the JSON column was chosen
partly so this expansion is non-breaking).

**State-table-pollution avoidance.** The schema readers exclude
`sluice_cdc_state` and `sluice_migrate_state` from their result so
running sluice doesn't create a "your migration has an extra table"
surprise on subsequent re-migrations.

**Idempotency assumptions.** Schema phase 1 (`CREATE TABLE`) is now
emitted with `IF NOT EXISTS` on both engines, making re-runs cheap
no-ops. Phases 2 (indexes) and 3 (constraints) are not yet
idempotency-fortified — a CREATE INDEX with a clashing name would
fail. Resume from those phases is therefore best-effort: in practice
the failed phase is the latest work, so there is nothing pre-existing
to clash with. A future PR can pre-query catalog tables and skip.

**Migration ID derivation.** When `--migration-id` is empty, the ID
hashes source/target engine names plus DSN host info — same shape
`Streamer.resolveStreamID` uses. Operators who need stable identity
across DNS shifts or host renames pass `--migration-id` explicitly.
The hash is kept short (16 hex chars) so the PK column stays compact
and human-friendly in `psql`.

**Failure handling never masks the original error.** When a phase
errors, the orchestrator attempts a final state write recording the
phase and truncated message. If that secondary write also fails, it
joins via `errors.Join` so the operator sees both — the primary
cause stays the head of the chain, preserving any phase-hint the
caller already attached.

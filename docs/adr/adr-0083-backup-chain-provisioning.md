# ADR-0083: Backup-chain provisioning (`--chain-slot`) and the chain-resume preflight

- **Status:** Accepted (implemented; task #40)
- **Date:** 2026-06-10
- **Relates to:** ADR-0046 (inline backup-chain rotation), the Phase 3
  logical-backup design (`docs/dev/design/logical-backups-phase-3.md`),
  ADR-0020 / Bug 15 (ack-after-apply slot discipline)

## Context

A snapshot-anchored full backup records, on its manifest's
`EndPosition`, the chain-handoff position the next `backup
incremental` must resume CDC from: `{slot, consistent_point_lsn}`. The
anchor slot used to pin the snapshot is deliberately **temporary**
(timestamped name, dropped on Close) — a standing slot nobody consumes
retains WAL forever, the classic abandoned-slot footgun, and the
"Contain Postgres complexity" tenet says don't leave server-side state
behind silently. The chain-handoff slot was therefore *the operator's
responsibility* to create **before** the full (via `sluice sync start`
or manually).

Benchmarking the backup surface on a 133 GB corpus (2026-06-10)
surfaced three distinct failures of that contract, one per layer:

1. **Nothing provisions the chain.** A full backup against a fresh
   source records an EndPosition naming a slot that does not exist.
   The first `backup incremental` then refused with "source has pruned
   past parent's terminal position" — misleading twice over (nothing
   was pruned; the slot never existed).
2. **The natural recovery is the silent-loss shape.** Creating the
   slot *after* the full (exactly what the misleading refusal invites)
   makes the next incremental **succeed silently without the
   in-between writes**: PostgreSQL's walsender fast-forwards
   `START_REPLICATION` to the slot's `confirmed_flush_lsn` without
   complaint. The live add-table flow already guards this exact class
   (`Engine.ReadSlotPosition`: "events in [snapshot-LSN,
   confirmed_flush_lsn] would be silently dropped"); the backup chain
   had no counterpart.
3. **The publication has the same creation-time rule, sharper.**
   pgoutput resolves publication membership with a **historic catalog
   snapshot** at each WAL record's LSN. A publication created after
   the anchor cannot decode the chain's first window at all (loud
   `publication "sluice_pub" does not exist` from the walsender, even
   though `pg_publication` now contains it — observed live in the
   benchmark); a table *added* to an existing publication late is
   silently filtered for the earlier records.

A fourth, adjacent hazard fell out of implementing the fix: the
chain consumers (`backup incremental`, `backup stream`) attach no
applier, so the CDC reader's keepalive fell back to acking the
**streamed** LSN (the pre-Bug-15 shape) — events parsed by the pump
but discarded at window close could advance `confirmed_flush_lsn`
past the recorded EndPosition, silently gapping the next link
(timing-dependent: a keepalive landing in the window-close teardown).

## Decision

Three coordinated pieces, all loud-failure-first:

### 1. `backup full --chain-slot` provisions the chain at the anchor

With `--chain-slot`, the **persistent chain slot** (named by
`--slot-name`, default `sluice_slot`) is created *as* the snapshot
anchor instead of the timestamped temporary slot. Its
`consistent_point` **is** the recorded EndPosition, so the chain has
zero gap *by construction*. The publication (`sluice_pub`,
`FOR ALL TABLES`) is ensured **before** the slot so the historic
catalog covers the chain from its first LSN.

Lifecycle: commit-on-success. `ir.BackupSnapshot` grows a `CommitFn`
the orchestrator calls exactly once, after the final manifest flips
`complete`; only then does Close skip the slot drop. A failed run's
Close drops the slot so retries start clean. An already-existing slot
is refused loudly (its consistent point is *not* this backup's anchor;
the refusal names the three plausible owners and the recovery).

> **Amended by ADR-0085 (task #42):** Commit now fires once the run's
> anchor-stamped IN-PROGRESS manifest is durable — an interrupted run
> deliberately keeps the slot (it is the WAL-retention guarantee a
> resume adopts), and the already-exists refusal's crashed-run clause
> now says "re-run the same command — resume adopts the slot" instead
> of advising drop + retry (which released the gap-covering WAL).

Defaults unchanged: without the flag, the temporary-anchor shape and
an end-of-anchor INFO hint ("to chain incrementals … re-run with
--chain-slot"). Engines without a slot concept (MySQL) WARN-no-op
(the #26 discipline); requesting `--chain-slot` where the
snapshot-anchored path is unavailable (no opener, `wal_level=replica`)
is a refusal, not a silent degrade.

`OpenBackupSnapshot` / `OpenBackupSnapshotForTables` now take an
`ir.BackupSnapshotOptions` struct (SlotName + PersistChainSlot) — the
extension point task #39's parallel readers will reuse.

### 2. Chain-resume preflight (`ir.ChainResumePreflighter`)

Before `backup incremental` / `backup stream` opens CDC at the
parent's EndPosition, the engine verifies the slot can actually serve
it. Postgres refuses when the slot is missing (message says it may
never have existed, names `--chain-slot`) or when
`confirmed_flush_lsn` is **ahead** of the parent position (the
silent-loss shape, with exact positions and recovery in the message).
`confirmed_flush_lsn` at or behind the parent is healthy (equal is
the steady-state; behind just means the final ack didn't land —
PostgreSQL replays from the requested LSN). MySQL omits the surface:
binlog positions are client-side and pruning already fails loudly at
stream open.

### 3. Chain-consumer ack discipline (hold + ratcheted release)

The PG CDC reader gains `HoldSlotAckAtCommitted()` /
`ReleaseSlotAckTo(pos)`: with hold set (before `StreamChanges`), the
keepalive never advertises past `max(startLSN, released ceiling)`;
the orchestrators release to each window's EndPosition **after** its
manifest commits. One-shot incrementals effectively release via the
next run's start ack; the long-lived `backup stream` releases per
rollover, bounding source WAL retention to ~one rollover window. The
clamp composes with (does not replace) the ADR-0020 applier tracker.

## Consequences

- The two-step operator dance (create slot + publication before the
  first full, or hand-edit positions) collapses to one flag, and every
  way of getting it wrong is now a loud refusal instead of a quietly
  gapped chain.
- A `--chain-slot` full that crashes hard (no Close) leaks the slot.
  ~~The next `--chain-slot` run hits the already-exists refusal whose
  message names the drop command. The resume-path interaction is
  task #42's design question.~~ Resolved by ADR-0085: the re-run
  RESUMES and adopts the slot (its snapshot opens temp-anchored, so
  the refusal is never reached); the refusal now fires only on a
  fresh-start run and no longer advises drop+retry as crash recovery.
- WAL retention cost is explicit and documented in the flag help: an
  unconsumed chain slot retains WAL until the next incremental (or
  `sluice slot drop`).
- Pins: unit (commit-only-on-success, opts threading, fallback
  refusals, ack clamp/ratchet semantics, preflight position shapes) +
  integration on real PG (end-to-end zero-setup chain with checksum
  verification, late-created-slot refusal, missing-slot guidance,
  existing-slot refusal, uncommitted-Close-drops-slot /
  committed-Close-keeps-slot).

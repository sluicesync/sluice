# ADR-0111: Native-MySQL resumable cold-copy via re-snapshot-from-cursor

## Status

**Proposed (2026-06-22).** Brings the **native vanilla-MySQL** concurrent cold-copy
path to the resilience parity that [ADR-0072](adr-0072-resumable-coldstart-copy.md)
gave the **VStream** (PlanetScale/Vitess) cold-start. Roadmap task #96.
`-race`-before-tag concurrency + **value-fidelity-critical** chunk (the CDC-anchor
decision is a silent-loss risk if wrong → mandatory value-fidelity review + pinned
differential tests).

## Context

The v0.99.103 PS-320-v13 live run rode the full 12→39→62→214 GB storage-grow on the
**write** side (the ADR-0110 grow-gate + the wall-clock retry both held — 17 gate
trips, 0 write exhaustion). But during the prolonged 214 GB grow a **source-read**
connection (`products`) dropped with `mysql: rows iteration: invalid connection`, and
that aborted the whole cold-copy → auto-resnapshot → **full restart from row 0** (~56 GB
re-copied). That is the exact restart-from-scratch outcome the grow-window arc exists to
eliminate.

### Why the migrate-path read-retry can't just be wired in

The migrate cold-copy already has `copyTableWithSourceReadRetry` (ADR-0109): on a
classified source-read drop it opens a **fresh** reader and resumes the table from its
persisted chunk cursor. That is safe for migrate because migrate's readers are
**independent / re-observable** — migrate is one-shot with no CDC handoff, so there is
no single cross-table consistent position to preserve.

The **sync native-concurrent** path is different. `cdc_snapshot_concurrent.go` takes an
FTWRL, records ONE binlog position (the CDC handoff anchor), then pins N connections
each in `START TRANSACTION WITH CONSISTENT SNAPSHOT` at that instant. InnoDB **cannot
recreate that consistent read-view** once a connection drops — a fresh reader would read
at a *different* position, silently mixing snapshot points across tables. So naively
wiring `copyTableWithSourceReadRetry` here would be a **silent-loss bug**, which is why
ADR-0109 explicitly scoped the sync path out.

### What ADR-0072 already does (for VStream only)

ADR-0072 made the VStream cold-start COPY resumable: it checkpoints a per-table
`TablePKs` cursor to the control table on a bounded cadence, and on a drop either
reconnects the VStream **in place** from `currentVgtid`+`TablePKs` (Phase C) or
warm-resumes — vtgate re-establishes the copy plan and resumes `WHERE pk > lastpk` for
the still-copying tables, flowing through the idempotent applier with zero loss. This
works because vtgate **can** resume a copy from a cursor against the primary's schema
engine. The native-MySQL `CONSISTENT SNAPSHOT` has no such re-establishable copy plan,
so native MySQL never got ADR-0072 and falls back to restart-from-scratch.

## Decision

Give the native-MySQL concurrent cold-copy a **re-snapshot-from-cursor** recovery — the
native analog of ADR-0072, accounting for the non-re-observable snapshot:

1. **Per-table PK-cursor checkpoint (native `CopyCheckpointer`).** The concurrent reader
   (`concurrentBinlogRows`) checkpoints each table's last-emitted PK to the control
   table on the same bounded N-rows-or-T-seconds cadence ADR-0072 uses, plus a per-table
   "complete" marker when a table's copy finishes. Reuses the existing
   `ir.CopyCheckpointer` seam + `applyCopyCheckpoint` wiring; the cursor is the table's
   ordered PK (the same shape the keyset-chunk machinery already persists).

2. **Recovery = re-snapshot at P′, resume from cursors, keep the CDC anchor at the
   ORIGINAL P.** On a classified source-read drop, instead of dropping all target tables
   and re-copying from 0:
   - Re-establish a fresh consistent snapshot (new FTWRL → new position **P′**).
   - **Skip** tables already marked complete.
   - **Resume** each incomplete keyed table from its persisted PK cursor (`WHERE pk >
     lastpk`) read at P′.
   - **Keep `sluice_cdc_state.source_position` anchored at the ORIGINAL P** (the
     earliest position), NOT P′.

3. **Why anchoring CDC at the EARLIEST position is correct (the value-fidelity crux).**
   After recovery, completed tables reflect P; resumed tables reflect a mix of P (rows ≤
   cursor) and P′ (rows > cursor). Anchoring CDC at the earliest position P and letting
   the **idempotent** applier replay P→now means:
   - A row that changed between P and P′ and was read at its P value → CDC re-applies the
     change. Converges.
   - A row read at its P′ value → CDC re-applies P→P′ again → idempotent UPSERT/delete-by-PK
     → converges.
   So **keyed tables converge to the exact source state**. This is the same idempotent-
   replay guarantee ADR-0010/ADR-0072 already rely on; anchoring at the *latest* position
   would instead **skip** P→P′ changes on the completed tables — silent loss — so the
   earliest-anchor is load-bearing.

4. **Keyless tables: truncate + restart that table (at-least-once).** A table with no
   usable identity key has no safe mid-table cursor, so on recovery it truncates and
   re-copies from row 0 at P′ — always dup-free/loss-free against the source, consistent
   with the existing keyless at-least-once cold-copy contract (Bug 143). Only the keyless
   tables restart, not the whole run.

5. **Bounded + loud.** The re-snapshot recovery rides the same bounded budget as the
   other grow-window retries; on exhaustion it surfaces loudly (and the old
   full-restart auto-resnapshot remains the ultimate backstop). The source binlog at P
   must still be available — the operator already ensures `binlog_expire_logs_seconds`
   > 48 h (the PlanetScale-import requirement), which this depends on; if P has been
   purged, recovery falls back to a full re-snapshot at P′ (restart-from-scratch, the
   current safe behavior) with a loud WARN naming the retention cause.

## Consequences

- **Win:** a source-read drop during a grow (or any transient on a long native-MySQL
  cold-copy) resumes only the incomplete tables from their cursors instead of re-copying
  everything — eliminating the restart-from-scratch that v13 hit, the operator's core
  goal. Native MySQL reaches ADR-0072 (VStream) parity.
- **Cost:** per-table cursor checkpointing on the native concurrent path (a bounded
  control-table write, off the hot path, exactly as ADR-0072); a re-snapshot/resume
  recovery routine; the CDC-anchor-at-earliest bookkeeping.
- **Not changed:** the happy-path cold-copy, the consistent-snapshot model on the first
  pass, the exactly-once CDC contract. Keyed convergence and keyless at-least-once are
  preserved by construction.

## Validation

- **Unit:** the cursor checkpoint cadence + per-table complete marker; the recovery
  skip-completed / resume-from-cursor / truncate-keyless decision; the CDC-anchor stays
  at the original P across a re-snapshot (the silent-loss guard).
- **Value-fidelity review (mandatory):** re-derive the keyed-converge / keyless-at-least-once
  matrix; prove anchoring at the earliest position cannot skip a P→P′ change on a
  completed table.
- **Integration (`-race`):** inject a classified source-read drop mid-copy on one table
  of a multi-table native-MySQL→MySQL concurrent copy; assert recovery re-snapshots,
  skips completed tables, resumes the dropped table from its cursor, anchors CDC at the
  original position, and converges byte-identically to a clean run (src md5 == dst,
  0 dups/gaps) — keyed AND keyless variants.
- **Live:** re-run the PS-320 grow scenario (the v13 setup) and confirm a source-read
  drop during the grow resumes-from-cursor instead of restarting from scratch.

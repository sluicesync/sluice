# ADR-0111: Native-MySQL resumable cold-copy via re-snapshot-from-cursor

## Status

**Accepted (2026-06-22, implementation landed).** Brings the **native
vanilla-MySQL** concurrent cold-copy path to the resilience parity that
[ADR-0072](adr-0072-resumable-coldstart-copy.md) gave the **VStream**
(PlanetScale/Vitess) cold-start. Roadmap task #96. `-race`-before-tag concurrency
+ **value-fidelity-critical** chunk (the CDC-anchor decision is a silent-loss risk
if wrong → mandatory value-fidelity review + pinned differential tests).

**Implementation note — scope landed vs. deferred (see Consequences).** The
**in-process re-snapshot-from-cursor recovery** (§Decision 2–5, the operator's
core goal: a source-read drop resumes incomplete tables instead of restarting
from row 0) is implemented, with the §3 CDC-anchor-at-earliest invariant enforced
in code (a runtime guard + a dedicated unit pin) and the keyed-converge /
keyless-at-least-once matrix tested. The **control-table PERSISTENCE of the
per-table cursors** (§Decision 1) — and therefore a PROCESS-restart resume of an
interrupted native cold-copy — is **deferred** as a value-fidelity-driven scoping
decision: persisting a mid-cold-copy cursor on the native concurrent path without
also (a) native `SnapshotStreamResumer` routing and (b) a durable-write watermark
coupling (the concurrent copy path deliberately wires no durable-progress
reporter) would let a process crash leave the persisted cursor AHEAD of the
durably-written rows → a silent gap on restart. The cursors therefore live
in-memory (sufficient for the in-process recovery, which is the WIN); the
persistence + process-restart resume is a separable follow-up.

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
- **Cost:** in-memory per-table cursor tracking on the native concurrent path; a
  re-snapshot/resume recovery routine; the CDC-anchor-at-earliest bookkeeping. The
  keyed happy-path read changes from a single full-scan to a cursor-paginated read
  (ORDER BY pk, LIMIT N) so a re-snapshot can resume WHERE (pk) > cursor — a small
  read-side cost for the resumability; keyless tables keep the single full scan.
- **Implemented (in-process recovery, the WIN):** a source-read drop during a grow
  (or any transient on a long native-MySQL cold-copy) re-snapshots and resumes only
  the incomplete tables from their cursors instead of re-copying everything —
  eliminating the restart-from-scratch that v13 hit. The recovery closes the old
  snapshot transactions BEFORE acquiring the fresh FTWRL (otherwise the new FTWRL
  blocks behind the old transactions' metadata locks — observed as a multi-minute
  stall in the first integration run, then fixed).
- **DEVIATION — control-table cursor persistence + process-restart resume DEFERRED
  (value-fidelity-driven).** §Decision 1 specified persisting the per-table cursors
  to the control table. That is NOT implemented; the cursors live in-memory, used
  only by the in-process recovery. Reason: the native concurrent copy path
  deliberately wires NO durable-write progress reporter
  (`copy_concurrent_tables.go` — "MID-COPY CHECKPOINT DISABLED"), so a persisted
  cursor could, on a PROCESS crash, sit AHEAD of the durably-written rows → a silent
  gap on a process-restart resume. Persisting safely would require additionally (a)
  native `SnapshotStreamResumer` routing (today the vanilla flavor's
  `PositionCarriesCopyCursor` returns false and `OpenSnapshotStreamFromPosition`
  refuses — only VStream resumes) and (b) coupling a durable-write watermark to the
  cursor (the ADR-0072 v0.99.9 breadcrumb machinery). Both are separable, larger
  changes; shipping persistence without them would REGRESS process-restart to a
  silent gap. The in-process recovery — the operator's core goal — needs only the
  in-memory cursor. Follow-up: add the durable-watermark + native resumer to
  graduate to process-restart parity.
- **DEVIATION — keyless recovery is at-least-once, not truncate-dedup.** §Decision 4
  specified TRUNCATE+restart for keyless tables on recovery. The reader cannot
  truncate the target (that is target knowledge a source reader must not hold), so a
  keyless table instead re-reads from the start on the fresh snapshot: loss-free,
  possibly with duplicate rows — exactly the existing keyless at-least-once cold-copy
  contract (Bug 143; the recovery WARN names it). Making keyless recovery dup-free
  would need a pipeline-side target-truncate hook; deferred as a refinement.
- **Not changed:** the happy-path cold-copy correctness, the consistent-snapshot model
  on the first pass, the exactly-once CDC contract. Keyed convergence (exactly-once)
  and keyless at-least-once are preserved by construction; the CDC anchor stays at the
  earliest P (a runtime guard refuses any advance).

## Validation

- **Unit (landed):** the per-table cursor + complete-marker tracking; the recovery
  skip-completed / resume-keyed-from-cursor / restart-keyless decision; the orderable-PK
  family matrix (every PK type family, the Bug-74 discipline — a missed family would
  route a resumable keyed table to the lossy keyless restart); the bounded backoff; the
  no-resnapshot-wired-is-terminal and binlog-purged-falls-back guards; and **the
  dedicated CDC-anchor-stays-at-P silent-loss guard** (`TestVerifyCDCAnchorUnchanged` +
  `TestConcurrentReader_AnchorNeverMutatesUnderRecovery`).
- **Integration (landed; `-race` is the CI gate — see below):** inject a CLASSIFIED
  source-read drop mid-copy on one table of a multi-table native-MySQL concurrent copy;
  assert the genuine recovery (real FTWRL re-snapshot on the container) resumes the
  dropped KEYED table from its PK cursor and converges to the exact source id set (no
  gap, no dup), anchors CDC at the ORIGINAL position, and a post-snapshot insert still
  surfaces on CDC from that anchor; a KEYLESS variant re-reads from start (at-least-once,
  every source value present). `TestNativeConcurrentResume_KeyedFromCursor` /
  `TestNativeConcurrentResume_KeylessAtLeastOnce`.
- **`-race` (CI-only, REQUIRED before tag):** this chunk is concurrency-touching
  (swappable connection set under connMu, peer-coalesced recovery under recoveryMu, the
  abandon-old-conns-while-peers-read window). The local box is CGO=0 so `-race` cannot
  run here; the main session must land it through CI's `-race` Integration job before any
  tag.
- **Live (pending main session):** re-run the PS-320 grow scenario (the v13 setup) and
  confirm a source-read drop during the grow resumes-from-cursor instead of restarting
  from scratch.

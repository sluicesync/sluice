# ADR-0109: Cold-copy source-read reconnect + resume resilience

- **Status:** Accepted
- **Date:** 2026-06-21
- **Related:** ADR-0108 (cold-copy *target-write* reparent-retry — the write-side sibling), ADR-0096 (keyset-chunked reads), ADR-0101/0102 (native concurrent cold-copy + per-table fan-out), ADR-0007 (position/data atomicity), ADR-0038 (apply-phase retry classifier), ADR-0010 (idempotent UPSERT). Roadmap item 34.

## Context

A bulk cold-copy that runs for tens of minutes can have its **source read connection
dropped mid-table** by a transient that is *not* itself a source fault. The live
finding (Track-D PS-320, 2026-06-21): copying onto a non-Metal PlanetScale target,
the target's **binlog volume hit `errno 28 — No space left on device` during a storage
auto-grow** (a fast bulk load generates enormous binlog churn that fills the volume
before it grows). The target replica's SQL thread failed
(`HA_ERR_RBR_LOGGING_FAILED`), so under semi-sync the target **primary's writes
stalled** — they *blocked* rather than returning an error. sluice's reader/writer
pipeline then backpressured: the writer could not drain, so the reader stopped
consuming from the source, the source connection went idle past the source server's
`net_write_timeout` (default 60 s), and the **source server closed the read
connection** → `unexpected EOF` → `mysql: rows iteration: invalid connection` → the
whole cold-copy aborted.

ADR-0108 made the *target-write* path ride through a transient by retrying a write
that **returns a retriable error**. It does not (and cannot) help here: the target
write **blocked** rather than erroring, so the first error sluice saw was on the
**source-read** side. Riding through a transient target stall during cold-copy
therefore also requires the **source-read** side to reconnect and resume, rather than
abort the run. This is loud-failure-safe today (the copy dies cleanly with a `--resume`
hint; no silent loss), but not resilient — a copy that happens to cross a target
storage-grow mid-flight dies and needs a manual restart.

## The sync-path constraint (corrected after implementation)

The first cut of this ADR assumed a **per-table reconnect + resume** was always
possible. It is **not** on the `sync` cold-start path — and that is the path the live
Track-D failure took. The `sync` native cold-copy holds **one consistent multi-table
snapshot** (FTWRL-coordinated, all readers frozen at a single binlog position so CDC
can stitch from it). **MySQL cannot re-establish that snapshot on a fresh connection**
— a reconnect gets a *later* point, so re-reading one table on a new connection would
mix rows from two points and break exactly-once CDC stitching. Per-table reconnect is
therefore only viable on the `migrate` path (independent per-table readers, no shared
consistent point). The fix is split accordingly.

## Decision

A three-pronged defense, addressing the mechanism (a target stall backpressures the
pipeline → the source read idles past `net_write_timeout` → the source drops it):

### (A) Raise the source read session timeouts — PRIMARY (prevents the drop)

sluice **`SET SESSION net_write_timeout` / `net_read_timeout`** to a generous bounded
value (default ~10 min) on every source read connection it opens (the same session
seam that already sets `workload=olap` for PlanetScale reads). A transient target
stall (e.g. a storage auto-grow taking seconds-to-minutes) then no longer causes the
source to drop sluice's idle read connection; when the target recovers, the writer
drains, the reader resumes consuming, and the copy continues — **no reconnect, no
re-snapshot, no consistency problem.** The bound stays finite so a *genuinely* dead
target still surfaces (complemented by sluice's existing source-unresponsive
detection), rather than hanging forever.

### (B) Bounded auto-restart of the cold-start — BACKSTOP (sync path)

If a source read still drops (a stall longer than the raised timeout, or an unrelated
drop), the classified-retriable read error (see (C)) must **not** be a fatal exit.
On the `sync` path — where per-table resume is impossible — it triggers a **bounded,
backed-off auto-restart of the whole cold-start** (re-snapshot at a fresh consistent
point + re-copy), rather than crashing the process. This re-copies, which is
wasteful, but it is the only consistency-preserving recovery given the snapshot
constraint, and it is bounded + loud (never an infinite crash-loop like an external
watchdog). It composes with the existing ADR-0093 reactive-resnapshot machinery.

### (C) Per-table reconnect + resume — `migrate` path only

On the `migrate` path (independent per-table readers, no shared consistent snapshot),
a classified-retriable source-read error opens a **fresh per-table reader** and
resumes from a dup/loss-safe position, bounded, loud on exhaustion. This is the
read-side analog of ADR-0108 and is the efficient (no re-copy) recovery where the
architecture permits it.

### Classification (shared by A/B/C)

### Classification

Reuse `classifyApplierError` (the ADR-0038 classifier). The source connection drop
surfaces as `gomysql.ErrInvalidConn` / `io.EOF` / `driver.ErrBadConn` /
`connection reset by peer` / `broken pipe` / `i/o timeout` — all already classified
retriable. Any non-retriable read error (decode error, real query error) stays
terminal, exactly as today. The retry never invents a new retry class.

### Resume-position safety, by copy path (the value-fidelity core)

The resume point must be **at or behind the durable (committed-to-target) frontier** —
never past it (that would silently lose the rows between the resume point and the
durable frontier). Three cases:

1. **Keyset-chunked read (ADR-0096/0102 — the big-table path, parallel/fan-out).**
   Each chunk persists its `chunk.LastPK` after a flush commit; that is the durable
   position. On reconnect, resume the in-flight chunk from `WHERE (pk) > LastPK`
   (`ReadRowsBatchBounded(ctx, table, LastPK, upTo, limit)`). Already-committed rows
   are not re-read (dup-free); no gap is left (loss-free). This is the existing
   crash-resume machinery; item 34 routes an in-flight drop into the same path. This
   is the path the live-failing `documents` table (integer PK, `--copy-fanout-degree
   4`) takes.

2. **Idempotent (UPSERT) read path (VStream snapshot / warm-resume / add-table).**
   Resume from the durable frontier; the UPSERT (`ON DUPLICATE KEY UPDATE`) absorbs
   any re-emitted overlap (ADR-0010). Loss-safe because the persisted COPY position is
   gated at-or-behind the durable frontier (ADR-0007). If a precise mid-table durable
   position is unavailable, restart the table from the start — the UPSERT makes that
   safe (just re-work).

3. **Plain single-stream / keyless / non-orderable-PK read (no safe mid-table keyset
   cursor).** Cannot resume from a partial position without risking a dup (plain
   INSERT) or a gap. Fall back to **truncate the target table + restart that table's
   copy** — a clean slate that is always dup-free and loss-free, bounded by the same
   retry budget. This is less efficient (re-copies the table) but correct; it is only
   taken for tables that are not keyset-chunkable (small tables, keyless tables, or
   non-orderable PKs). Keyless cold-copy is already at-least-once (Bug 143), so a
   restart is consistent with the existing contract.

### Bounds, observability, composition

- **Budget:** mirror ADR-0108 — 12 attempts, exponential backoff 100 ms → 30 s cap
  (~4 min envelope), enough to ride a storage-grow / failover, short enough that a
  genuinely-down source surfaces. Package-baked (no config field → no zero-value
  trap). Honor `ctx.Done()` during backoff.
- **Loud:** a `WARN` per retry (table, attempt, backoff, err); a **loud terminal
  error** on exhaustion that wraps the underlying transient (never silent, never
  infinite).
- **Per-table, fan-out-safe:** the retry is local to a table's read loop, so a
  transient on one table reconnects and resumes without aborting its sibling table
  copies (improves on today's errgroup-cancel-all). A genuine non-retriable error
  still cancels peers, unchanged.

### Non-goals

- Does **not** change the durable-frontier / position-atomicity contract (ADR-0007).
- Does **not** make the *target-write stall itself* recoverable beyond ADR-0108 — it
  makes the **source-read side** survive the backpressure the stall induces, so the
  copy resumes once the target recovers (e.g. the auto-grow completes).
- Postgres `COPY`-protocol source-read path: the same gap exists; this ADR scopes the
  implementation to the MySQL source (the demonstrated path) and notes PG as a
  follow-up.

## Consequences

- A cold-copy survives a transient source-read drop — including the backpressure-EOF
  induced by a target storage-grow stall — by reconnecting and resuming from a
  dup/loss-safe position, instead of dying. Combined with ADR-0108 (target-write
  reparent-retry), the cold-copy now rides through both faces of a PlanetScale
  storage-auto-grow event (the reparent-*error* face and the disk-full-*stall* face).
- A real user migrating from their own MySQL (default `net_write_timeout=60s`) no
  longer needs to hand-tune the source timeout to survive a target stall.
- The truncate-restart fallback (case 3) is a named efficiency wart for
  non-keyset-resumable tables; it is always correct.

## Validation

Pinned by unit tests: keyset-chunked resume from `LastPK` after an injected
retriable read drop (dup-free + loss-free, src==dst); idempotent resume-from-frontier;
plain non-chunkable truncate-restart; first-attempt non-retriable read error stays
terminal; budget exhaustion → loud terminal; ctx-cancel during backoff unwinds;
sibling tables unaffected by one table's transient. The live re-validation requires a
**fresh small-volume** non-Metal PS target (so the copy re-crosses the 10→39 GB grow
that triggers the binlog-fill stall); a pre-grown volume will not reproduce it. The
`-race` integration gate runs before tagging (concurrency / crash-recovery chunk).

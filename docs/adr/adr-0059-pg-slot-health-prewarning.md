# ADR-0059 — Pre-emptive Postgres replication-slot health WARNings

## Status

**Accepted (2026-05-24).** Closes task #46 — severity-A finding F13 of
the 2026-05-22 Reddit-research run
(`C:\code\sluice-reddit-research-2026-05-23.md`). Sluice now probes
the active replication slot's WAL-retention pressure and active-
liveness on a 30-second cadence and emits structured slog WARNs
before the slot can be invalidated by Postgres's retention-bound
eviction, turning a slow-burning silent-loss class into a loud-
failure event the operator can act on.

## Context

### The silent-loss class operators reported

The dominant Postgres logical-replication failure mode reported in
operator threads (the F13 finding) is not loud at all — it's silent:

1. A replication slot exists and is healthy.
2. The consumer (sluice, or any other process) falls behind, crashes,
   or has its connection killed by a `wal_sender_timeout`.
3. The slot's `restart_lsn` stops advancing while the source keeps
   writing WAL. Postgres retains every WAL segment between
   `restart_lsn` and `pg_current_wal_lsn()` to honour the slot.
4. The retained WAL exceeds the `max_slot_wal_keep_size` GUC bound.
5. Postgres invalidates the slot (`pg_replication_slots.wal_status`
   transitions to `'lost'`).
6. The operator discovers the broken pipeline some time later — often
   hours after the actual incident — and the only recovery is a full
   re-snapshot (the LSN the consumer was tracking no longer has any
   WAL segments backing it).

Sluice's pre-F13 surface gave the operator no warning between steps 2
and 5. The existing F2 surface
(`pg_stat_replication_slots.spill_*`, ADR shipped in v0.74.x) exposes
in-memory-decode spilling pressure, which is a different signal —
spilling tells you the slot's decoder is under memory pressure right
now; F13's retention-bound metric tells you the slot is approaching
*eviction*. Both can fire independently.

### What "loud-failure to operator" should look like for F13

Three operator-visible conditions are worth surfacing on a periodic
cadence:

1. **WAL retention pressure WARN at >= 70%.** "Slot is holding back
   N MB of WAL, which is 70% of the `max_slot_wal_keep_size` bound;
   the consumer may be falling behind." The operator has runway to
   investigate (the slot won't evict for the remaining 30% headroom
   even under bursty workloads typical of OLTP traffic).
2. **WAL retention pressure CRITICAL at >= 85%.** Same shape, louder
   tone (`critical=true` slog key), more urgent action hint
   ("intervene now, or accept a re-snapshot").
3. **Slot-inactive WARN at >= 30m.** The slot exists but no consumer
   is attached, and it's been that way for at least 30 minutes —
   long enough to skip transient reconnects and short enough to land
   inside an operator's pager window.

None of these conditions auto-act. The tenet of "loud-failure to
operator, not auto-remediation" applies: pausing the sluice writer or
raising the GUC on the source on sluice's own initiative would
violate operator trust. The WARN is the surface; the action is the
operator's.

## Decision

Add a per-stream background goroutine in the streamer
(`internal/pipeline/slot_health.go`) that probes a new optional
engine-side interface (`ir.SlotHealthReporter`) on a 30-second cadence.
Postgres's `SchemaReader` implements the reporter via a single SQL
query against `pg_replication_slots` + `pg_current_wal_lsn()` +
`pg_settings`. The threshold-and-rate-limit logic is a pure function
on top of the reporter's snapshot, unit-tested independently of any
live DB. WARN emission goes through `log/slog` with structured fields
matching the rest of sluice's loud-failure surfaces.

### Thresholds and rationale

| Condition                  | Threshold           | Why                                                                                                                                                                                                |
| -------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Retention pressure WARN    | 70%                 | Leaves a ~30% headroom buffer for routine bursts (large index builds, ETL windows). Below this the operator gets no signal; above it they have time to act before eviction.                        |
| Retention pressure CRIT    | 85%                 | A 15% headroom is "act now" territory — typical bursty OLTP traffic can consume 5-10% of `max_slot_wal_keep_size` inside a single checkpoint interval. Distinct slog key so it can be paged on.    |
| Inactivity WARN            | 30 minutes          | Longer than any legitimate reconnect window we've observed (cloud PG failover events typically resolve inside 5-10m). Shorter than a typical operator pager rotation (4-8h), so the alarm lands.   |
| Per-condition rate-limit   | 5 minutes           | Empirically operator-friendly: same condition re-firing every 30 seconds floods the log; same condition emitted once every 5 minutes is "I can still see this is still happening" without spam.    |

### State transitions emit immediately

The rate-limit window applies only to *same-state repeats*. A
transition (clean → warn, warn → critical, critical → warn, anything
→ clean) emits unconditionally, regardless of how recently the prior
WARN fired. The reasoning: a warn70 → critical85 escalation inside
the 5m window is exactly the case where the operator most needs
*prompt* visibility — suppressing it on rate-limit grounds would
defeat the purpose.

### Clear events emit an INFO, not silence

When a previously-firing condition resolves (e.g. the consumer drains
its backlog and the lag percentage drops back below 70%), the loop
emits a one-line `slog.Info` "condition cleared" with the slot name
and current values. The alternative — silence — leaves the operator
guessing whether the alarm resolved itself or sluice's warner just
broke. The cost of one INFO line on transition is trivial.

### -1 (unlimited) bypasses the percentage path

`max_slot_wal_keep_size = -1` is Postgres's documented "unlimited"
sentinel and is the **default** value on every PG 14+ installation
the operator hasn't tuned. There is no retention bound to compare
against; the evaluator skips the percentage warnings on this path
(the inactivity path still fires). This is the most common
configuration: operators who haven't read the WAL-retention chapter
won't see retention warnings, which is correct — there's nothing to
warn about. Operators who *have* set the GUC will see warnings
calibrated to their bound.

### `max_slot_wal_keep_size = 0` is treated as "no percentage path"

The extreme value 0 (PG documents this as "slots cannot retain WAL
beyond the next checkpoint") would make every non-zero lag report
infinity-percent under the naive formula. The evaluator special-
cases 0 to skip the percentage path (same shape as -1). Operators
who deliberately set 0 are accepting frequent slot loss as a policy;
the inactivity path still surfaces the consequence.

### Retention supersedes inactivity when both hold

A slot can be both lagging and inactive simultaneously (the consumer
crashed in the middle of being behind). The evaluator surfaces the
retention WARN, not the inactivity WARN, because:

- Retention is the eviction-imminent signal — it's the one PG will
  act on without operator intervention.
- The retention WARN's structured fields already include `active`,
  so the inactivity context isn't lost.
- Surfacing both would double-spam the log on every probe.

### Why a separate goroutine instead of piggybacking on the pump's keepalive timer

The CDC reader's `pump()` loop owns the replication-mode `*pgconn.PgConn`
exclusively and intentionally avoids ad-hoc SQL queries on that
connection (replication-mode connections cannot run normal queries).
Threading the probe through the pump would either (a) require a
second `*sql.DB` plumbed into the pump struct just for this one query
and the discipline of "this query must complete inside the keepalive
deadline or we lose a keepalive," or (b) require a second goroutine
anyway, defeating the "no new goroutine" framing.

The cleanest shape mirrors F2's `attachSpillReporter` wiring: the
streamer opens a dedicated `*sql.DB`-backed `SchemaReader` on the
source DSN, type-asserts the optional interface, and spawns a
goroutine on attachment. The goroutine and its DB are torn down by a
cleanup closure the streamer `defer`s.

## Scope

### In

- New `ir.SlotHealthReporter` interface + `ir.SlotHealth` struct.
- Postgres implementation in `internal/engines/postgres/slot_health_reporter.go`.
- Threshold evaluator + rate-limit state in
  `internal/pipeline/slot_health.go`, with the probe goroutine.
- Streamer wiring in `attachSlotHealthProbe`, mirroring the F2
  `attachSpillReporter` shape.
- Unit tests for every threshold and rate-limit boundary.
- Integration test for the SQL query against real Postgres.

### Out (deferred to separate tasks)

- **MySQL binlog-retention parallel.** MySQL's retention model
  (`expire_logs_days` + `binlog_expire_logs_seconds`) is filename-
  and time-based rather than byte-bound, and the operator-visible
  failure surface is different (the source returns
  `ER_MASTER_FATAL_ERROR_READING_BINLOG` rather than silently
  invalidating a slot). A separate task will design the MySQL
  parallel; it's not a "port the same code over" exercise.
- **Prometheus metric exposure.** Sluice's hand-written metrics
  registry (`internal/pipeline/metrics.go`) could surface
  `sluice_slot_wal_retention_percent` and
  `sluice_slot_inactive_seconds` gauges as small follow-ups; that's
  one extra `MetricsServer.Attach*` closure and ~15 lines of
  exposition formatting. Out of scope here to keep the PR focused on
  the WARN surface, which is the always-on / opt-out-by-default piece.
- **`sluice diagnose` integration.** The diagnose bundle already
  captures `pg_replication_slots` rows; surfacing F13's evaluator
  output in the bundle is a natural follow-up but not required for
  the WARN surface to be useful on its own.
- **Auto-action on threshold cross.** "On >85%, pause the sluice
  writer" / "On inactive >30m, drop and recreate the slot" would
  violate the loud-failure-to-operator tenet. Operators get the WARN;
  the recovery action remains the operator's.

## Consequences

### Trust-building

This is exactly the silent-loss class sluice's tenets prioritise over
feature throughput. An operator running F13's worst case (default GUC
not tuned, consumer crashes overnight) used to discover a broken
slot the next morning with no recovery path other than re-snapshot;
now they see structured WARNs in their log aggregator starting at
70%, escalating to CRITICAL at 85%, and a follow-up at the 30m
inactivity mark. The signal precedes the loss by hours.

### Cost

- One additional `*sql.DB` connection per stream, idle except for a
  single-row query every 30 seconds. Cost: one PG backend session;
  workload: negligible.
- One additional goroutine per stream. The race detector covers it
  (the loop's only mutable state is the per-condition rate-limit map,
  owned exclusively by the loop goroutine).

### What changes for cross-engine pairs

For MySQL-source streams the type-assertion to `ir.SlotHealthReporter`
fails, the dedicated reader is closed, and the probe doesn't run.
The stream is otherwise unaffected. When the MySQL parallel ships
(deferred), MySQL will implement a different optional interface
(binlog-retention-shaped) and the streamer will attach both
independently.

### What changes for `--dry-run`

Skipped — same as F2's `attachSpillReporter`. Dry-run doesn't run a
real stream, so probing a slot that doesn't exist would be noisy and
useless.

## Verification

Same matrix as the implementation tests:

- 70% boundary crossed → WARN emits.
- 85% boundary crossed (including skipping past 70 in one step) →
  CRITICAL emits.
- `max_slot_wal_keep_size = -1` → percentage path silent.
- `max_slot_wal_keep_size = 0` → percentage path silent (no divide-
  by-zero).
- Slot inactive >= 30m → inactive WARN emits.
- Slot active again → cleared INFO emits.
- Repeated warn within 5m → suppressed.
- Repeated warn outside 5m → re-emitted.
- 70 → 85 within rate-limit window → CRITICAL emits (transition rule).
- Both retention and inactivity hold → retention wins.

Plus an integration test against a live PG 16 container confirming
the SQL query returns the expected fields and the GUC-to-bytes
conversion is correct (64 MB → 67108864 bytes, -1 → -1).

## References

- Task #46 (F13 implementation prompt).
- `C:\code\sluice-reddit-research-2026-05-23.md` — F13 finding,
  severity A, source-thread quotes.
- ADR shipped in v0.74.x covering F2 (`pg_stat_replication_slots`
  spill counters) — adjacent surface, related but distinct signal.
- CLAUDE.md tenets — "Zero users is the current reality, not a
  problem to rush past" (silent-loss prevention > feature throughput)
  and "Validate end-to-end before building more" (F13 closes a known
  gap in the existing CDC surface before new features are layered on
  top).
- PostgreSQL docs: `max_slot_wal_keep_size`,
  `pg_replication_slots.wal_status`, `pg_wal_lsn_diff`,
  `pg_settings`.

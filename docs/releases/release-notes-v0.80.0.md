# sluice v0.80.0 — Pre-emptive PG slot-health warnings (closes silent slot eviction)

**Headline:** Minor release closing the **silent PG replication-slot loss** class — the dominant operator pain in Postgres logical-replication threads. Before v0.80.0, sluice's `pg_stat_replication_slots.spill_txns/spill_bytes` surfacing (v0.74.0 / F2) told operators about decoding spill, but said nothing about the slow burn toward slot eviction: a slot accumulates WAL because the consumer falls behind / disconnects, retention limits eventually fire, the slot drops with no warning, and the operator is left with a broken pipeline and no way back without a full re-snapshot. v0.80.0 surfaces the slow burn *before* eviction with three operator-actionable WARN conditions.

## Added

- **`feat(pipeline/postgres): pre-emptive PG slot-health warnings (#46 / ADR-0059)`**

  A 30-second-cadence background probe per PG-sourced stream surfaces three WARN conditions:

  - **WAL retention ≥70%** of `max_slot_wal_keep_size` — warning level. Names the slot, the bytes held, the percentage, and the action hint.
  - **WAL retention ≥85%** — critical level. Same shape with a more urgent action hint (eviction imminent).
  - **Slot inactive ≥30m** — when `pg_replication_slots.active = false` with no recent re-attach.

  Each condition is **de-duplicated within a 5-minute rate-limit window**; severity transitions (e.g. 70 → 85) emit immediately even within the window; **condition-clears emit a one-line INFO** so operators see the recovery, not just the alarm. `max_slot_wal_keep_size = -1` (unlimited; the PG default) cleanly bypasses the percentage warnings — no false positives on default PG.

  ### Architecture

  - `ir.SlotHealthReporter` optional interface + `ir.SlotHealth` value type
  - `internal/engines/postgres/slot_health_reporter.go` — PG impl, queries `pg_replication_slots` + `pg_current_wal_lsn()` + `max_slot_wal_keep_size` GUC
  - `internal/pipeline/slot_health.go` — pure-function threshold evaluator + rate-limit state + per-stream goroutine; mirrors F2's `attachSpillReporter` shape (dedicated `*sql.DB`, non-fatal on cross-engine sources)

## Tests

- **18 unit tests** covering every threshold/rate-limit boundary (Bug 74 "pin the class, not the representative" discipline): 70%/85% + skip-past-70 transitions, -1 unlimited bypass, 0 extreme bypass, inactive below/above threshold, active-probe-resets-inactivity, rate-limit suppress + expire, 70→85 transition emits inside window, clear→INFO, still-clean silent, retention-supersedes-inactive, plus 4 lifecycle tests for the probe goroutine + `slotHealthProbeAttachment.Close` idempotency.
- **4 integration tests** against PG 16 testcontainer (no-slot / empty-slot / default-unlimited GUC / explicit-64MB GUC-to-bytes); ~8.4s total.
- **`-race` integration gate passed in CI** — concurrency-class change (goroutine + ticker + per-loop state map + `sync.Once` close); validated on Linux runner per CLAUDE.md "Concurrency chunks" discipline.

## Docs

- **ADR-0059 — Pre-emptive PG slot-health pre-warning** (`docs/adr/adr-0059-pg-slot-health-prewarning.md`). Covers motivation (Reddit-research F13), thresholds + their rationale, the rate-limit policy + state-transition semantics, the per-stream goroutine + dedicated-`*sql.DB` design, and explicitly documents what's deferred.

## Compatibility

- **Behavior change to flag**: every Postgres-sourced stream now gets a background probe goroutine + a dedicated `*sql.DB` connection on the source. Cost is one idle backend session per stream (negligible against any production sluice deployment); benefit is operator-visible WARN before silent slot eviction. Disabled in `--dry-run` mode (the wiring path is bypassed). MySQL-sourced and target-only streams see no change. Cross-engine sources without a `SlotHealthReporter` impl (e.g. MySQL source) silently skip the probe.
- **Minor version bump (v0.79.1 → v0.80.0)** because the operator-visible WARN surface is a new observable behavior.
- **Severity a** — closes a catalogued silent-loss class. Postgres operators running long-lived sluice streams against busy sources are the most likely to have been near the silent-eviction edge without realizing it.
- **No new flag**. Always-on (except DryRun); the rate-limit window prevents noise on healthy slots.

## What's NOT in v0.80.0 (deferred to follow-ups)

Documented in ADR-0059 §6:

- **MySQL binlog-retention parallel** — different shape (MySQL doesn't have slots; the equivalent is binlog retention vs. consumer position). Separate task; operators specifically asked for PG slots per F13.
- **Prometheus metric exposure** — small follow-up (one extra `MetricsServer.Attach*` closure mirroring the F2 spill closure). Keep this release focused on the WARN-log surface.
- **`sluice diagnose` integration** — surfacing the probe state in the standard-level bundle.
- **Auto-action on threshold cross** (e.g. auto-pause sluice writer at 85%). The tenet is loud-failure-to-operator, not auto-remediation; operator action stays in operator's hands.

## Who needs this

- **Any Postgres operator running long-lived sluice streams** against a busy source — eventually the consumer falls behind, the slot accumulates WAL, retention bounds get tight, and the slot evicts. Pre-v0.80.0, this happens silently; v0.80.0 logs the slow burn 5-15 minutes before the cliff so the operator has time to act.
- **Operators with `max_slot_wal_keep_size = -1`** (unlimited; PG default) — the inactivity warning (30m) still applies and catches the consumer-disconnected class even when retention is unbounded.
- **Operators not running PG sources** — see no observable change.

## The session arc value (housekeeping note)

This is the second feature release in the v0.79.x → v0.80.x arc closing severity-a Reddit-research findings. F13 (this release) joins F12+F16 (v0.79.0 ADD COLUMN forwarding) and F18 (v0.78.x hard-delete matrix) as the closed entries. Remaining Reddit-research severity-a items: F10 (cutover sequence priming, #49), F11 (CDC schema-drift detection, #47), F17 (source-side heartbeat writer, #48). Each is independently testable and shippable.

## Cross-references

- [ADR-0059 — Pre-emptive PG slot-health pre-warning](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0059-pg-slot-health-prewarning.md)
- [F2 spill-bytes surfacing (v0.74.0)](https://github.com/sluicesync/sluice/releases/tag/v0.74.0) — the foundation this release builds on
- [Reddit research F13 catalogue entry](https://github.com/sluicesync/sluice/blob/main/docs/research/) — operator-pain source
- Bug 74 lesson: see `CLAUDE.md` § *Pin the class, not the representative* — the 18-unit-test class matrix follows this discipline

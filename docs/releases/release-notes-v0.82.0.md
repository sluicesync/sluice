# sluice v0.82.0 — Source-side sluice_heartbeat writer (F17)

**Headline:** Minor release closing the silent slot-eviction / binlog-rotation class on idle source DBs. Sluice now optionally writes a tiny periodic row to a sluice-owned table on the source; the INSERT generates WAL (PG) / binlog (MySQL) so the CDC consumer's position advances even against a quiet source. v0.80.0's F13 detected the symptom (slot accumulating WAL); v0.82.0's F17 prevents the cause.

## Added

- **`feat(pipeline/engines): F17 — source-side sluice_heartbeat writer (#48 / ADR-0061)`**

  ### Why it exists

  When a source DB has no writes for an extended period:
  - **Postgres**: the replication slot's `restart_lsn` stops advancing, the slot accumulates WAL it doesn't actually need, and PG's `wal_keep_size` / `max_slot_wal_keep_size` policies can evict the slot or fill disk.
  - **MySQL**: the binlog position the consumer is reading from gets further from current; if `binlog_expire_logs_seconds` rotates past the consumer's position, the consumer loses its ability to resume.

  Low-traffic source DBs (off-hours, weekends, dev environments) cause silent slot/binlog gaps that surface as failure cascades when the sluice consumer reconnects and finds its position has been recycled.

  ### CLI surface

  - **`--source-heartbeat-interval=DUR`** (default `0s` = disabled) — opt-in cadence. Typical value: 30s.
  - **`--source-heartbeat-prune-window=DUR`** (default 1h; 0 disables) — periodic DELETE bounds the heartbeat-table growth. At 30s cadence + 1h prune, the table holds ~120 rows steady-state.
  - **`--source-heartbeat-table-name=NAME`** (default `sluice_heartbeat`) — override for hostile DBA-managed namespaces. Pre-created tables work too.
  - **`--no-source-heartbeat`** — opt-out escape hatch (silences the permission-warning when an operator knows the source can't grant CREATE).

  ### Permission-denied path is graceful

  When the role lacks `CREATE TABLE`, the writer WARNs once and the stream continues — F17 is not fatal to the rest of sluice. The WARN names three remediation options:
  1. Grant `CREATE` to the connecting role
  2. Pre-create the table (sluice writes if it exists)
  3. Pass `--no-source-heartbeat` to silence the warning

  ### Default-OFF rationale

  The INSERT is a behavior change on the source DB. Operators on regulated systems must explicitly enable. The opt-in default is the conservative choice; the opt-out shape (`--no-source-heartbeat`) is the safety valve for operators on managed DBs / read-replicas where DDL is restricted.

## Architecture

- `ir.HeartbeatWriter` optional interface + `ir.ErrHeartbeatPermission` sentinel — `internal/ir/health.go`
- PG impl: `internal/engines/postgres/heartbeat_writer.go` — `BIGSERIAL` PK + `TIMESTAMPTZ DEFAULT NOW()` schema; prune via parameterized interval-typed DELETE
- MySQL impl: `internal/engines/mysql/heartbeat_writer.go` — `AUTO_INCREMENT` PK + `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
- Pipeline wiring: `internal/pipeline/source_heartbeat.go` — per-stream goroutine driven by `time.Ticker`, dedicated `*sql.DB`, `sync.Once`-guarded cleanup. Mirrors F13's `attachSlotHealthProbe` shape.

## Tests

- **7 pipeline-package unit tests** — loop / attachment / opt-out branches (Bug 74 "pin the class" discipline)
- **3 MySQL engine unit tests** — table-name guard, permission classifier, sentinel matching
- **5 PG integration tests** — table create + schema, accumulation, prune, WAL advancement, permission-denied surfaces sentinel
- **5 MySQL integration tests** — same matrix against binlog position
- **All 12 CI jobs SUCCESS including the `-race` Integration shards** on Linux. **-race gate (per CLAUDE.md "Concurrency chunks") verified before tag.**

## F13/F17 pairing — release-notes detail

| | F13 (v0.80.0 / ADR-0059) | F17 (v0.82.0 / ADR-0061) |
|---|---|---|
| Role | Detect | Prevent |
| Surface | WARN log when slot consumption ≥70%/85% or inactive ≥30m | Prevent slot consumption from rising in the first place |
| Default | Always-on (no opt-out) | Default-OFF (opt-in via `--source-heartbeat-interval`) |
| Cost | One background probe goroutine + `*sql.DB` per PG stream | Same shape — one writer goroutine + `*sql.DB` per stream |

Operators with both enabled get full lifecycle coverage: F17 keeps the slot/binlog moving; F13 catches the rare case where it still falls behind.

## Compatibility

- **Drop-in upgrade from v0.81.0.** Default-OFF; operators who don't set `--source-heartbeat-interval` see no observable change.
- **Minor version bump (v0.82.0)** because four new operator-facing flags are added.
- **Severity a** — prevents the silent slot-eviction / binlog-rotation class. PG operators against busy-by-day-quiet-by-night sources are the most likely audience.
- **Behavior change to flag for opted-in operators**: a new sluice-owned table `sluice_heartbeat` is auto-created on the source. ~120 rows steady-state at default 30s cadence + 1h prune.

## Who needs this

- **Postgres operators against busy-by-day-quiet-by-night sources** — the canonical F17 audience. Turn on `--source-heartbeat-interval=30s` and never see another slot-eviction surprise from off-hours quiet periods.
- **MySQL operators with aggressive `binlog_expire_logs_seconds`** — same logic, applied to binlog retention.
- **Operators not opting in** see no observable change.

## Cross-references

- [ADR-0061 — Source-side sluice_heartbeat writer](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0061-source-side-heartbeat-writer.md)
- [ADR-0059 — Pre-emptive PG slot-health pre-warning (v0.80.0 / F13)](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0059-pg-slot-health-prewarning.md) — the symptom-detection sibling

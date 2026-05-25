# ADR-0060 — Source-side `sluice_heartbeat` writer (F17)

* Status: Accepted (2026-05-25)
* Severity: A (silent-loss-class on idle sources)
* Reddit-research finding: F17 (2026-05-22 run)
* Sibling: [ADR-0059 — PG slot-health pre-warning (F13)](adr-0059-pg-slot-health-prewarning.md) — symptom-detection sibling

## Context

When a source database has no writes for an extended period — off-hours, weekends, a developer-environment source that gets used Mondays only — the CDC consumer's position never advances. Both shipping engines have failure modes that follow from that:

* **Postgres.** The replication slot's `restart_lsn` doesn't move while the WAL head keeps advancing (other system processes still write WAL — autovacuum, checkpoints, system catalog updates). The slot retains WAL between its frozen `restart_lsn` and the moving head; once that gap exceeds `max_slot_wal_keep_size`, Postgres invalidates the slot (`wal_status → 'lost'`) and the consumer loses its checkpoint. ADR-0059 (F13) lands the WARN line *when* this is about to happen; F17 prevents it from happening in the first place.
* **MySQL.** The binlog position the consumer is reading from gets further from the master's current position as the master accumulates events (DDL, system events, `mysql.gtid_executed` updates). If `binlog_expire_logs_seconds` rotates past the consumer's position, the consumer can't resume — it gets a "binary log file X has been purged" error on reconnect.

Both classes are silent-failure-shaped from the operator's perspective: the symptom (slot lost / binlog purged) surfaces only on the *next* consumer reconnect, which may be days after the actual eviction. The recovery is destructive: a full re-snapshot of the source.

The underlying mechanism is the same in both engines: the source's retention policies are time / size bounded, but the consumer's *position* doesn't advance without forward source-side activity. F17's fix is to make sluice itself the source of forward activity — a periodic INSERT into a sluice-owned table that generates WAL (PG) / binlog (MySQL) the consumer reads as progress.

## Decision

Sluice writes a tiny row into a source-side `sluice_heartbeat` table on a configurable interval. Each write generates a small amount of WAL / binlog traffic, which advances the consumer's position. The writer is opt-in (operators must pass `--source-heartbeat-interval=DUR`) and degrades gracefully when the source role lacks CREATE / INSERT privilege.

### Schema

The table is engine-neutral in concept; each engine implements the DDL that matches its idiom:

* **Postgres** — `(id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL DEFAULT NOW(), stream_id TEXT NOT NULL)` in the schema referenced by the DSN's `schema` query parameter (default `public`).
* **MySQL** — `(id BIGINT NOT NULL AUTO_INCREMENT, ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, stream_id VARCHAR(255) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4` in the connection's default database.

The `ts` column uses the server's clock (PG `NOW()` / MySQL `CURRENT_TIMESTAMP`) so the prune comparison doesn't trust the writer's clock; the `stream_id` column labels rows by originating stream so operators inspecting the table by hand know which sluice instance produced each row.

### Engine surface (IR)

A new optional `ir.HeartbeatWriter` interface, implemented on the engine's `SchemaReader` (parallel to F13's `SlotHealthReporter` on the same surface):

```go
type HeartbeatWriter interface {
    EnsureHeartbeatTable(ctx context.Context, tableName string) error
    WriteHeartbeat(ctx context.Context, tableName, streamID string) error
    PruneHeartbeat(ctx context.Context, tableName string, olderThan time.Duration) (int64, error)
}

var ErrHeartbeatPermission = errors.New("heartbeat: insufficient privilege")
```

Engines wrap permission-denied errors (PG SQLSTATE 42501 — `insufficient_privilege`; MySQL error 1142 — `ER_TABLEACCESS_DENIED_ERROR`; MySQL error 1044 — `ER_DBACCESS_DENIED_ERROR`) in `ErrHeartbeatPermission` so the pipeline wiring can `errors.Is` check the case deterministically.

### Pipeline wiring

A new `attachSourceHeartbeat` method on `pipeline.Streamer` mirrors the F13 `attachSlotHealthProbe` shape exactly:

* Opens a dedicated source-side `SchemaReader` (the CDC reader's connection lives in replication mode and isn't safe for ad-hoc INSERTs).
* Type-asserts to `ir.HeartbeatWriter`.
* Calls `EnsureHeartbeatTable` once at startup.
* Spawns a per-stream goroutine driven by a `time.Ticker` at the operator-supplied interval.
* Returns a cleanup attachment whose `Close()` cancels the goroutine and closes the dedicated reader.

The goroutine has two independent tickers — write cadence (operator-supplied) and prune cadence (fixed at 1 minute, package-variable so tests can drive it down). Each tick INSERTs / PRUNEs; transient errors WARN and continue; permission-revoked errors WARN and tear down cleanly.

### Configuration

| Flag | Default | Notes |
|------|---------|-------|
| `--source-heartbeat-interval=DUR` | `0s` (disabled) | Opt-in; typical value 30s. Zero leaves the source untouched. |
| `--source-heartbeat-prune-window=DUR` | `1h` | Rows older than this are deleted on a periodic prune pass. `0` disables prune. |
| `--source-heartbeat-table-name=NAME` | `sluice_heartbeat` | Operator override for hostile-namespace cases. |
| `--no-source-heartbeat` | `false` | Opt-out escape hatch (CLI override of YAML-configured interval). |

## Thresholds and rationale

### Why opt-in, not opt-out

F17's INSERTs are a **behaviour change on the source DB**. Even though the table is small and the writes are tiny, the operator's expectation when running sluice for the first time is "this tool reads my source." A surprise-write into a regulated database is the kind of trust violation the project's "zero users is the current reality, not a problem to rush past" tenet calls out as load-bearing. Opt-in via `--source-heartbeat-interval=DUR` makes the behaviour change explicit and visible in the operator's command line.

Sibling ADR-0059 (F13) is *passive* — it only reads `pg_replication_slots` and emits WARN logs. F13 is on by default because it never touches the source. F17 *writes*; it's off by default for that reason.

### Why 30s as the typical interval

The bound that matters is "smaller than the source's retention policy minus a reasonable margin." PG defaults to `max_slot_wal_keep_size=-1` (unlimited — no eviction by size policy), but operators on managed Postgres often set bounded values in the GB range. MySQL defaults to `binlog_expire_logs_seconds=2592000` (30 days), but managed services like RDS and Aurora often shorten this to hours. A 30-second cadence keeps the slot's `restart_lsn` / binlog position well inside any reasonable retention window without significant source-side load:

* PG heartbeat write produces ~100-200 bytes of WAL per INSERT (measured in the integration test). At 30s cadence, that's ~24 KB/hour, ~576 KB/day.
* MySQL heartbeat write produces ~150-250 bytes of binlog per INSERT (measured in the integration test). Same order of magnitude.

Both are negligible against any realistic source workload's noise floor. The cadence is operator-tunable; the documentation recommends 30s as a starting point.

### Why a 1-hour prune window default

The heartbeat table's worst-case row count is `(3600s / write_interval)` rows per hour. At 30s cadence, that's 120 rows/hour. With a 1-hour prune window the steady-state table size is ~120 rows — trivial. The window is generous enough that an operator inspecting the table via `SELECT MAX(ts) FROM sluice_heartbeat` for "when did the writer last fire?" sees a populated table; small enough that the table never bloats.

A 0 prune-window disables prune entirely (for short forensic runs); the 1h default is the production-stream value.

### Why prune cadence is fixed at 1 minute

Prune is cheap (one DELETE with a server-side date comparison). The fixed 1-minute cadence keeps the goroutine simple — a separate ticker, no derived computation. Operators tuning the write cadence don't have to also reason about the prune cadence; the package-variable lets tests drive it down without exposing a per-stream knob.

## Loud-failure discipline

Three error classes need clean handling:

1. **Insufficient privilege at `EnsureHeartbeatTable`.** The connecting role lacks CREATE TABLE on the schema. Action: WARN once with an actionable hint ("grant CREATE on the schema, pre-create the table manually, or set `--no-source-heartbeat`") and skip the writer goroutine. The stream continues without F17.
2. **Insufficient privilege at `WriteHeartbeat` (mid-stream).** The role had CREATE at startup but lost INSERT later (operator revoked, DBA rotation). Action: WARN once and tear down the writer goroutine. The dedicated source connection stays open for the streamer's lifetime so a future restart can re-evaluate.
3. **Transient errors at `WriteHeartbeat` or `PruneHeartbeat`.** Connection reset, statement timeout. Action: WARN and continue — the next tick retries. F17 is best-effort; a single missed write doesn't lose the stream.

The half-permission case (CREATE + INSERT granted, DELETE revoked) is also covered: `PruneHeartbeat` surfaces `ErrHeartbeatPermission`, the loop stops the prune ticker (so we don't keep retrying), and the write path continues. The table grows unbounded in this configuration but the CDC stream still benefits from F17's idle-source protection.

## Alternatives considered

* **Heartbeat per-replication-slot via `pg_replication_slot_advance`.** PG has a function that explicitly advances a slot's `confirmed_flush_lsn` without consuming. This works for PG only and requires REPLICATION privilege; it's a strictly-PG solution that doesn't generalize. F17's INSERT-shaped heartbeat works on both engines uniformly.
* **`UPDATE` on an existing table instead of INSERT into a new one.** Picking an existing table to UPDATE avoids the CREATE TABLE permission requirement, but it (a) couples sluice to operator-managed schemas in ways that are hard to predict, (b) generates more WAL/binlog per write (UPDATEs carry before-image + after-image vs an INSERT's single after-image), (c) creates ambiguity about which row to UPDATE on each tick. The sluice-owned table is operationally simpler.
* **Built-in INSERT loop on `sluice_cdc_state`.** The control table on the *target* has the same shape, but writing to the target wouldn't advance the source's slot/binlog position. F17 specifically needs source-side activity; target-side writes don't help.

## Implementation notes

* The MySQL writer uses a conservative table-name guard (`[A-Za-z0-9_]+`) because MySQL's backtick-quoted identifiers don't escape interior backticks. Operators can override the name but only within the safe ASCII alnum + underscore set.
* The PG writer uses the existing `quoteIdent` helper which doubles interior `"` characters per the SQL standard; no name guard is needed.
* Both engines surface the prune count via the slog DEBUG channel so a `--log-level=debug` operator can observe the prune cadence working. Production log level (INFO+) shows only the "attached" line at startup and any WARN/error.

## Tests

* **Unit tests** (`internal/pipeline/source_heartbeat_test.go`) — loop lifecycle (ticks + cancels), transient error continues, permission error terminates, prune cadence fires / is skipped on disable, half-permission stops prune ticker, attachment Close is idempotent, opt-out branches don't open source connections.
* **Engine unit tests** (`internal/engines/mysql/heartbeat_writer_test.go`) — table-name guard allowlist/rejection, permission-error classifier, IR sentinel matches through wrap.
* **PG integration test** (`internal/engines/postgres/heartbeat_writer_integration_test.go`) — table create + schema check + idempotency, row accumulation, prune drops old rows, WAL position advances after writes, permission-denied surfaces `ErrHeartbeatPermission`.
* **MySQL integration test** (`internal/engines/mysql/heartbeat_writer_integration_test.go`) — same matrix as PG but against `SHOW MASTER STATUS` binlog position.

## Out of scope

* **Per-table heartbeat.** Just stream-level. Operators wanting per-table monitoring use existing audit columns or sluice's existing per-stream metrics.
* **Prometheus metric for heartbeat-write latency.** Small follow-up if operator demand surfaces; doesn't change the F17 surface.
* **`sluice diagnose` integration.** The diagnose bundle (ADR-0056) can grow a heartbeat-table summary in a future patch; not load-bearing for F17.
* **Cross-engine fanout.** Each stream writes to its own source. A consolidation deployment (Shape A) has each source-stream running its own heartbeat against its own source — that's enough.

## Roll-out

Patch release (v0.81.0). Operators see the new flag in `sluice sync start --help`; nothing changes for operators who don't pass it. The release notes call out F17 as the opt-in idle-source protection, sibling to F13.

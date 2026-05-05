# Roadmap

Living list of work items beyond the current state, with enough context per entry that any one of them could be picked up as a self-contained chunk. Priority order is *suggested*, not strict.

Each entry has the same shape: a one-line summary, a *why* (the user-visible payoff), a *what* (load-bearing technical detail), and any *gotchas / open questions* known going in.

---

## Recently landed

For continuity when a chunk references "the previous work":

### v0.1.0 foundations

- **Simple-mode orchestrator** — three-phase apply, wired into `sluice migrate`.
- **Integration coverage in all four directions**: MySQL→MySQL, PG→PG, MySQL→PG, PG→MySQL. CI Integration job runs them on every PR.
- **MySQL CDC reader** — binlog client (go-mysql-org/go-mysql), GTID and file/pos modes, schema cache invalidated on DDL, Insert/Update/Delete/Truncate events.
- **Postgres CDC reader** — pgoutput plugin via pgx replication-mode connection, RELATION-message-driven schema cache, wal_status checks on resume.
- **MySQL VStream CDC reader** — FlavorPlanetScale, multi-shard with auto-discovery and reshard detection, snapshot+CDC handoff.
- **Snapshot→CDC handoff** — gapless cutover via `START TRANSACTION WITH CONSISTENT SNAPSHOT` (MySQL) and `EXPORT_SNAPSHOT`+`SET TRANSACTION SNAPSHOT` (PG).
- **Position persistence** — per-target `sluice_cdc_state` control table, position commit in the same tx as data writes.
- **Postgres COPY-protocol writer** — `chanCopySource` adapter wrapping pgx `CopyFrom` for ~3-5x faster bulk load on PG targets.
- **Identity sequence sync** — post-bulk `setval(pg_get_serial_sequence(...), MAX(id))` so user inserts don't collide with bulk-copied IDs.
- **`sluice sync start` / `sync status` / `sync start --dry-run`** — operator-facing CLI for streams.

### v0.2.x bug-fix and operator-UX waves

- **`sluice slot list` / `slot drop`** — operator-facing slot management; auto-drop on failed cold-start; `wal_status='unreserved'|'lost'` detection on resume.
- **Postgres slot creation with `FAILOVER true` on PG 17+** — slots survive Patroni / `sync_replication_slots` failover when configured. Warning on PG ≤ 16.
- **Translation policy fixes**: JSON wire encoding for MySQL targets (no `_binary` charset prefix), warm-resume engine alias, PG UPDATE empty-WHERE under REPLICA IDENTITY DEFAULT, TIMESTAMP precision matching on `CURRENT_TIMESTAMP` defaults, applier `CAST(? AS JSON)` for JSON-typed WHERE columns (Bug 6), composite-PK DELETE filter (Bug 8).
- **Operator docs**: `docs/postgres-source-prep.md` covers required GUCs, slot lifecycle, wal_status recovery, and the failover-survival mechanisms (Patroni `slots:`, PlanetScale "Logical slot name", PG 17 `sync_replication_slots`).

### v0.3.x feature wave

- **`sluice migrate --resume`** — resumable simple-mode migrations via per-target `sluice_migrate_state` table, phase + per-table progress tracking, truncate-and-redo for in-progress tables. See ADR-0015.
- **`sluice sync stop`** — graceful drain via control-table polling; works across machines, fits k8s lifecycle hooks.
- **`--include-table` / `--exclude-table`** — table filtering at the orchestrator boundary, glob patterns, YAML config parity.
- **Structured logging via `log/slog`** — `--log-level` actually works; bulk-copy progress lines every 2s; phase-aware error hints.
- **Composite-PK CDC regression coverage** — every direction (PG→PG, MySQL→MySQL via binlog and VStream, MySQL→PG cross-engine).
- **Generated column support** — read-side capture (`Column.GeneratedExpr` + `GeneratedStored`), write-side emission, row-path filtering so the target's GENERATED clause does the recomputation. Verbatim expression passthrough; non-portable expressions fail loudly on the target.
- **CHECK constraint support** — same shape as generated columns: schema-read capture into `Table.CheckConstraints`, DDL emission, verbatim expression passthrough. Discovered (and now strips) two more layers of MySQL stored-form decoration: charset introducers (`_utf8mb4'literal'`) and delimiter-escape forms (`\'literal\'`). Generated columns benefit from the same normalizer.
- **`--type-override TABLE.COLUMN=TYPE`** — CLI form of the YAML `mappings:` config; one-off overrides without writing a YAML file. Wholesale-precedence over YAML when both are supplied.

### v0.4.x feature wave

- **Batched CDC apply** — `--apply-batch-size N` accumulates up to N changes per target transaction, with the position write of the last change committed alongside. Default 1 keeps v0.3.x behaviour; production tuning is 100–500. Source-transaction-boundary aware flushing deferred to a follow-up. See ADR-0017.
- **Per-batch bulk-copy checkpointing** — resume mid-table from a PK cursor rather than truncate-and-redo; idempotent INSERTs tolerate the brief replay window between batch commit and cursor write; tables without a PK fall back to v0.3.0 behaviour. See ADR-0018. CLI: `--bulk-batch-size`.
- **Cross-engine expression translation for GENERATED + CHECK** — bidirectional translation pass at the writer boundary covers the common-idiom set across MySQL ↔ PG (CONCAT/||, ::cast, ~~/LIKE, ANY/IN, JSON_EXTRACT/->>). Verbatim passthrough remains the policy for unrecognized constructs. See ADR-0016.
- **Bug 9 fix** — cold-start no longer hangs on populated dest tables (pre-flight refusal + goroutine-leak fix + `--force-cold-start` escape hatch + clearer log shape).
- **Bug 11 fix** — `stop_requested_at` cleared at sync-start so a previous `sync stop` doesn't leave a sticky signal.

### Foundational ADRs (0001–0018)

IR-first, sealed interfaces, kong+koanf, three-phase apply, MySQL flavors, pgoutput, position persistence, go-mysql, Streamer-as-separate-orchestrator, idempotent applier semantics, SlotManager optional surface, pglogrepl bypass for FAILOVER, applier value-shaping with `CAST(? AS JSON)`, phase-aware error-hint registry, migration resume design, layered expression translation, batched CDC apply, per-batch bulk-copy checkpointing.

---

## Next up

The "multi-TB credibility" theme. v0.4.x has good per-row and per-batch numbers; v0.5.x should scale up to the workloads pgcopydb-class tools target.

### 1. Parallel within-table copy

**Why.** The headline feature for multi-TB datasets. v0.4.x copies each table sequentially with a single reader and writer connection; on a 16-vCPU host with a 500 GB single table that leaves 15 cores idle. pgcopydb's signature performance comes from splitting each large table into N PK ranges (typically 4–16 chunks) and copying them in parallel into separate target connections. 4–8× wall-clock improvement is realistic.

**What.** Extend the orchestrator's bulk-copy phase: for tables above a configurable size threshold (default ~100k rows or ~100 MB), split the PK range into N chunks and run N reader/writer goroutine pairs in parallel. The per-batch checkpointing infrastructure from v0.4.0 is most of the read-side plumbing — `BatchedRowReader.ReadRowsBatch` already takes a cursor; the parallel path picks N disjoint cursor ranges and runs each chunk to completion with its own checkpoint entry in `sluice_migrate_state.table_progress`.

**Design questions.**
- Range-splitting strategy: equal PK-value ranges (cheap but skewed if PKs are clustered), equal row counts (requires a sample query first), or NTILE-via-offset (one count(*) plus N OFFSET reads). Recommend NTILE-via-OFFSET for v1; same query shape across both engines and predictable splits.
- Concurrency cap: `--bulk-parallelism N` flag (default = min(8, NumCPU)). Per-target connection pool needs to accommodate.
- Composite PKs: ranges are over the leading column of the PK, with subsequent columns following naturally via the row-comparison cursor.
- Tables without a PK: fall back to single-reader sequential, same as the per-batch checkpointing fallback.

**Gotchas.**
- All N parallel readers share the same `EXPORT_SNAPSHOT` on PG so they see a consistent view. Snapshot-stream's existing snapshot setup already captures the consistent point; need to verify the snapshot is re-importable on N pinned connections.
- Identity-sequence sync (phase 3.5) runs after all parallel chunks complete. Already at the right place in the sequence.
- Resume on a partially-complete parallel copy: each chunk's cursor is independent, so resume re-runs only the chunks that didn't reach `complete`. Naturally aligns with the per-batch checkpointing model.

---

### 2. Throughput metrics — MB/s, ETA, projected completion

**Why.** For multi-TB workloads the operator's question shifts from "is it making progress?" to "will it finish before my downtime window closes?". Today the bulk-copy progress line logs `rows=N rate=R` (rows/sec); the operationally useful numbers at scale are bytes/sec and ETA-against-source-table-size.

**What.** Extend `progressTicker` to emit a richer record:
```
bulk copy progress table=foo rows=12345 bytes=2.3GB rate=85k_rows/s mbps=18 eta=14m
```
The bytes count comes from `len(scanBuf)` per row in the reader (cheap; aggregates per row); ETA needs the source table's total row count (one `SELECT COUNT(*)` at the start of each table's copy, on a separate connection so it doesn't block the streaming read). Naturally pairs with #1 since parallel chunks make the rate-and-ETA math more meaningful.

**Gotchas.**
- `COUNT(*)` on a 500 GB table can take seconds-to-minutes. Run it asynchronously alongside the first batch; the ETA appears once it's available.
- For PG, `pg_class.reltuples` is a fast estimate; trade accuracy for speed on huge tables. Document the estimate's staleness.

---

### 3. MySQL `LOAD DATA INFILE` writer

**Why.** Vanilla MySQL bulk-load via `LOAD DATA LOCAL INFILE` is typically 5–10× faster than batched INSERT. The IR already declares `BulkLoadLoadDataInfile` as a capability but no engine implements it; vanilla MySQL falls through to `BulkLoadBatchedInsert`.

**What.** A new `RowWriter` strategy in `internal/engines/mysql/row_writer.go` selected by the engine's `Capabilities.BulkLoad` field. Streams rows as TSV/CSV over the local-infile protocol; bypasses per-row INSERT parsing. Fallback to BatchedInsert remains for PlanetScale (which doesn't allow `LOAD DATA LOCAL INFILE`).

**Gotchas.**
- The MySQL server has to be configured with `local_infile=ON` (default off in 8.0+). Document the prerequisite; surface a clear error when it's not enabled and fall back to batched INSERT with a warning.
- TSV escaping for binary columns is fiddly. The existing `prepareValue` helper handles per-type shaping; the LOAD DATA path needs an analogous serialiser.

---

### 4. Source-transaction-boundary aware CDC batching

**Why.** v0.4.x's batched applier flushes on row count + Truncate. PG `Begin`/`Commit` and MySQL `XID`/`GTID` events would let the applier preserve transactional cohesion: a 5000-row source transaction commits as one 5000-row target transaction instead of 50 batches of 100. Cleaner semantics; matches what operators expect when running CDC against an OLTP source.

**What.** Surface `Begin`/`Commit`-equivalent events in the IR (currently filtered before reaching the applier). The applier flushes its in-flight batch on `Commit` and starts a new one on `Begin`; the row-count cap remains as an upper bound for huge transactions.

**Gotchas.**
- Source transactions can span multiple seconds and many MB. The row-count cap is the safety valve.
- The IR-layer plumbing for these events is its own focused chunk. ADR-0017 calls this out as the deliberate v0.4.x scope cut.

---

### 5. Memory-bounded streaming

**Why.** For huge rows (TEXT columns with megabyte-scale content, BYTEA blobs) the channel + tee + writer-batch chain can hold significant buffered memory. Need to verify there's actual backpressure at high data volumes; today the channels are unbuffered but the writer's per-batch accumulation isn't bounded by bytes.

**What.** Audit the row-streaming path for memory accumulation. Add a `--max-buffer-bytes` knob (default ~64 MB) that bounds the writer's per-batch accumulation by total byte size in addition to row count. Bytes-aware chunking matches how pscale-cli batches by ~1 MB statement body rather than row count.

---

### 6. Source-side read parallelism for PG (snapshot infrastructure)

**Why.** Prerequisite for #1 on PG sources. Multiple pinned connections need to see the same consistent snapshot. PG's `SET TRANSACTION SNAPSHOT '<name>'` already supports this, and the snapshot-stream code captures the snapshot name; need to verify the snapshot can be re-imported on N parallel reader connections.

**What.** Mostly a verification + small refactor: confirm `EXPORT_SNAPSHOT` snapshot survives across N pinned connections; if not, capture the snapshot once and pin it for the lifetime of the parallel-copy phase. Likely no code beyond plumbing the snapshot name through to N readers, but the integration test against a real PG instance is load-bearing.

---

### 7. Network compression for cross-host copies

**Why.** Lower priority. Multi-TB at gigabit is hours of pure bandwidth time. Both pgx and the MySQL driver support compression but it's not configured in our DSNs. Probably mostly a documentation update — the connection-string knob exists on both sides.

**What.** Document the `compress=true` (MySQL DSN) and `sslmode=...` + `gssencmode=...` settings on PG DSNs as a tuning recommendation for cross-host copies. Only worth real implementation work if testing surfaces it as a specific bottleneck.

---

### 8. Other latent cross-engine type edges

Tracked here so they're not forgotten; each will surface once the relevant test exercises it.

- TIMESTAMP precision differences beyond the `CURRENT_TIMESTAMP` default fix (e.g. `TIMESTAMP(6)` ↔ `TIMESTAMPTZ` round-trips).
- CHARSET/COLLATION translation across dialects.

---

### 9. PG-native types auto-emit on MySQL targets

**Why.** v0.3.x and v0.4.x refuse `Inet`/`Cidr`/`Macaddr`/`Array` from PG sources with a clear error pointing at `--type-override`. Auto-emitting `VARCHAR(N) CHECK (regex)` matches the doc-promised behaviour and removes the toil for every PG→MySQL migration that touches these types.

**What.** Wire up the policy in `internal/engines/mysql/ddl_emit.go` — when an unsupported type arrives and a sensible auto-mapping exists, emit `VARCHAR(N)` plus a CHECK constraint with a per-type regex. CHECK regex registry: `Inet` → `^[0-9.]+$|^[0-9a-fA-F:]+$` (loose IPv4/IPv6), `Cidr` → above + `/[0-9]+$`, `Macaddr` → `^[0-9a-fA-F:.-]+$`. `--type-override` continues to work as the explicit override path.

**Gotchas.**
- MySQL's REGEXP support varies. Confirm against MySQL 8.0+ (sluice's baseline).
- Document the loosened validation (regex catches gross malformation but doesn't enforce all RFC details). Operators wanting strict format checking can use `--type-override` to a tighter shape.

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 2 (per-batch checkpointing for resume). Propose a design before writing code."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.

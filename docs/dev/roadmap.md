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

### Foundational ADRs (0001–0015)

IR-first, sealed interfaces, kong+koanf, three-phase apply, MySQL flavors, pgoutput, position persistence, go-mysql, Streamer-as-separate-orchestrator, idempotent applier semantics, SlotManager optional surface, pglogrepl bypass for FAILOVER, applier value-shaping with `CAST(? AS JSON)`, phase-aware error-hint registry, migration resume design.

---

## Next up

### 1. CDC apply throughput — batched commits with idempotency

**Why.** v0.3.0's robustness testing measured the applier at ~6.5 rows/sec on PG→MySQL CDC (one source transaction with 5000 INSERTs lands on the destination at ~150ms per row). Each row is committed in its own target transaction per ADR-0010 ("idempotent applier semantics"), which is correct for resume safety but expensive at production scale where one source transaction touching thousands of rows is common.

**What.** A configurable `--apply-batch-size N` flag on `sluice sync start` (or the `Streamer.ApplyBatchSize` field for programmatic callers). When set to >1, the applier accumulates N changes and commits them in a single target transaction along with the position write of the last change. Resume idempotency is preserved: replaying any prefix of the stream still produces the same final state thanks to ON CONFLICT / ON DUPLICATE KEY UPDATE semantics that the applier already uses on Insert.

**Design questions.**
- Default value: 1 (current behavior, conservative) vs 100 (~150x throughput improvement at the cost of larger replay-on-crash window). Recommend 1 as the default with the flag as the explicit opt-in for production tuning.
- Cross-table batches: a single transaction can touch multiple source tables. Should the batch boundary follow source-transaction boundaries (preserve atomicity of the source's transactional unit), follow row count (simple but breaks transactional cohesion), or both with whichever fires first? The Begin/Commit pgoutput messages give us source-transaction boundaries on PG; MySQL binlog has equivalent BEGIN/XID/COMMIT events. Reach for source-transaction boundaries when available; fall back to row count otherwise.
- Schema-event boundaries: a Truncate or DDL event mid-batch should force a flush before the schema change applies.

**Gotchas.**
- The position written at the end of a batch must be the position of the *last applied* change in the batch, not the first. On crash + resume, replaying the tail of the batch from that position reproduces the missed changes via the applier's idempotency.
- Non-PK tables (where ON CONFLICT/ON DUPLICATE KEY isn't usable) lose idempotency on replay; document that batched-commit mode amplifies the existing no-PK caveat from ADR-0010.

---

### 2. Per-batch checkpointing for resume (bulk-copy phase)

**Why.** v0.3.0's resume truncates and re-copies any in-progress table on retry. For multi-hour copies of single huge tables, per-batch progress would let resume pick up mid-table.

**What.** Track per-table batch high-watermarks (PK-ordered LIMIT cursor or COPY byte offset) in `sluice_migrate_state.table_progress` rather than a binary in-progress / complete flag. The bulk-copy phase commits its position every N batches.

**Open questions.**
- Storage shape: extend the JSON `table_progress` to `{"users": {"phase": "in_progress", "last_pk": "...", "rows_copied": 12345}}` vs a sidecar table.
- Idempotency on resume: does the COPY/INSERT need an upsert form to handle already-copied rows from the previous attempt? Today's bulk-copy assumes empty-target; per-batch checkpointing requires ON CONFLICT or ON DUPLICATE KEY semantics on the bulk path.

See ADR-0015 for the trade-off the v1 truncate-and-redo decision settled.

---

### 3. Other latent cross-engine type edges

Tracked here so they're not forgotten; each will surface once the relevant test exercises it.

- TIMESTAMP precision differences beyond the `CURRENT_TIMESTAMP` default fix (e.g. `TIMESTAMP(6)` ↔ `TIMESTAMPTZ` round-trips).
- CHARSET/COLLATION translation across dialects.

---

## How to use this doc

When starting a new chunk in Claude Code:

1. Pick an item from "Next up". Earlier items have more context inheritance.
2. Open the relevant section in the prompt: *"Read CLAUDE.md and docs/dev/roadmap.md section 2 (per-batch checkpointing for resume). Propose a design before writing code."*
3. Iterate on the plan.
4. Implement.
5. Update this file when the chunk lands — move the entry to "Recently landed" and trim it to one line.

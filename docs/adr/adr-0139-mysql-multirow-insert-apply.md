# ADR-0139: Multi-row INSERT coalescing for MySQL CDC apply (Bug 169)

## Status

**Accepted (2026-06-28).** Fixes Bug 169 — found by the v0.99.153 PlanetScale target phase. The MySQL counterpart to ADR-0092/0138 (which made the Postgres apply path RTT-independent). Scope: the MySQL applier's batched apply (single-lane batch loop + ADR-0104 concurrent lanes).

## Context

ADR-0092/0138 made Postgres CDC apply RTT-independent by pipelining each batch onto one `pgx.Batch`/`SendBatch` (one round trip per batch, regardless of N). MySQL has **no pipelining primitive** — both its single-lane batch loop and its concurrent lanes (ADR-0104) dispatch one serial `tx.ExecContext` per change, so a batch of N changes is N network round trips. On a LAN that is invisible; over WAN it caps apply at `lanes × (1/RTT)`.

### Bug 169 (the cost — measured)

The PlanetScale target phase (sluice 0.99.153, SQLite→PS, real WAN) measured, on the **same 317,613-change backlog**:

- **Postgres (pipelined): ~5,000 changes/s** — drains in ~60s.
- **MySQL default config: effective STALL (~0.2/s).** Worse than merely slow: serial apply over WAN is so slow that a batch cannot commit inside PlanetScale **Vitess's 20-second transaction killer**, so Vitess kills the txn, the AIMD controller thrashes (1000 → … → 62), and almost no work commits durably.
- **MySQL, kill-free (tiny time-bounded batches): ~20/s** (~36/s at `--apply-concurrency 8`) — ~100× below PG, ~130× below the ~2,647/s source rate.

So continuous sync to a MySQL target over WAN (PlanetScale, or any cross-region MySQL) is non-viable for write-heavy workloads. The Vitess-killer stall is a *consequence* of the slow apply (a batch that can't finish in 20s) — raising apply throughput so a batch commits quickly also resolves the stall.

### Why not the Postgres approach

`pgx.Batch` is a pgx protocol feature; the MySQL wire protocol has no equivalent client-side statement pipelining, and `go-sql-driver/mysql` does not expose one. The two MySQL options are:

1. **Multi-statement** (`multiStatements=true`): pack N `;`-separated statements into one Exec. But parameterised multi-statement requires `interpolateParams=true` (client-side value interpolation), which **changes the value-encoding path** — exactly the Bug-74-class value-fidelity hazard. Rejected for the first cut.
2. **Multi-row INSERT** (`INSERT … VALUES (…),(…),… AS new ON DUPLICATE KEY UPDATE …`): pack N same-table, same-column-shape inserts into ONE statement, **still parameterised** (each value bound to a `?` exactly as today). One round trip for a run of N inserts. Value encoding is **byte-identical** to the current single-row path (same `prepareApplierValue` → `?` binding; only more placeholder groups per statement), so the Bug-74 hazard does not apply. This is the standard MySQL bulk technique and the safe choice.

Insert-heavy bulk traffic (the dominant CDC cold-catch-up and bulk-load shape) benefits fully; UPDATE/DELETE-heavy streams keep the serial cost for now (a future multi-statement escalation, with its own value-fidelity review, is the follow-up if measurements demand it).

## Decision

**Coalesce consecutive, same-table, same-column-shape, keyed INSERTs into one parameterised multi-row `INSERT … ON DUPLICATE KEY UPDATE` statement**, in both MySQL apply paths.

### The accumulator (`mysqlBatchTx`)

Mirror the PG `*pgxBatchTx` structure: `BeginTx` returns a `*mysqlBatchTx` (wrapping the `*sql.Tx`, satisfying `appliershared.BatchTx`'s `Rollback()`), which buffers a **pending run** of coalescable inserts:

- **`dispatch(change)`**:
  - An Insert that is **coalescable** (keyed table — not ADR-0089 keyless; same `schema.table` and identical ordered column set as the current pending run) is appended to the pending run (no round trip).
  - Any **non-coalescable** change (Update, Delete, Truncate, SchemaSnapshot, a keyless-table Insert, an Insert to a different table, or one with a different column set) first **flushes the pending run** (emit the multi-row INSERT — preserving apply order), then executes that change serially via the existing `dispatch`/`txExec`.
- **`flushPending()`**: emits `buildMultiRowInsertSQL(...)` for the buffered rows in one `ExecContext`, then clears the run. Bounded by **rows-per-statement and byte caps** (see below) — when the pending run hits the cap it auto-flushes and starts a new run.
- **`WritePosition`** flushes the pending run **before** writing the position (all data durable before the position row), then writes the position on the tx.
- **`Commit`** flushes any remaining pending run, then commits under the Bug-56 watchdog.
- **`Rollback`** discards the buffer and rolls back the tx.

### The concurrent lane path (ADR-0104)

`laneApplierAdapter.ApplyLaneBatch` applies its `[]ir.Change` through the **same coalescing logic** (consecutive same-table/same-shape keyed inserts → multi-row INSERT; flush before any serial change; flush at end before commit). A lane's batch is key-hashed (same key → same lane), so its inserts are distinct-PK rows of a small set of tables — coalescing is highly effective. Encoding reuses `buildMultiRowInsertSQL` (identical to the serial path).

### Load-bearing correctness invariants

- **Apply order preserved.** Only *consecutive* coalescable inserts are grouped; the pending run is flushed before any non-insert change, table switch, or column-shape change, so the at-the-target apply order matches the source change order. Within one multi-row INSERT, MySQL applies the VALUES list left-to-right for `ON DUPLICATE KEY UPDATE`, so two same-PK inserts in one run resolve last-wins — identical to serial.
- **Value fidelity unchanged (the Bug-74 reason it's safe).** `buildMultiRowInsertSQL` reuses `NonGeneratedRowKeys` + `prepareApplierValue` per cell and binds every value to a `?`; the wire encoding of each value is byte-identical to the single-row path. No interpolation, no codec change. The value-fidelity-reviewer re-derives the family matrix anyway, but the binding path is provably the same.
- **Idempotency (ADR-0010) preserved.** The multi-row form keeps the row-alias `AS new ON DUPLICATE KEY UPDATE` clause, so replay of any prefix reproduces the same final state — the resume contract is unchanged.
- **Keyless stays single-row (ADR-0089).** A keyless-table Insert is never coalesced (the at-least-once keyless guard already forces batch-of-1); it flushes the pending run and applies alone.
- **Column-shape grouping.** Rows in one multi-row INSERT must share an identical ordered column list (`NonGeneratedRowKeys`); a row whose present-column set differs flushes the run and starts a new one. (CDC inserts for a table normally carry the full row, so runs are long in practice.)
- **Caps.** A pending run flushes when it reaches a rows-per-statement cap or would exceed the existing per-batch byte cap, so a single statement never exceeds `max_allowed_packet`. The overall batch transaction is still bounded by the AIMD batch size + the byte cap, which — with the throughput now high enough to commit quickly — keeps commits inside Vitess's 20s killer.

### Vitess transaction-killer interaction

The multi-row throughput gain is what resolves the stall: batches now commit fast enough to land inside the 20s window. No separate time-cap is introduced in this cut; if a residual killer interaction is observed, a per-tx time budget is a follow-up. The AIMD controller is unchanged.

## Consequences

- MySQL CDC apply throughput over WAN rises from ~lanes/RTT to roughly batch-size/RTT for insert-heavy traffic, making continuous sync to PlanetScale-MySQL / cross-region MySQL viable; the Vitess-killer stall is resolved as a side effect.
- UPDATE/DELETE-heavy streams keep the serial per-row cost for now — a documented limitation; multi-statement (with a value-fidelity review) is the follow-up if demanded.
- The single-lane and concurrent-lane paths share `buildMultiRowInsertSQL` and the coalescing helper, one source of truth.
- This is a concurrency change (the lane path) → the `-race` integration gate runs before any tag (concurrency-chunk rule), and the value-fidelity-reviewer reviews the multi-row builder before it lands.

## Validation

Pinned by: unit tests for `buildMultiRowInsertSQL` (N-row SQL shape, arg flattening, `AS new` clause, single-row equivalence) and the coalescing state machine (flush on table-switch / column-shape-change / non-insert / keyless / cap, order preservation); integration tests proving exactly-once + full value-family fidelity under a mixed insert/update/delete workload on both the single-lane and concurrent paths (differential vs the serial path — byte-identical target state); and a re-measure on the cross-region rig (the PlanetScale-MySQL apply throughput should rise from ~20/s toward the PG band for insert-heavy traffic).

# sluice v0.99.154

**Fixes Bug 169: continuous CDC apply to a MySQL target was bottlenecked by network round-trip latency — over a high-latency link (cross-region, or a managed target like PlanetScale over the public internet) it couldn't keep up and, on PlanetScale's Vitess, stalled behind the 20-second transaction killer. MySQL apply now coalesces consecutive inserts into one multi-row statement, making insert-heavy apply round-trip-efficient. This is the MySQL counterpart to the v0.99.153 Postgres fix. No data loss in any version (exactly-once always held); this is an apply-throughput / convergence fix.**

## Fixed

**HIGH — continuous CDC apply to a MySQL target was round-trip-bound over high-latency links and stalled behind PlanetScale Vitess's transaction killer (Bug 169).** Found by the v0.99.153 PlanetScale target phase: on the same 317,613-change backlog over real WAN, a Postgres target drained at ~5,000 changes/s (the pipelined v0.99.153 fix) while a MySQL target managed an *effective stall* (~0.2/s) in its default config and only ~20/s when capped kill-free — roughly 100× slower. There was **no data loss** — exactly-once held throughout; this is purely an apply-throughput / convergence ceiling.

**Root cause.** Unlike Postgres — which pipelines a whole batch onto one round trip — the MySQL applier dispatched one serial statement per change in both its single-lane batch loop and its concurrent key-hash lanes, so a batch of N changes was N network round trips, capping apply at `lanes × (1/RTT)`. Over WAN that is slow enough that a batch can't commit inside PlanetScale Vitess's 20-second transaction killer, so Vitess kills the transaction and the adaptive batch-size controller thrashes, making almost no durable progress.

**Fix.** MySQL has no client-side statement-pipelining primitive (the protocol feature Postgres's driver uses), so apply now **coalesces consecutive same-table, same-shape, keyed INSERTs into one parameterised multi-row `INSERT … VALUES (…),(…) AS new ON DUPLICATE KEY UPDATE …`** — one round trip for a run of N inserts instead of N — in both the single-lane and concurrent-lane paths. The throughput gain is what resolves the Vitess-killer stall: batches now commit fast enough to land inside the 20s window. Insert-heavy CDC (bulk catch-up and load) benefits fully; UPDATE/DELETE-heavy streams keep the serial per-row cost for now (a future enhancement, tracked in ADR-0139).

**Why it's safe (value fidelity).** The multi-row form changes only *how many* placeholder groups are in one statement, never *how* a value is encoded: every cell is still bound to a `?` through the same value codec the single-row path used — no client-side interpolation, no per-type dispatch. So the wire encoding of every value is byte-identical to before. Apply order is preserved (the pending insert run is flushed before any non-insert change, table switch, or column-shape change; within one statement MySQL resolves same-key rows last-wins, identical to serial), idempotent replay (the row-alias `ON DUPLICATE KEY UPDATE` clause) is unchanged, keyless-table inserts are never coalesced (they keep the at-least-once single-row guarantee), and a run auto-flushes at a 1 MiB / 60,000-placeholder cap so one statement never exceeds `max_allowed_packet`.

## How it was found

The **PlanetScale target phase** of the SQLite/D1 test program — a deliberate test of continuous sync to managed MySQL and Postgres targets over real WAN latency, which measured the MySQL-vs-Postgres apply gap directly on the same backlog. (The Postgres half had been made round-trip-independent in v0.99.153.)

## Compatibility

Behavior-preserving for correctness — exactly-once, value fidelity, apply order, idempotent replay, and the keyless guarantee are all unchanged. Pinned by unit tests (the multi-row builder's shape + row-major argument flattening + single-row byte-equivalence, and the coalescing state machine's flush boundaries) and an integration differential that applies a mixed insert/update/delete workload over the full value-family matrix — decimals, JSON, bool→TINYINT(1), VARBINARY with embedded NUL/0xFF, microsecond timestamps, big integers beyond 2^53, and NULL mixed with non-NULL inside a single coalesced statement — through the serial (oracle), single-lane-coalescing, and 4-lane concurrent paths, asserting byte-identical target state. The `-race` integration gate passed before tagging (concurrency change). MySQL-target only; Postgres-target apply is unchanged.

## Who needs this

Anyone running **continuous CDC sync to a MySQL target over a high-latency link** — cross-region replication, or syncing to a managed MySQL/Vitess (PlanetScale) over the public internet — where, before this, insert-heavy apply was capped at roughly `lanes / round-trip-time` and, on Vitess, could stall entirely. On a LAN nothing changes.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.154 · **Container:** ghcr.io/sluicesync/sluice:0.99.154
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

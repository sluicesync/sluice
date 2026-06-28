# sluice v0.99.153

**Fixes Bug 168: continuous CDC apply was bottlenecked by network round-trip latency, so a high-latency sync (cross-region, or a managed target over the public internet) could not keep up under load — the replication lag diverged even though throughput was fine on a LAN. The default concurrent apply path now pipelines each lane, making apply throughput independent of round-trip time on Postgres targets. No data loss in any version (exactly-once always held); this is an apply-throughput / convergence fix for high-latency continuous sync.**

## Fixed

**HIGH — continuous CDC apply was RTT-bound over high-latency links; cross-region / WAN sync lag diverged under load (Bug 168).** Found by the v0.99.152 Vultr cross-region test round (New Jersey ↔ Amsterdam, 80.9 ms RTT): a `sqlite-trigger` → Postgres continuous sync pinned at **~45–63 changes/s** of steady-state apply *regardless of `--apply-batch-size`*, and under a ~3000 ops/s source workload the replication lag **diverged** (a 780k-change backlog that never drained). There was **no data loss** — exactly-once held throughout; this is purely an apply-throughput / convergence ceiling on high-latency links (cross-region, and to a milder degree managed targets such as PlanetScale at ~7 ms).

**Root cause.** The concurrent key-hash apply lanes (the **default** apply path, four lanes) dispatched each change as its own network round trip inside the lane transaction, so apply throughput was capped at `lanes × (1/RTT)` ≈ 4 × 12.5/s = 50/s — the lane transaction amortised only the commit fsync, not the per-row execs, which is exactly why batch size did not move it. v0.99.x's earlier single-lane pipelined-apply work (ADR-0092) had already solved this for the single-lane batch path (queue every statement onto one pipelined batch flushed in a single round trip, independent of batch size) but that pipelining was never carried into the concurrent lanes. On a LAN (~0.1 ms RTT) the per-row round trip is invisible — which is why it surfaced only cross-region, and why the local 10–20 GB at-scale runs (low RTT) never caught it.

**Fix (Postgres).** Each concurrent lane now applies its batch through the same pipelined path — the lane pool is opened in `DescribeExec` mode and the lane queues every change onto one batch flushed in a single round trip (one round trip per lane batch instead of one per row), so apply throughput is governed by batch size and lane count rather than round-trip time. Value encoding is byte-identical to the serial path because the lane reuses the exact same statement builders and value codec the single-lane pipelined path uses (the value-fidelity differential pins, exercised across the full type-family matrix, now run through the pipelined lane path). Exactly-once is untouched — only the in-lane transport changed; each lane still commits the same change set atomically, and the frontier checkpoint and warm-resume are unchanged. The serial per-row dispatch is preserved as a loud, one-time-WARN fallback if the pipelined connection escape is ever unavailable.

## How it was found

The **Vultr cross-region** leg of the SQLite/D1 large-scale test program — a deliberate test of continuous sync over real wide-area latency (80.9 ms), which the prior local at-scale runs (sub-millisecond RTT) could not exercise. The same round also confirmed, over real WAN, that parallel bulk copy does not out-scale a single stream (COPY is bandwidth-bound, not RTT-bound) and that big-integer (> 2^53) and BLOB fidelity from a live Cloudflare D1 source stays byte-exact end-to-end.

## Compatibility

Behavior-preserving for correctness — exactly-once, value fidelity, and warm-resume are untouched (verified by the concurrent exactly-once / dependent-ordering / warm-resume / full-family differential integration pins now running through the pipelined lanes, plus a new pin asserting the pipelined path is actually taken so a silent regression to serial can't pass unnoticed). The change is Postgres-target only. The `-race` integration gate passed before tagging (concurrency change).

**Note on MySQL targets:** the MySQL applier has no pipelined-apply primitive (both its single-lane and concurrent paths dispatch serially), so MySQL CDC apply remains round-trip-bound over high-latency links. Bringing it to parity needs a new MySQL pipelining primitive with its own value-fidelity review and is tracked as a separate follow-up (ADR-0138). On a LAN / low-latency link MySQL apply is unaffected.

## Who needs this

Anyone running **continuous CDC sync to a Postgres target over a high-latency link** — cross-region replication, or syncing to a managed Postgres over the public internet — where, before this, apply throughput was capped at roughly `lanes / round-trip-time` and the replication lag could diverge under a busy source. On a LAN nothing changes.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.153 · **Container:** ghcr.io/sluicesync/sluice:0.99.153
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

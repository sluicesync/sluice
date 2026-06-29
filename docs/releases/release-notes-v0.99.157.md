# sluice v0.99.157

**Fixes the UPDATE/DELETE tail of Bug 169: continuous CDC apply to a MySQL target was round-trip-bound for update/delete-heavy workloads over high-latency links (the v0.99.154 fix only covered inserts). UPDATEs and DELETEs now coalesce too, so update/delete-heavy continuous sync to MySQL over WAN — including managed MySQL/Vitess like PlanetScale — keeps up instead of crawling. No data loss in any version (exactly-once always held); this is an apply-throughput / convergence fix.**

## Fixed

**HIGH — UPDATE/DELETE-heavy MySQL CDC apply was round-trip-bound over high-latency links (Bug 169 tail; ADR-0140).** v0.99.154 made INSERT-heavy MySQL apply round-trip-efficient by coalescing inserts into one multi-row statement, but UPDATEs and DELETEs still dispatched one serial round trip each, so any non-trivial update/delete fraction over WAN collapsed to roughly `lanes / round-trip-time`. A dedicated two-tier characterization measured the cost: local latency-injection put a 50/40/10 insert/update/delete mix at ~28/s (80 ms RTT) vs thousands/s for inserts; on real PlanetScale-MySQL (~101 ms) pure update/delete managed ~37/s, and on the default config the Vitess 20-second transaction-killer fired (a lane's serial U/D batch needs hundreds of round trips, far over 20 s) and the adaptive back-off recovered with a ~115 s dead-window plus a sawtooth (~13/s). It converged and exactly-once held throughout — purely throughput/convergence.

**Fix.** UPDATEs and DELETEs now coalesce on the MySQL target, **parameterised** (no client-side interpolation — value encoding is unchanged, the same property that made the INSERT fix safe). A keyed, non-PK-changing UPDATE is applied as the *same* multi-row `INSERT (after-image) … ON DUPLICATE KEY UPDATE` upsert the INSERT path already uses (the row exists in a valid CDC stream, so MySQL takes the update branch — identical end state, keyed by primary key); consecutive DELETEs coalesce into one `DELETE … WHERE pk IN (?, …)` (single-column PK) or the row-value tuple `DELETE … WHERE (a, b) IN ((?, ?), …)` (composite PK). Round trips per lane-batch drop from one-per-row to ~one, so update/delete-heavy apply moves toward the batched band — and because batches now commit fast, the Vitess transaction-killer stops firing.

**A behaviour change worth noting:** this changes keyed UPDATE/DELETE matching from the prior full-before-image `WHERE` to primary-key-based. That is correct and self-healing for a keyed CDC stream — the source's intent is "this key's row becomes <after>" / "this key's row is gone" — and sluice never treated a zero-row UPDATE/DELETE as an error, so nothing that depended on the full-row match is lost. The cases where it would differ stay on the serial full-before-image path: **keyless tables** (no primary key to key on — also the at-least-once contract), **PK-changing UPDATEs** (upserting at the new key would orphan the old-key row), and non-row events. Apply order is preserved (a switch between an upsert-run and a delete-run flushes the pending run first; within one upsert statement MySQL resolves same-key rows last-wins = source order), and idempotent replay is unchanged.

## How it was found

The "update/delete-heavy mixed workload over WAN" tail flagged after the v0.99.155 PlanetScale-MySQL round, then quantified with a purpose-built Tier 1 (local latency-injection) + Tier 2 (real PlanetScale-MySQL) characterization that confirmed the ~100× gap and the Vitess transaction-killer interaction.

## Compatibility

Behavior-preserving for correctness — exactly-once, value fidelity, apply order, idempotent replay, and the keyless guarantee are all unchanged. **MySQL target only** — Postgres already pipelines UPDATE/DELETE onto one `pgx.Batch` round trip (ADR-0092), so it never had this tail. Pinned by unit tests (the `DELETE … IN` builder for single + composite PKs; the coalescer state machine), an integration differential applying a mixed insert/update/delete workload over the full value-family matrix serially (the oracle) vs single-lane-coalescing vs 4-lane concurrent and asserting byte-identical target state with the coalescing actually engaged, a composite-primary-key (VARBINARY / DECIMAL-as-text / VARCHAR) `DELETE … IN` differential against real MySQL, and targeted pins for delete-then-update (no resurrection), PK-changing update (serial, no orphan), keyless update/delete (serial), and idempotent replay. The `-race` integration gate passed before tagging (concurrency change).

## Who needs this

Anyone running **update/delete-heavy continuous CDC sync to a MySQL target over a high-latency link** — cross-region replication, or a managed MySQL/Vitess such as PlanetScale over the public internet — where, before this, update/delete apply was capped at roughly `lanes / round-trip-time` and (on Vitess) paid a recurring transaction-killer tax. On a LAN nothing changes; insert-heavy workloads were already fast since v0.99.154.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.157 · **Container:** ghcr.io/sluicesync/sluice:0.99.157
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md

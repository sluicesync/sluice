# ADR-0120: Relax `MinimizeSkew` on the multi-shard VStream concurrent-apply path

## Status

**Proposed (2026-06-25).** Roadmap item 27. A throughput optimization on the
multi-shard (Vitess/PlanetScale) VStream CDC apply path: let both shards stream
and drain concurrently during an apply-deficit backlog instead of vtgate holding
the ahead shard back to keep the merged stream commit-time ordered.

**CRITICAL-ordering surface, design-gated.** This touches the CDC ordering +
resume-position contract — a silent-loss class if an assumption is wrong. The
consumer audit below (mandated by the roadmap) found **no** consumer that depends
on cross-shard commit-time ordering, so the relaxation is correctness-safe. It is
nonetheless gated on a **live A/B on a multi-shard Vitess source** + a `-race`
integration pass before the *default* is changed. `MinimizeSkew` is a
**source-side vtgate flag**, so the A/B needs only a multi-shard Vitess source
(the self-hosted `vitessreshard` 2-shard local cluster, or a Vultr-hosted Vitess
for scale) feeding ANY target (the target is throttled to form the backlog);
real PlanetScale is **not** required — it would only add cross-region/scale
realism to the headline numbers. The implementation lands as an **opt-in flag whose default
preserves today's behavior** (`MinimizeSkew=true`), so landing it changes nothing
until an operator opts in; flipping the default is a separate, A/B-gated step.

## Context

The VStream request sets `MinimizeSkew=true` (`cdc_vstream.go:894`), so vtgate
holds the *ahead* shard's delivery to keep the merged multi-shard stream
commit-time ordered. Under an apply-deficit (a backlog where apply < source for a
while) this becomes a throughput cap (roadmap item 23 Phase-A RCA, live-confirmed
on v0.99.81): the ahead shard's delivery is frozen until the behind shard fully
drains — observed as shard `80-` frozen for many minutes while `-80` drained at
~123 GTID/s. The merged ordering buys correctness only if some consumer relies on
cross-shard commit order. Range-sharding guarantees a given `(table, PK)` lives
in exactly ONE shard, so there is no cross-shard same-key dependency; the
concurrent key-hash apply lanes (ADR-0104) already serialize same-key changes
within a lane; and `StopOnReshard=true` closes the only window (a reshard) where
a key could transiently exist on two shards.

## Decision

### 1. Opt-in flag, default unchanged

Add `--vstream-relax-skew` (and the matching DSN param `vstream_relax_skew`),
default **false** = today's `MinimizeSkew=true`. When set, the steady-state CDC
VStream request (`buildVStreamRequest`, `cdc_vstream.go:894`) is built with
`MinimizeSkew=false`, so vtgate delivers both shards concurrently. The flag is
**opt-in-named** (relax = the non-default action), so the Go zero value preserves
the proven behavior for every construction — the inverse of the v0.99.51 trap, by
the same reasoning: the safe/common default is the zero value. Plumbed CLI →
package override + source-DSN param, mirroring the ADR-0118
`--vstream-copy-table-parallelism` precedent (explicit CLI value wins over the
DSN param, which keeps working verbatim).

**Not gated on the apply lane count.** The roadmap framed the relaxation as "when
concurrent apply is engaged," but the consumer audit (§2) shows correctness is
*independent* of apply concurrency — same-key changes live in one shard and are
ordered within it regardless of cross-shard interleave, so even serial apply is
safe with skew relaxed. Gating the source-side flag on the target-side lane count
would add a cross-layer coupling (the reader does not otherwise know the apply
degree) for zero safety benefit, so the flag controls `MinimizeSkew` directly. The
*benefit* is largest with concurrent apply (the default since ADR-0106), which is
the intended pairing and what the help text recommends; a serial-apply operator
who sets it gets relaxed delivery with little upside but no correctness risk.

Scope: only the steady-state CDC stream. The COPY-phase streams
(`cdc_vstream_snapshot.go:323/1345`, `cdc_vstream_copy_concurrency_pump.go:299`)
keep `MinimizeSkew=true` — the cold-copy snapshot wants the shards synchronized at
the cut, and item 27's win is purely the backlog-drain latency on the *tail*, not
the copy. (A future amendment may revisit the copy path if a copy-phase A/B
warrants it.)

### 2. Why it is correctness-safe (the consumer audit, roadmap-mandated)

Every consumer of the multi-shard VStream tracks per-shard state independently and
makes **no** cross-shard commit-time-ordering assumption. Audited 2026-06-25:

| Consumer | Location | Cross-shard ordering assumption | Verdict |
|---|---|---|---|
| Merged-stream pump / dispatch | `cdc_vstream.go:965–1046` | none — shard id used only for the field cache, never for ordering; `ir.Change` carries no shard | SAFE |
| Position orderer | `position_orderer.go:160–218` (`vstreamPositionAtOrAfter`) | per-shard GTID-set containment — a **partial** order; divergent per-shard GTIDs are simply unordered, never mis-compared | SAFE |
| Lane apply router | `laneapply/router.go:50–62` | routes by `(table, PK)` key-hash only; shard id isn't available, so same key → same lane regardless of shard | SAFE |
| Frontier / checkpoint | `laneapply/frontier.go:32–151` | advances by **source-sequence** contiguity and records tx-boundary positions; not per-shard order | SAFE |
| Snapshot→CDC stitch | `cdc_vstream_snapshot_stitch.go:58–156` | per-shard GTID-set **minimum** (intersection), computed independently per shard | SAFE |
| Per-shard watchdog | `cdc_vstream_shard_progress.go:83–266` | already built to *tolerate* asymmetric advancement (it distinguishes a real wedge from the MinimizeSkew hold) — relaxing skew only makes the "peer advanced" signal more often legitimately true | SAFE (see §3) |
| Resume reachability | `cdc_vstream.go:756–804` | verifies each shard against **its own** `@@global.gtid_purged`; already handles widely-divergent per-shard positions (incl. one shard mid-COPY, another pure-CDC) | SAFE |

The position token is a sorted `[]shardGtid` (`encodeVStreamPos`,
`cdc_vstream_position.go:151`); per-shard positions are independent, and resume
re-establishes each shard from its own stored GTID. The only operational change
is that checkpoint boundaries are recorded **more frequently** (a VGTID advances
whenever either shard commits, rather than only when the held shard is released) —
benign, and slightly better for resume granularity.

### 3. The one nuance worth pinning: the stall watchdog

`cdc_vstream_shard_progress.go` WARNs when shard S is stale *and a peer is fresh*,
explicitly noting that signature ALSO matches a normal `MinimizeSkew` catch-up
hold (so the WARN is advisory). With skew relaxed, a genuine apply-deficit no
longer freezes the ahead shard, so the "S frozen while peer advances" signature
becomes a *more reliable* wedge indicator (less benign-hold noise). No code change
is required, but the WARN message's "or a normal MinimizeSkew catch-up hold" hint
should be made conditional on the flag (drop that clause when skew is relaxed) so
the operator isn't pointed at a non-existent cause. Pin: a unit test asserting the
request carries `MinimizeSkew=false` exactly when (flag set ∧ lanes_W>1), and the
watchdog message variant.

## Consequences

- **Win (when opted in):** an already-formed multi-shard backlog drains both
  shards concurrently instead of serializing behind the slowest shard — the
  catch-up latency improvement item 27 targets. Item 23(c) already prevents the
  pathology from *forming* in steady state, so this is a catch-up-latency win, not
  a steady-state one (hence opt-in, not default-on, until the A/B).
- **Cost:** none at the default (flag off → byte-identical request). When on,
  marginally more checkpoint writes (benign).
- **Default unchanged on landing.** Flipping the default to relaxed is a separate
  step gated on the live A/B (below). This ADR lands the mechanism + the safety
  proof; it does not change anyone's behavior.
- **Not changed:** the COPY-phase MinimizeSkew, the position token format, the
  per-shard resume contract, the lane-apply exactly-once, `StopOnReshard`.

## Validation

- **Unit (lands with the flag):** `buildVStreamRequest` carries `MinimizeSkew=false`
  iff the relax-skew flag/DSN is set, else true; the override-wins-over-DSN
  resolution; the COPY-path requests stay `true` unconditionally; the watchdog WARN
  message variant.
- **`-race` integration (CI, before any default flip):** the existing
  vitesscluster / vstream suites with the flag set, asserting exactly-once
  convergence (src==dst checksum) under concurrent multi-shard apply with skew
  relaxed — same coverage the default path has, proving the relaxation doesn't
  regress correctness on the local Vitess-24 cluster.
- **Live A/B (local run done 2026-06-25 — correctness confirmed, throughput delta pending cross-region scale).** A gated A/B test (`vitess_cluster_reshard_relax_skew_integration_test.go`, `//go:build integration && vitessreshard`) reshards a `vitess/lite` cluster `-`→`-80,80-` and runs the production reader twice (`vstream_relax_skew` unset vs `=true`), under a fast cross-shard writer + a throttled consumer. **Result:** exactly-once held in BOTH runs (delivered set == source committed set; 0 gap, 0 dup, 0 value mismatch) — the load-bearing correctness check, confirming the relaxation is safe under concurrent multi-shard apply. The *hold* did **not** reproduce on a single-host cluster (both shards drained concurrently regardless of the flag, ≤1 s frozen streaks) because the two tablets commit with near-identical timestamps, so `MinimizeSkew` has no cross-shard temporal skew to act on. The documented hold (item 23: shard `80-` frozen for minutes) is a real cross-region/scale phenomenon, exactly as anticipated below — so the throughput delta still needs either an engineered-skew harness or a cross-region source (Vultr-Vitess or PlanetScale). The default stays off pending that throughput confirmation.
- **Live A/B (REQUIRED before flipping the default):** measure both-shard drain
  time with `MinimizeSkew` on vs off on a **multi-shard Vitess source with a
  pre-formed apply-deficit backlog**. Because `MinimizeSkew` is a source-side
  vtgate flag, this needs only a multi-shard Vitess source — the self-hosted
  `vitessreshard` 2-shard (`-80`/`80-`, hash VINDEX) local Docker cluster, or a
  Vultr-hosted Vitess for more scale — feeding ANY target (a local PG/MySQL, or a
  non-sharded PlanetScale MySQL). The backlog is induced by a fast cross-shard
  writer plus a throttled/slowed target (e.g. a toxiproxy bandwidth toxic, the
  same technique the `benchmarks/cdc` soak uses). Real multi-shard PlanetScale is
  NOT required for the A/B; it would only sharpen the cross-region/scale realism
  of the headline numbers (the original Track-B v0.99.81 finding's setting). This
  A/B is the gate between "flag landed, default off" and "default flipped to
  relaxed."

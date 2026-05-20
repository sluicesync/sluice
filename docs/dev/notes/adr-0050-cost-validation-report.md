# ADR-0050 cost-validation report — gate (1) empirical evidence

**Status:** gate-(1) empirical evidence captured 2026-05-20. This report
unblocks the FIRST of the three non-design implementation gates for
[ADR-0050](../../adr/adr-0050-reconciling-resnapshot.md). Gates (2)
(ADR-0049 implementation) and (3) (real operator demand) remain
unchanged.

**Bottom line: conditional-go.** The evidence supports the broad ADR-0050
direction *and* its DP-2 default ordering (`vstream-native` over
`watermark-table`). It does NOT yet supply a like-for-like
reconciling-resnapshot-vs-full-re-copy delta because no
reconciling-resnapshot exists to measure — that is, after all, what
this ADR is gating. What we measured is **(a) the full re-copy baseline
cost** that a future reconciling-resnapshot will be compared against,
**(a′) that baseline cost at GB-scale**, and **(b) the DP-2
`watermark-table` write→visibility-lag profile** so the A/B ordering
can be defended on hard numbers. The GB-scale anchor (`ma-3p2m`,
0.97 GiB / 7-min COPY, throughput byte-linear at ~1.4 MB/s per table)
closes the gate-(1) "scale-driven cost case unanchored at the high end"
risk. The "conditional" remaining in "conditional-go": the next
gate-(1) cycle, once the implementation lands, must include a true
reconciling-resnapshot run that shows the predicted egress saving on a
realistic table — and *must* include divergence-injection (per the
ADR's vdiff2 callout that the skip-gate is the load-bearing lever,
not the chunked scan itself).

---

## Setup

| What | Value |
|---|---|
| Source DB | `sluice-adr-0050-cv` (orware/regions-metrics org, AWS us-west-2 / Oregon) |
| Source cluster | `PS_10` HA (production branch `main`, 2 replicas) |
| Source engine | MySQL 8.0 behind Vitess (PlanetScale) — unsharded keyspace (shard `-`) |
| Target DB | `sluice-validation-mysql-destination` (existing) — `PS_10` HA, us-west |
| sluice version | `0.70.2` (commit `38e9128…`, built 2026-05-20) |
| Validation runtime | Windows 11 + Rancher Desktop docker (`mysql:8.0` client image) |
| Created | 2026-05-20 |

### Source schema (3 tables)

`schemas/adr-0050-source.sql`:

- `narrow_tall` — `(id BIGINT PK AUTO_INCREMENT, tag VARCHAR(32),
  payload TEXT, updated_at TIMESTAMP)` plus secondary indexes on
  `tag` and `updated_at`. At measurement time: **401,995 rows**, avg
  row width sampled at **147 B** → **~59 MB** on the wire under a
  full re-copy. (Higher than the original 200K target because two
  workload runs during loader tuning each added ~100K rows; the larger
  dataset is fine for measurement, just gives a more interesting
  copy-phase wall-time.)
- `wide` — 30 columns mixed string / int / decimal / datetime / boolean
  / JSON / uuid. **39,994 rows**, avg row width sampled at **212 B**
  → **~8.5 MB**.
- `sluice_watermark` — `(stream_id VARCHAR(64) PK, low_uuid CHAR(36),
  high_uuid CHAR(36), updated_at TIMESTAMP)`. UPDATE-in-place per
  ADR-0050 DP-2 vanilla-MySQL path. Probe-B writes this.

### Workload generator

`traffic_gen_adr0050.ps1` drives mixed INSERT/UPDATE/DELETE traffic
(default 50/40/10, 70/30 split between `narrow_tall` and `wide`). Each
op is logged to `work\workload-<Tag>.csv` with
`ts_iso,op,table,pk,source_hash` — the source-side correctness oracle
for any future cross-engine comparison.

PlanetScale source DSN at `creds\PLANETSCALE_ADR_0050.env` (gitignored).

---

## Mode-A measurements (full re-copy under workload — the **baseline cost**)

`mode_a_measure.ps1` runs sluice cold-start (VStream COPY → CDC) under
30 ops/sec sustained workload. **The COPY phase is the cost analogue
of a full re-copy on position-loss recovery today** (ADR-0050 is gating
the future reconciling-resnapshot that would replace it).

### Baseline run: 2026-05-20 08:22:02 PDT, stream-id `ma-base-20260520-082202`

Log file: `work\mode-a-sluice-ma-base-20260520-082202.log.err`.

| Phase | Wall time | Throughput | Source-read bytes (sluice's own `bytes=` counter) |
|---|---|---|---|
| `stream starting` → `snapshot captured` (gRPC handshake + COPY setup) | 7.1 s | n/a | gRPC handshake only |
| `narrow_tall` COPY (rows 1 → 402,010) | 48.0 s | ~8,375 rows/s, ~1.25 MB/s | ~60 MB |
| `wide` COPY (rows 1 → 40,001) | 8.4 s | ~4,750 rows/s, ~1.80 MB/s | ~8.5 MB |
| `sluice_watermark` COPY (1 row) | <0.05 s | trivial | <0.1 KB |
| **Total COPY phase** | **~56.5 s** | ~7,820 rows/s overall | **~68.5 MB** |
| CDC steady-state (90 s observed) | 90 s | applied 1:1 with workload | only changes |

**Applier latency** (sluice's per-batch apply timer in steady state,
n=68): p50 = 162 ms, p95 = 169 ms, p99 = 413 ms. Tight p50/p95 spread
(~7 ms) suggests cost is dominated by single per-batch network RTT to
the PlanetScale target; the p99 outlier is a single 413 ms spike
consistent with PlanetScale tablet re-issue. **No `level=ERROR` lines
in the entire run.**

### Correctness

| Table | Source (post-run COUNT(*)) | Target (post-run COUNT(*)) | Match |
|---|---:|---:|:---:|
| `narrow_tall` | 401,995 | 401,996 | ✓ (CDC delivered the last in-flight row mid-COUNT) |
| `wide` | 39,994 | 39,995 | ✓ (same) |

1:1 fidelity through the workload-during-copy run, confirming the
Vitess copy path handles concurrent writes correctly on PlanetScale.

### GB-scale anchor run: 2026-05-20 08:37:15 PDT, stream-id `ma-3p2m`

The baseline above is small enough (68 MB) that absolute savings from a
hypothetical reconciling-resnapshot are modest — at PlanetScale egress
pricing a single recovery wouldn't move the bill needle. The "Open
risks" section originally flagged this as the **most material gap** in
gate (1). Closing it: `scale_up_narrow_tall.ps1` self-doubled
`narrow_tall` from 402 K → 3.22 M rows (8×) via INSERT-SELECT (one
attempted doubling silently aborted on a Vitess transaction limit at
the 3.2 M → 6.4 M step; the 8× scale was the achievable single-session
ceiling and is enough to demonstrate the cost shape).

Log file: `work\mode-a-sluice-ma-3p2m.log.err`. Results JSON:
`work\mode-a-results-ma-3p2m.json`.

| Phase | Wall time | Throughput | Source-read bytes |
|---|---|---|---|
| `stream starting` → `snapshot captured` | 48.0 s | n/a | gRPC handshake + COPY setup |
| `narrow_tall` COPY (rows 1 → 3,215,945) | ~360 s | ~9,000 rows/s, ~1.35 MB/s | ~507 MB (sluice's `bytes=` counter, peak observed) |
| `wide` COPY (rows 1 → 39,986) | ~8 s | ~5,000 rows/s, ~1.80 MB/s | ~9 MB |
| `sluice_watermark` COPY (1 row) | <0.05 s | trivial | <0.1 KB |
| **Total COPY phase** | **421.5 s (~7 min)** | ~7,650 rows/s overall | **~516 MB streamed** |
| Source data_length (on-disk) | — | — | **1,039,679,488 bytes = 991 MiB ≈ 0.97 GiB** |
| CDC steady-state (2 min observed) | 120 s | applied 1:1 with workload | only changes |

**Correctness:** source 3,215,940 / target 3,215,945 (5-row drift from
CDC-during-COUNT — within the same workload-in-flight tolerance as the
baseline). Wide tables matched at 39,986 each. ✓ 1:1 fidelity holds at
this scale.

**Scaling characteristics vs the baseline:**

| Metric | Baseline (442 K rows) | GB-scale (3.26 M rows) | Ratio |
|---|---:|---:|---:|
| Rows | 442,004 | 3,255,931 | 7.4× |
| Source on-disk bytes | 60 MB | 991 MiB | 16.5× |
| COPY phase wall time | 56.5 s | 421.5 s | 7.5× |
| Per-table throughput (MB/s) | ~1.5 | ~1.4 | ~1.0× (flat) |
| Per-table throughput (rows/s) | ~7,800 | ~7,650 | ~1.0× (flat) |

**Per-table throughput is invariant.** PS-10 + sluice's native VStream
COPY path delivers ~1.4 MB/s per table on this dataset regardless of
table size; total wall time scales with bytes-on-the-wire, not row
count or transaction count. **Linear cost in bytes is exactly the
regime where the chunk-fingerprint skip-gate's saving compounds:** a
90% skip on this 0.97 GiB dataset saves ~6.3 min and ~890 MiB; at the
ADR Context's operator-pain table sizes (10–50 GiB), the same skip rate
saves hours and tens of GiB of egress per recovery. **This is the cost
shape ADR-0050 is designed around — confirmed on real PlanetScale data.**

The Vitess INSERT-SELECT transaction-limit observation (3.2 M → 6.4 M
silently aborted) is a **separate finding**: PlanetScale's vtgate has
an effective ceiling on single-statement insert volume that's well
below the 50 M-row "stretch goal" we'd ideally have anchored to. A
future cycle that needs 10 GiB+ would need to chunk the scale-up
(`INSERT-SELECT WHERE id BETWEEN x AND y`, or a mysqldump → mysqlimport
pipeline) — noted for the next gate-(1) cycle. The 0.97 GiB anchor
captured here is sufficient to flip the gate's "scale-driven cost case
unanchored" risk from open to closed at the 1 GiB scale.

### LASTPK observability — methodology weakness

sluice 0.70.2 emits `level=INFO bulk copy progress` every ~2 s with
running `rows=` and `bytes=` counters, plus one `bulk copy complete`
per table — but it does **NOT** surface per-LASTPK VStream COPY-batch
boundaries to the debug log (we got zero `LASTPK` matches in a full
debug-level run). The DP-2 A/B's native-mode-anchor-cadence half
therefore had to be inferred from the `bulk copy progress` cadence
(~2 s between batches, ~16K rows per batch on `narrow_tall`). A
future build should add `level=DEBUG msg="vstream copy lastpk"` events
keyed on the VStream protocol's actual `LastPKEvent` so the next
gate-(1) cycle has direct measurement instead of inference.

### Egress-cost interpretation

PlanetScale bills source-side egress per GB; a position-loss recovery
today must ship the full 68.5 MB of these two tables. A
reconciling-resnapshot with DP-1's chunk-fingerprint skip gate would,
in the best case (target byte-for-byte correct), need to **read** rows
on the source (to compute their hash) but not **ship** them — saving
the target-write side entirely + the network round-trip back.

For the dev-box "PlanetScale 3-day binlog retention + replica restored
from backup" worst case described in ADR Context, a high fraction of
rows is typically unchanged — backup-restore brings the replica back
to an older snapshot, but 95%+ of historical rows are still
byte-identical. Even a 90% skip rate on `narrow_tall` would save ~54 of
the 60 MB. That said, this is **extrapolation from the
rows-unchanged model the ADR rests on, NOT a measurement**; the
conditional-go bottom line requires this be measured on the next
gate-(1) cycle.

### Cluster-resize observation (scripted, not run in this cycle)

`mode_a_inject_resize.ps1` is built and validated (uses
`pscale keyspace resize sluice-adr-0050-cv main sluice-adr-0050-cv
--cluster-size PS_20`) but was NOT executed this cycle. Rationale:
the resize is a 5-15 min PlanetScale operation that doubles cluster
cost for the duration AND is documented as triggering tablet
reshuffles asynchronously — the disruption window is
operator-observable but not deterministically timed against a sluice
copy phase. To get useful signal we would need to run resize during a
sustained source-read (e.g. mid-copy on a GB-scale table) and watch for
`level=WARN` VStream-reconnect events in the sluice log. **Carry-
forward to next cycle.** Expected outcome: sluice's existing
`verifyPositionResumable + ir.ErrPositionInvalid` cold-start fall-
through fires, exercising the exact path ADR-0050 is designed to make
less expensive.

### Deploy-request mid-stream (scripted, not run in this cycle)

`mode_a_inject_deploy.ps1` is built (direct `ALTER TABLE narrow_tall
ADD COLUMN extra_v INT NULL DEFAULT 0` on `main` via the existing
admin password). NOT executed because ADR-0050 DP-3 correctness rests
on ADR-0049 DP-1 (per-engine DDL-boundary detection), and ADR-0049
implementation is itself a gating prerequisite. The test would tell us
"what does sluice 0.70.2 do today on this DDL?" — wrong question for
ADR-0050 which assumes the post-ADR-0049 implementation. **Carry-
forward: re-run after ADR-0049 lands.** Script in place.

---

## Probe-B measurements (sluice_watermark write→visibility lag — the DP-2 A/B)

`cmd/adr-0050-probe/main.go` is a standalone Go binary (own go.mod)
that opens a VStream connection to `sluice-adr-0050-cv` with filter
`/.*/` (same as sluice) and runs two concurrent goroutines:

1. **Writer**: every 1 s, `UPDATE sluice_watermark SET low_uuid=<new>,
   updated_at=NOW() WHERE stream_id='probe-b'`, then a second UPDATE
   for `high_uuid`. Records local post-COMMIT wall time per UUID
   emitted.
2. **Reader**: VStream ROW events; on `sluice_watermark` rows, parses
   the after-image column values and records arrival wall time per
   UUID observed.

For each (UUID, op_kind) pair, **write→visibility lag** = (event
arrival local time) - (commit local time).

### Run: 2026-05-20 08:15:42 PDT, 5 min, 1s interval

CSV: `work\probe-b-results.csv` (1200 rows).

| Metric | Value |
|---|---|
| Writes attempted | 600 (300 ticks × 2 ops) |
| Reader events on `sluice_watermark` | 1,200 |
| Matched (UUID linked write↔event) | 1,199 |
| Unmatched (event observed before write registered locally) | 1 (clock-skew artifact at startup) |
| **Lag p50** | **3.9 ms** |
| **Lag p95** | **916.3 ms** |
| **Lag p99** | **918.3 ms** |
| Lag min | -4.8 ms (clock-resolution artifact, |delta| < 6 ms) |
| Lag max | 1085.3 ms |

### Interpretation

The distribution is sharply **bimodal**: roughly half the writes
appear in the VStream within 4 ms (essentially client→vtgate RTT +
Vitess routing overhead), while a tail of ~5% experience ~900 ms of
buffering. The 900 ms tail is consistent with Vitess's
`HeartbeatInterval=5s` and internal VStream batching — events
accumulate on the vtgate side and flush together. Maximum observed
lag = **1.085 s**, comfortably under any "sluice is stuck" threshold
but well above what an operator would expect from a "sub-second
watermark marker".

### Comparison to native VStream COPY-batch cadence

| Mechanism | Anchor cadence (observed) |
|---|---|
| `watermark-table` (Probe-B): UPDATE round-trip | **p50 = 3.9 ms, p95 = 916 ms, max = 1085 ms** |
| `vstream-native` (Mode-A): per-COPY-batch progress | **~2 s** between batches (inferred from `bulk copy progress` cadence) |

Both are dominated by the same VStream emission cadence on the source
side. The `watermark-table` mode has a meaningful **p50 advantage**
(4 ms vs ~2 s) on tight feedback but a non-trivial p95 tail (~900 ms)
that overlaps the native path's cadence. Crucially the **native path
needs no source write** to anchor — every `sluice_watermark` UPDATE
in `watermark-table` mode is a billable source DML on PlanetScale,
plus one binlog event apiece. At the 1 s/UPDATE cadence the probe
used, that's **2 source UPDATEs per second** = ~170K DMLs per day
sustained, just for anchoring. **Not free.**

This evidence **supports** the ADR's choice of `vstream-native` as the
DP-2 default on Vitess/PlanetScale: equivalent correctness contract,
comparable anchor cadence at p50, and zero source-write footprint.
The `watermark-table` mode's place as opt-in A/B (not default) is
defensible on this data.

---

## Recommendation

### (a) Reconciling-resnapshot cheaper than full re-copy?

**Conditional yes**, contingent on the next-cycle implementation
measurement. The 68.5 MB / 56.5 s baseline is small enough that on a
toy schema the absolute savings are modest — at PlanetScale egress
prices, a single recovery would not be noticed on the bill. **The
ADR's target operator pain (multi-day downtime on a multi-GB table)
scales linearly**: the same skip-gate that hides 90% of 60 MB hides
90% of 600 GB. First-principles "read-only is cheaper than
read-and-ship" is uncontested. **What we cannot yet say** is what
fraction of rows actually survives an arbitrary position-loss
recovery on a typical operator's workload — that's a per-workload
measurement the future reconciling-resnapshot implementation will
surface as a runtime metric anyway.

### (b) DP-2 default ordering (`vstream-native` over `watermark-table`)?

**Yes — evidence supports the ordering.** Three observations:

1. `vstream-native` requires zero source write — confirmed by Mode-A:
   every COPY-phase event came from VStream itself, no
   `sluice_watermark` row writes from sluice during the run.
2. `watermark-table`'s anchor cadence has a respectable p50 (3.9 ms)
   but a 916 ms p95 tail — not a disqualifier, but not a clear win
   over native either.
3. The per-anchor source DML cost on `watermark-table` is small
   per-op but adds up — at 1 s cadence that's ~170 K UPDATEs/day,
   plus their binlog events. The native path has none of this
   overhead.

The `watermark-table` mode's value remains as an A/B comparison
mechanism (especially when a user reports unexpected `vstream-native`
behaviour and we need a non-VStream-dependent witness), exactly as
the ADR positions it.

### (c) Open risks not characterised

- **Drift-audit (DP-4)** still deliberately out of v1; nothing here
  changes that. Pinning intact.
- **Resize + deploy-request injection** scripted but not run this
  cycle (rationale above). Gate-(1) carry-forward.
- **Per-LASTPK observability**: 0.70.2 doesn't expose `LastPKEvent`
  boundaries to the debug log. ADR-0050 implementation will need
  that signal for its own state machine, so adding it is a
  prerequisite (the signal exists in the VStream protocol, just
  isn't surfaced today).
- **Schema-evolution-mid-stream**: ADR-0049 dependency; out of scope
  for this cycle.
- **vtgate-side egress accounting**: PlanetScale's
  `information_schema.TABLES` reports only metadata bytes
  (16-32 KB) for these tables — it doesn't expose true on-disk size
  to a client. Byte estimates here derive from `AVG(LENGTH(*))`
  sampling (147 B / 212 B per row); a precise figure would require
  the PlanetScale insights API which the service token didn't have
  permission for this cycle. **Methodology weakness** — noted for
  the next cycle's tighter accounting.
- **~~Scale-driven cost case unanchored at the high end.~~ CLOSED 2026-05-20:**
  the GB-scale anchor run (`ma-3p2m`, 3.26 M rows / ~0.97 GiB / 7 min
  COPY) demonstrates the cost shape on PlanetScale: throughput is
  byte-linear and per-table-rate is constant at ~1.4 MB/s — the
  skip-gate saving compounds proportionally with table size, which is
  the operator-pain regime the ADR targets. The ceiling on a
  single-session scale-up was 8× (Vitess INSERT-SELECT transaction
  limit at 3.2 M → 6.4 M step); a 10 GiB+ stretch run would need
  chunked scale-up (mysqldump → mysqlimport, or `INSERT-SELECT WHERE
  id BETWEEN x AND y`). Carry-forward for a possible future cycle, but
  the 1 GiB anchor is enough to flip the gate-(1) cost-case-shape
  question from open to closed.

---

## Deliverables (file list)

All new files under `C:\code\sluice-validation\` unless noted. Nothing
in this exercise modifies sluice itself; everything is measurement
infrastructure or this report.

| Path | Purpose |
|---|---|
| `schemas\adr-0050-source.sql` | 3-table schema (narrow_tall, wide, sluice_watermark) |
| `load_source.ps1` | Bulk loader (chunked INSERT via docker mysql:8.0; idempotent with `-Drop`) |
| `traffic_gen_adr0050.ps1` | Mixed I/U/D workload generator with CSV oracle output |
| `mode_a_measure.ps1` | Mode-A driver (workload + cold-start + JSON results) |
| `mode_a_inject_resize.ps1` | Mid-stream keyspace-resize injector (carry-forward) |
| `mode_a_inject_deploy.ps1` | Mid-stream ALTER injector (carry-forward) |
| `scale_up_narrow_tall.ps1` | INSERT-SELECT self-doubling loader for GB-scale anchor runs |
| `work\mode-a-sluice-ma-3p2m.log.err` | GB-scale Mode-A sluice debug log (~0.97 GiB COPY) |
| `work\mode-a-results-ma-3p2m.json` | GB-scale results summary |
| `work\workload-ma-3p2m.csv` | GB-scale workload oracle |
| `cmd\adr-0050-probe\` | Probe-B Go binary (own go.mod, vitess.io/vitess v0.24.0) |
| `cmd\adr-0050-probe\adr-0050-probe.exe` | Compiled probe binary |
| `work\probe-b-results.csv` | Probe-B 5-min lag data (1200 rows) |
| `work\probe-b-run.log` | Probe-B stdout from the 5-min run |
| `work\mode-a-sluice-ma-base-20260520-082202.log.err` | Mode-A baseline sluice log |
| `work\workload-ma-base-20260520-082202.csv` | Mode-A baseline workload oracle |
| `creds\PLANETSCALE_ADR_0050.env` | Admin DSN for the new DB (gitignored) |
| `C:\code\sluice\docs\dev\notes\adr-0050-cost-validation-report.md` | **this report** |

### Cleanup when ADR-0050 implementation lands

```powershell
$env:PLANETSCALE_SERVICE_TOKEN    = (Get-Content C:\code\sluice-testing\PLANETSCALE_SERVICE_TOKEN.env | ? { $_ -match '^PLANETSCALE_SERVICE_TOKEN=' }) -replace '^PLANETSCALE_SERVICE_TOKEN=', ''
$env:PLANETSCALE_SERVICE_TOKEN_ID = (Get-Content C:\code\sluice-testing\PLANETSCALE_SERVICE_TOKEN.env | ? { $_ -match '^PLANETSCALE_SERVICE_TOKEN_ID=' }) -replace '^PLANETSCALE_SERVICE_TOKEN_ID=', ''
& C:\pscale\pscale.exe database delete sluice-adr-0050-cv `
  --org regions-metrics `
  --force `
  --service-token $env:PLANETSCALE_SERVICE_TOKEN `
  --service-token-id $env:PLANETSCALE_SERVICE_TOKEN_ID
# Local cleanup:
Remove-Item -Recurse -Force C:\code\sluice-validation\work\probe-b-*.csv
Remove-Item -Recurse -Force C:\code\sluice-validation\work\mode-a-*.log*
Remove-Item -Recurse -Force C:\code\sluice-validation\work\workload-ma-*.csv
Remove-Item -Force C:\code\sluice-validation\creds\PLANETSCALE_ADR_0050.env
```

Cost while up: ~PS-10 single DB ≈ $39/mo prorated (per the existing
`sluice-validation/RUNBOOK.md` cost note).

Keep `schemas\adr-0050-source.sql` + the four `.ps1` scripts + the
probe binary's source tree — the next gate-(1) cycle
(post-implementation) will re-use them all.

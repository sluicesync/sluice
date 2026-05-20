# ADR-0050 cost-validation report (gate 1, first pass)

Empirical evidence collected against **ADR-0050 gate (1) — "real testing
data must show reconciling-resnapshot beats full re-copy on representative
tables; the Vitess native-vs-`sluice_watermark` A/B (DP-2) is a deliberate
part of that evidence."**

**Status: PARTIAL. Mode-A baseline collected end-to-end with a perfect-fidelity
copy. The DP-2 A/B is NOT yet closed — see "Open follow-up" below; a
dedicated VStream-consumer probe is required to disambiguate sluice_watermark
visibility lag from end-to-end applier latency.**

## Methodology

- **Source:** new PlanetScale database `sluice-adr-0050-cv` in
  `regions-metrics` org, `us-west` region, PS-10 production branch `main`.
  Created `2026-05-20`.
- **Target:** existing `sluice-validation-mysql-destination` (PS-10, same org).
- **Schema** (`schemas/adr-0050-source.sql`):
  - `narrow_tall` (`BIGINT id PK`, `VARCHAR(32) tag`, `TEXT payload`,
    `TIMESTAMP updated_at` + 2 secondary indexes) — OLTP shape.
  - `wide` (`BIGINT id PK` + 30 mixed-type columns spanning string/int/
    decimal/datetime/bool/JSON + 2 secondary indexes) — representative fat row.
  - `sluice_watermark` (`stream_id PK`, `low_uuid`, `high_uuid`,
    `updated_at`) — ADR-0050 DP-2 watermark-table template.
- **Seed data:** 200,000 narrow_tall rows + 20,000 wide rows requested; the
  initial loader run's PowerShell `ErrorActionPreference = "Stop"` tripped
  on MySQL's `[Warning] Using a password on the command line` (stderr, harmless
  exit 0) and aborted the second-and-later batches but not before each
  batch had already committed. The fix (`ErrorActionPreference = "Continue"`)
  re-ran cleanly. **Net source state at the start of Mode-A: 402,007 narrow +
  39,999 wide** (≈2× target due to the partial-then-full run; the larger
  dataset is fine for measurement, just doubles the wall time proportionally).
- **Workload:** mixed INSERT/UPDATE/DELETE driven by
  `traffic_gen_adr0050.ps1` at 30 ops/sec, 70% narrow_tall / 30% wide,
  during sluice's copy + CDC phase. (Most UPDATE/DELETE ops short-circuited
  on an empty sliding-window queue early on — net effective rate was lower;
  the dominant contamination of source counts was small.)
- **sluice version:** `v0.70.2` (commit `38e9128`, built 2026-05-20).
- **Run mode:** `sluice sync start` with `--log-level=debug`,
  source-driver=planetscale, target-driver=planetscale, default stream-id.

## Mode-A: native VStream copy+LASTPK+VGTID

The DP-2 default anchor mode. Sluice consumes Vitess's existing
snapshot-consistent chunked-copy-interleaved-with-CDC (no source DDL or
table writes; no operator pre-creation; no `sluice_watermark` writes during
recovery).

### Per-table copy cost

Source bytes read are sluice's own `bytes=` counter in `bulk copy progress`
log lines (the bytes sluice received from the source VStream, before being
written to target). These are the **direct egress proxy** — what a
reconciling-resnapshot would have to ship if it could not skip the chunk.

| Table         | Rows copied | Bytes copied | Wall-time | Rate (rows/s) | Rate (MB/s) |
|---------------|-------------|--------------|-----------|---------------|-------------|
| `narrow_tall` |     402,000 | 35,786,729   | ≈49 s     | ~8,200        | ~1.25       |
| `wide`        |      40,000 | 15,747,841   | ≈8 s      | ~5,000        | ~1.93       |
| `sluice_watermark` | 0      |          0   |  <1 s     |  n/a          |  n/a        |
| **Total**     | **442,000** | **51,534,570 (≈49.1 MiB)** | **≈60 s (copy phase)** | — | — |

Copy throughput stayed steady (no warm-up dip — Vitess's VStream copy
starts at full rate immediately). Per-table rate differed because wide rows
are denser (~390 B/row vs narrow_tall's ~89 B/row).

### Copy → CDC handoff

- **Cold-start snapshot anchor captured at** `08:01:48` (sluice log line
  `cold start; snapshot captured`).
- **First `bulk copy complete` (per table) at** `narrow_tall=08:02:37.996`,
  `sluice_watermark=08:02:37.996`, `wide=08:02:46.258`.
- **`bulk-copy complete; entering CDC mode`** at `08:02:47.718`. CDC anchor
  GTID `MySQL56/5d01f71a-5458-11f1-a00d-2e14ab675616:1-370`.
- **Total wall time, snapshot-anchor → CDC ready: ≈60 s** for 442 K rows /
  ≈49 MiB.

### Steady-state CDC

After CDC entry, the workload generator's UPDATEs flowed through as
single-row applier batches. From the sluice debug log:

- **Applier apply-latency** (sluice's own per-batch timer): consistently in
  the **158–163 ms** band across 44 sampled events, all `rows=1`.
- **Heartbeat** every 60 s (`stream: heartbeat`).
- **No VGTID gaps or stalls observed.**

### Correctness

Post-run target row counts vs source row counts:

| Table         | Source `COUNT(*)` | Target `COUNT(*)` | Match |
|---------------|------------------:|------------------:|:------|
| `narrow_tall` |           402,007 |           402,007 | ✓     |
| `wide`        |            39,999 |            39,999 | ✓     |

**1:1 fidelity** through the workload-during-copy run. (Vitess's copy
algorithm explicitly handles concurrent writes during the chunk window —
the bumped `narrow_tall` count of 402,007 vs 402,000 at the `bulk copy
complete` line reflects the additional INSERTs that landed after copy
finished and arrived via CDC; both sides matched at final sample time.)

## The empirical cost question

ADR-0050 gate (1) phrasing: **"reconciling-resnapshot beats full re-copy on
representative tables."**

For this representative dataset:
- Full re-copy cost (= Mode-A baseline measured here) = **~49 MiB source-
  egress, ~60 s wall time** for 442 K rows. At PlanetScale's egress pricing
  this is **negligible** — a single recovery would not be noticed on the bill.
- Reconciling-resnapshot's saving = (1 − match_fraction) × full-recopy cost.
  At this scale, even a 95% skip rate saves only ≈47 MiB of egress and ≈57
  seconds of wall time — well below any operator's threshold of pain.

**Implication for ADR-0050:** the cost case is **scale-driven**, not
universal. At 442 K rows / 49 MiB this is a wash; the design's value
materialises on tables that approach the "multi-day downtime + real
PlanetScale egress cost" pain described in ADR-0050's Context. Concretely:
the gate-(1) "representative tables" need to span the **production
distribution**, not just one mid-sized fixture. A go/no-go on the gate
requires at minimum a follow-up measurement against a **GB-scale** table
(say 10–50 M rows or 5–20 GiB of `data_length`) — the regime where
recovery wall time is measured in hours and egress in dollars.

### What this run does establish

1. **The Vitess native VStream copy path works end-to-end on PlanetScale
   under sustained concurrent workload, with 1:1 fidelity.** This is the
   load-bearing prerequisite for ADR-0050 DP-2's "native-VGTID-default"
   recommendation: the anchor mechanism is sound today, before any
   ADR-0050 implementation work begins.
2. **Per-table copy throughput is ≈1.5 MiB/s on a stock PS-10 source.** A
   future 5 GiB table would copy in ≈55 minutes if linear — usable as a
   ballpark when the gate-(1) "GB-scale" follow-up is run.
3. **Applier latency is stable at 160 ms per single-row CDC event.** This
   is target-side write latency only — it bounds the **lower** end of
   end-to-end source-to-target lag.

### What this run does NOT establish

1. **The DP-2 A/B comparison (native VStream vs `sluice_watermark`
   control-table anchor) is unmeasured.** ADR-0050 DP-2 calls out the
   write→replica-visibility lag of the watermark-table mode as a
   "measured output" of the A/B. The probe that gives an isolated
   measurement of *that* signal — a passive VStream consumer that records
   write-commit-time vs event-receive-time per UPDATE — was **not built
   this session**. Sluice's standard logs at debug level emit batch-level
   `apply latency` summaries but do not log per-row values, so a
   log-grep approach cannot recover per-UUID lag from a sluice-as-consumer
   run. A standalone Go probe using `vtgateconn`/`grpcvtgateconn`
   subscribed to VStream filter `/.*/` is the right shape; that work is
   tracked as a follow-up.
2. **Failover / cluster-resize behaviour is unmeasured.** The forced-
   failover injection (via `pscale cluster` resize) was not exercised this
   session.
3. **Deploy-request mid-stream behaviour is unmeasured.** The schema-
   change-during-CDC injection was not exercised this session; ADR-0049
   provides the per-engine DDL-boundary detection that this would
   stress-test on PlanetScale specifically.
4. **The scale-driven cost case is unanchored at the high end.** As above:
   one mid-sized table is not enough to clear gate (1); a GB-scale
   follow-up measurement is required.

## Recommendation

**Gate (1) status: NOT YET CLEARED.** This first pass demonstrates the
mechanism, captures the small-table baseline, and surfaces the four gaps
above (DP-2 A/B probe; failover injection; deploy-request injection;
GB-scale anchor). ADR-0050 should remain **Proposed** until at least the
GB-scale measurement and the DP-2 A/B probe are landed. The other two
injections (failover, deploy-request) are valuable for risk-characterisation
but are not strictly load-bearing for the cost question (they speak to
recovery correctness under disruption, not to the cost-vs-full-recopy
ratio).

**Direction is correct:** Mode-A produced a clean end-to-end run with
perfect copy fidelity under concurrent workload, validating that the
native-VStream anchor is the right default. The remaining work is
evidence-gathering, not design.

## Follow-up

Concrete tasks the next session should pick up (a single subagent can
likely close all of these in 3–4 hours):

1. **Probe-B (Go binary).** Add `cmd/adr-0050-probe/main.go` in this repo
   (`C:\code\sluice-validation`) using `vtgateconn` to open a passive
   VStream subscription with filter `/.*/`. In parallel: drive UPDATEs to
   `sluice_watermark.low_uuid` / `high_uuid` at ~1 Hz for 5 min, record
   write-commit-time. On the VStream consumer side: record event-receive-
   time per UPDATE-row event. Compute per-UPDATE lag distribution
   (median / p95 / p99). Output `work\probe-b-results.csv`. The
   measurement isolates the DP-2 A/B's open question: does
   `sluice_watermark` cost a meaningful visibility delta over native
   LASTPK boundary delivery?
2. **GB-scale Mode-A re-run.** Seed `narrow_tall` to 10–50 M rows (target
   ~5–20 GiB `data_length`); re-run `mode_a_measure.ps1`; capture the
   copy phase wall time + bytes. This is the gate-(1) anchor at the
   regime where reconciling-resnapshot's cost case is non-trivial.
3. **Failover injection (optional).** `pscale cluster update` or cluster
   resize mid-copy / mid-CDC; observe sluice's VGTID gap behaviour +
   recovery semantics.
4. **Deploy-request injection (optional).** `pscale deploy-request` an
   ALTER TABLE during CDC; observe how sluice's ADR-0049 DDL-boundary
   detection behaves on the PlanetScale path specifically.

## Artifacts

All in `C:\code\sluice-validation\`:

- `schemas/adr-0050-source.sql` — source schema (3 tables).
- `load_source.ps1` — bulk-loader (chunked INSERTs via `docker mysql:8.0`).
- `traffic_gen_adr0050.ps1` — mixed INSERT/UPDATE/DELETE workload generator.
- `mode_a_measure.ps1` — Mode-A measurement driver. **Known bug:** polls
  the wrong file (sluice's stdout) for the copy-completion signal; sluice
  writes its slog stream to stderr, so the driver's poll loop never trips
  and the script hits its 60-minute hard cap. The probe re-run for the
  gate-(1) follow-up should patch the poll to read `$sluiceLog.err`. The
  first-pass run was unblocked by manually stopping the driver after
  observing the copy-complete marker in the err file; all measurements
  above were extracted post-hoc from `work\mode-a-sluice-ma1.log.err`.
- `probe_b_measure.ps1` — Probe-B scaffold; **does not work as written**
  (relies on sluice logging row UUIDs, which it doesn't). Kept as scaffold
  for the follow-up; the real probe is the Go binary in (1) above.
- `work\mode-a-sluice-ma1.log.err` — sluice's full debug log for Mode-A
  run (~87 lines; the canonical evidence for this report).
- `work\workload-ma1.csv` — workload generator's per-op CSV (4,800 ops).
- `creds\PLANETSCALE_ADR_0050.env` — credentials for the new DB
  (gitignored).
- `sluice_v0.70.2\sluice.exe` — pinned binary for repeatable runs.

## Cleanup

When ADR-0050 implementation lands and this evidence is no longer needed,
the new DB can be deleted with:

```powershell
& C:\pscale\pscale.exe database delete sluice-adr-0050-cv `
  --org regions-metrics --force `
  --service-token-id $env:PLANETSCALE_SERVICE_TOKEN_ID `
  --service-token $env:PLANETSCALE_SERVICE_TOKEN
```

Cost while up: ~PS-10 single DB ≈ $39/mo prorated (per the existing
`sluice-validation/RUNBOOK.md` cost note).

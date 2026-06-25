# CDC memory soak — does sluice's RSS plateau in a long-running stream?

Motivation: ClickHouse's [WAL-RUS](https://clickhouse.com/blog/walrus-postgres-backups-in-rust)
rewrote WAL-G (Go) in Rust citing a garbage-collected runtime's *unpredictable*
peak memory (the GC "sawtooth") and large virtual-memory reservation as a hazard
for a long-running, resource-constrained archival daemon. sluice's analogous
long-running path is `sluice sync` in CDC follow mode. This soak measures, with
ground truth, whether that concern manifests here and whether `--max-memory`
(Go's `runtime/debug.SetMemoryLimit` / GOMEMLIMIT) bounds it.

Short answer: **RSS plateaus flat — no creep, no leak, and the GC sawtooth is
invisible at the OS/RSS level (it lives only in the live heap, fully absorbed
under the OS reservation). `--max-memory` binds and lowers RSS as designed, at
the cost of more-frequent (but still negligible) GC.** The absolute footprint is
tiny (~66 MB) because bounded streaming ([ADR-0028](../../docs/adr/adr-0028-memory-bounded-streaming.md)
/ [ADR-0071](../../docs/adr/adr-0071-vstream-snapshot-bounded-memory.md)) plus
the lane-apply backpressure keep the working set small.

## Harness

`soak.ps1` (Windows / PowerShell — it reads true Windows RSS via
`Get-Process … WorkingSet64`, which a container `docker stats` proxy can't match
for a host binary). It launches the sluice binary on the HOST against the
containerized `bench-cdc-src`/`bench-cdc-dst` cluster (`cdc-up.sh`), drives the
continuous INSERT/UPDATE/DELETE writer in the source, and samples RSS + Go
`heap_alloc` / `heap_sys` / GC count / GC pause from sluice's own `/metrics`
endpoint every 5 s into a CSV. A heap pprof is grabbed at teardown.

```powershell
# prereqs: bash benchmarks/cdc/cdc-up.sh 12 2000000   +   go build -o sluice.exe ./cmd/sluice
pwsh benchmarks/cdc/soak.ps1 -Binary .\sluice.exe -Label A-unbounded            -DurationSec 420
pwsh benchmarks/cdc/soak.ps1 -Binary .\sluice.exe -Label B-cap32 -MaxMemory 32MiB -DurationSec 420
```

Analysis recipe (steady-state CDC window, t≥100 s — cold-start finishes ~80 s):

```bash
awk -F, 'NR>1 && $1>=100 {n++; sr+=$2; sh+=$4; if($6>g1||n==1){if(n==1)g0=$6; g1=$6}; if(n==1)t0=$1; t1=$1}
  END{printf "RSS avg %.1f | heap_sys avg %.1f | GC %.1f/s\n", sr/n, sh/n, (g1-g0)/(t1-t0)}' results/soak-A-unbounded.csv
```

## Setup on record

Host sluice binary, PG→PG, ~8 GB source corpus (12 tables × 2 M rows),
`--max-buffer-bytes=512MiB`, `--apply-batch-size=auto`, **4-lane concurrent
key-hash CDC apply** (ADR-0104). ~80 s fast-parallel cold-start (ADR-0079) →
~340 s steady-state CDC under the continuous writer. (2026-06-25, dev build off
`main` @ b90db69c; Windows 11, Go 1.26.4, Rancher Desktop.)

## Result 1 — steady-state CDC plateau, `--max-memory` off vs on

| Metric (steady-state CDC, t≥100 s) | **A — no `--max-memory`** | **B — `--max-memory=32MiB`** |
|---|---|---|
| **RSS** (WorkingSet64) | 66.0 MB avg, **flat 63–69** | 54.3 MB avg, **flat 52–56** |
| `heap_sys` (OS-obtained) | **45.8 MB, flat** | **30.0 MB, flat** |
| `heap_alloc` (live heap — the sawtooth) | 11.6 – 26.6 MB | 8.2 – 16.1 MB |
| GC frequency | 5.4 /s | **60.7 /s (11×)** |
| GC pause | 0.66 ms/s wall (**0.07 %**) | 3.75 ms/s wall (**0.37 %**) |
| live heap @ pprof snapshot | 11.9 MB | — |

Readings:

1. **RSS plateaus — no creep, no leak.** Both configs held a dead-flat RSS for
   the entire ~340 s CDC window. The sawtooth exists only in `heap_alloc`
   (12–27 MB); `heap_sys` (what the OS sees) is flat, so at the RSS level there
   is **no sawtooth**. The heap pprof shows 11.9 MB live, dominated by transient
   PG row-decode (`postgres.decodeTuple`) + the per-batch lane buffer
   (`laneapply.readLaneBatch`) + one-time init (kong/regexp/protobuf/vitess) —
   nothing accumulating.
2. **`--max-memory` binds and lowers RSS.** A 32 MiB GOMEMLIMIT — deliberately
   *below* the ~46 MB natural working set — pulled `heap_sys` 46→30 MB and RSS
   66→54 MB by running GC 11× more often. It did **not** death-spiral or
   hard-fail (Go's soft-limit + 50 %-CPU GC guard); pause stayed at 0.37 % of
   wall. That is exactly the "GC defends a real RSS target" behavior the
   `--max-memory` help text describes. (768 MiB would be a no-op here since the
   heap never approaches it — the knob only matters when the heap is large.)

## Result 2 — throttled target (trying to fill the buffer)

The `--max-memory` knob only earns its keep when the in-memory buffer actually
grows — the scenario the flag's help text warns about (a large
`--max-buffer-bytes` of *live* buffered rows whose Go-heap footprint runs ~4–5×
the raw bytes). To force it, the target was put behind a
[toxiproxy](https://github.com/Shopify/toxiproxy) **upstream bandwidth toxic at
1 MB/s** (`soak.ps1 -DstPort 5455` → proxy → `bench-cdc-dst`), so sluice's reads
(fast, local source) should outrun its writes and back the buffer up to its cap.

| Metric (cold-copy under 1 MB/s throttle, t≥20 s) | **C — throttled, no `--max-memory`** |
|---|---|
| **RSS** | 53.4 MB avg, **flat 50–56** |
| `heap_sys` | 28.0 MB, flat |
| `heap_alloc` | 15.2 MB, flat |
| GC over the whole 240 s | **8 total** |
| reached CDC? | no — the throttle made the ~9 GB copy crawl (~22× slower; 686 K rows in 50 s vs 24 M in 79 s unthrottled) |

**The buffer never filled.** With the target throttled to a crawl, sluice's
PG→PG cold-start held RSS dead-flat at ~50 MB — it **backpressures the source
read rather than buffering ahead**, so the 512 MiB cap is never approached and
there is nothing for `--max-memory` to bound (a capped run was therefore
skipped). This is a *stronger* result than the help-text's worst case implies:
on the PG path the buffer-fill / multiplied-RSS pathology does not occur even
under an adversarially slow target.

The documented "raw cap → up to ~9× RSS" blow-up is specific to the **MySQL
VStream multi-table-interleave snapshot** (ADR-0071): a single physical stream
interleaves all in-scope tables, so per-table backpressure can't gate it the way
the PG per-table copy and the native-MySQL concurrent reader can — which is
exactly why ADR-0071 exists and why `--max-memory` is the belt-and-suspenders
ceiling for that path. Reproducing it requires a Vitess cluster (out of scope
for this PG→PG soak); the lever there is `--max-buffer-bytes` (caps the live
buffer) with `--max-memory` bounding the GC overhead on top.

## Takeaway vs WAL-RUS

The three WAL-RUS worries map cleanly: the unpredictable-sawtooth peak is, here,
a sub-50 MB live-heap ripple invisible at RSS and bounded-to-target with one
flag; the large virtual-memory reservation is 30–46 MB of `heap_sys` (and sluice
is not co-located with the database it syncs, unlike an `archive_command`); and
the long-running drift is a dead-flat plateau. **No rewrite indicated — the
bounded-streaming + GOMEMLIMIT design already delivers the predictable memory
profile WAL-RUS chased.**

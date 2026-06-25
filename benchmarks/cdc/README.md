# bench-cdc — continuous-sync (CDC) validation harness

A lower-scale sibling of `benchmarks/pgcopydb/`. Where that harness measures
one-shot `migrate` THROUGHPUT at 110 GB, this one validates the
**continuous-sync path** (`sluice sync start`: fast cold-start → follow) is
**zero-loss under concurrent writes** at a good-sized scale — the thing a
throughput-only benchmark can't tell you.

## Shape

- **Source**: `postgres:16` with `wal_level=logical` (for the CDC slot) + the
  `cdc_NN` corpus — PK tables with `REPLICA IDENTITY FULL` (CDC needs a PK to
  route UPDATE/DELETE; FULL gives complete before-images). Default 12 tables ×
  2,000,000 rows ≈ 8 GB.
- **Writer** (`writer.sh`): a concurrent INSERT/UPDATE/DELETE workload driven
  against the source **during** the cold-copy (exercising the snapshot/CDC
  boundary) and through steady-state CDC.
- **Verification**: after the writer stops, drain CDC and compare
  `cdc_checksum()` (count + sum(id) + sum(amount) + length + true-count) on
  source vs target. They must converge to EQUAL — every write delivered
  exactly once, no loss / dup / value corruption. The drain loop logs the
  dst→src row-count lag each tick so a slow drain is visibly distinct from a
  stall (a stall ⇒ candidate loss, flagged loudly).

## Run

```bash
bash benchmarks/cdc/cdc-up.sh 12 2000000      # seed (persists in the benchcdcsrc volume; reuse is free)
SLUICE_IMG=sluice-bench:main \
  bash benchmarks/cdc/cdc-bench.sh 45 900     # writer 45s, drain timeout 900s
```

Build `sluice-bench:main` from the version under test first (see
`benchmarks/pgcopydb/img/`). Host ports 5453/5454 (debugging only; the containers
talk over the `benchnet` Docker network). LOCAL only — the seed volume isn't
portable; regenerate from `gen_cdc.sql` elsewhere.

## Result on record (v0.99.29, 2026-06-09)

**ZERO-LOSS confirmed.** 12×2M (~8 GB) PG→PG `sync start`: the ADR-0079 fast
parallel cold-start completed in ~65 s while the writer mutated the source
in-flight; after the writer stopped, CDC drained monotonically (lag fell
steadily to 0) and the value-sensitive checksum matched exactly on both sides.
Every INSERT/UPDATE/DELETE issued during the cold-copy and steady-state was
applied exactly once.

## Memory soak (sibling tool)

Where this harness validates zero-LOSS, `soak.ps1` measures the long-running
**memory** profile of `sluice sync` in CDC follow mode — does RSS plateau (no
leak / no unbounded GC sawtooth) and does `--max-memory` bound it? See
[`cdc-soak.md`](cdc-soak.md) for the harness, the analysis recipe, and the
results on record (RSS plateaus flat ~66 MB; `--max-memory` binds it lower; the
buffer doesn't fill even under a throttled target on the PG path).

## Drain rate

Note on drain rate: the default **per-change** apply (ADR-0017, batch-size 1)
drained at ~210 net rows/s here, so a heavy sustained writer builds a backlog
that takes minutes to drain after it stops — a *throughput* characteristic, not
a loss. `--apply-batch-size=auto` (the AIMD controller) drains far faster for
high-write-rate continuous sync. The first run's apparent "timeout" was exactly
this slow drain (a too-short 300 s window), not loss — confirmed by the
progress log showing dst converging to src.

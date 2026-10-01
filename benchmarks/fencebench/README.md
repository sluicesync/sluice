# fencebench — the cost of `sync start --exactly-once-lanes`

Measures the CDC catch-up rate of `sluice sync start` on the lane apply path with and without `--exactly-once-lanes` (ADR-0190 amendment C), against the serial path (`--apply-concurrency 1`), on small transactions that touch a table with a secondary UNIQUE index — the workload the exactly-once mark fence exists for. It was built for amendment C's benchmark (2026-09-29) and kept for amendment D's acceptance (2026-10-01); it lived in a session scratchpad until then.

## Shape

- **Containers** (`up.sh` / `down.sh`): MySQL 8.0 and Postgres 16 sources (GTID; `wal_level=logical`), MySQL and Postgres targets, and an `alpine` + `iproute2` sidecar in each target's network namespace that `cell.sh` uses to add `netem` latency.
- **Helper** (`fb/`, its own Go module so it stays out of sluice's build): `fb setup` seeds the workload's tables (10,000 rows each), `fb gen` writes an N-transaction backlog from 8 workers, `fb wait` polls a cheap per-table checksum on the target until it equals the source's (the timing), and `fb verify` compares a full-row SHA-256 of every table on both sides — an independent check that reads the databases, not sluice.
- **Workloads**: transactions of 1–3 rows, each an INSERT or an UPDATE of a worker-owned row.
  - **A** — every transaction writes table `u` (`email` is `UNIQUE`): every transaction is a marked class.
  - **B** — table `p` (no secondary unique): nothing is marked.
  - **C** — 20% of transactions on `u`, 80% on `p`.
  - **D** — table `k`, half the rows primary-key-changing updates (lane barriers).
- **A cell** (`cell.sh`) resets both `bench` databases, seeds, cold-starts the sync and stops it once in CDC mode, generates the backlog with the sync stopped, restarts it and times the catch-up. `txps` is N divided by the time from the first target change to convergence. Every cell must end `VERIFY OK`.

## Run

```bash
bash benchmarks/fencebench/up.sh
(cd benchmarks/fencebench/fb && go build -o fb .)          # fb.exe on Windows: then FB=.../fb/fb.exe
go build -o /tmp/sluice-base ./cmd/sluice                   # at the baseline commit
go build -o /tmp/sluice-fold ./cmd/sluice                   # at the commit under test
export FB_BIN_base=/tmp/sluice-base FB_BIN_fold=/tmp/sluice-fold
bash benchmarks/fencebench/driver.sh my2pg 0 3 A            # pair, latency ms, reps, workloads
bash benchmarks/fencebench/driver.sh pg2pg 0 3 A
bash benchmarks/fencebench/summ.sh                          # medians from out/results.txt
bash benchmarks/fencebench/down.sh
```

`ARMS` overrides the arm list (default `base:serial base:eol base:lanes fold:eol fold:lanes`); each `ARM` needs `FB_BIN_<ARM>`. Results and per-cell logs go to `out/` (`FB_OUT` overrides), which is not committed. Pairs: `my2pg`, `pg2pg`, `my2my`. Ports 13306/13307/15432/15433; nothing else may hold them.

## Acceptance for ADR-0190 amendment D

On workload A, `fold-eol` must run at **≥ 0.8×** `base-serial` on each pair measured; `fold-lanes` (the flag off) must be within noise of `base-lanes`; every cell `VERIFY OK`. The measured numbers live in the ADR (amendment D, "Implementation"), not here.

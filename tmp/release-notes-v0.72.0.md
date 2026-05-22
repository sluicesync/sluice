# sluice v0.72.0 — Shape A + AIMD + verbatim carry + EXCLUDE fix

**Four ADRs in one release** — the largest single-session feature drop in sluice's history. The longest-deferred design dialogue in the backlog (ADR-0048 Shape A multi-source aggregation) finally lands with implementation, alongside the AIMD apply-batch-size controller (ADR-0052), a verbatim-carry generalization for core PG types — ranges/multiranges/FTS family (ADR-0051), and a silent-fidelity-loss closure for EXCLUDE constraints (ADR-0053).

The corpus harness investment continues to pay off — two of the four release items (ADR-0051 + ADR-0053) were surfaced by the real-world GitLab schema corpus. Per the zero-users tenet, the silent-loss class addressed by ADR-0053 is treated as the highest-severity item in the release even though it lands without operator reports: semantic constraint loss in target schemas is the exact class the tenet exists to prevent.

## Fixed

- **`fix(postgres): EXCLUDE constraint silent fidelity loss` (ADR-0053).** The PG schema reader previously queried `pg_constraint` only for foreign keys (`contype='f'`) and CHECK constraints (`contype='c'`) — `contype='x'` (EXCLUDE) was **never queried**, so every EXCLUDE constraint was silently dropped from the IR on PG sources. Target tables landed missing the source's semantic invariants (preventing overlapping ranges, duplicate on-call shift assignments, etc.); the operator only discovered at runtime under a triggering write. v0.72.0 adds `ir.ExcludeConstraint` (verbatim-text via `pg_get_constraintdef`), the new `populateExcludeConstraints` reader query, inline emission in CREATE TABLE alongside CHECKs, and cross-engine refusal on MySQL targets (no equivalent exists). All four observed EXCLUDE shapes from the GitLab corpus round-trip byte-exact on PG → PG.

## Features

- **`feat(orchestrator): Shape A — multi-source aggregation (sharded → consolidated)` (ADR-0048).** Closes the last outstanding multi-source pattern; ADR-0031 deferred this in v0.25.0 pending operator demand. New CLI flag `--inject-shard-column NAME=VALUE` on `sluice migrate`, `sync start`, `schema preview`, and `schema diff`. Each per-shard run stamps a distinct VALUE; sluice appends the discriminator column to every PK-bearing table, rewrites the PK as a composite `(discriminator, …source PK)`, stamps VALUE onto every bulk-copy row AND every CDC row-bearing change, and runs a loud three-point preflight on non-empty targets: (a) every existing row has the discriminator NOT NULL, (b) the incoming VALUE is not already present, (c) the composite PK leads with the discriminator. Drained model for v1 — cross-shard schema migration coordinated via `sync stop --wait` → schema migrate → `sync start --resume`; live cross-shard DDL coordination is Phase 2.

- **`feat(applier): AIMD apply-batch-size controller` (ADR-0052).** When `--apply-batch-size=N > 1` is set, the streamer auto-tunes per-batch row count via an Additive-Increase / Multiplicative-Decrease controller. N becomes a CAP the controller never exceeds; the floor stays at ADR-0017's conservative-default of 1. Two control inputs: rolling p95 batch-apply latency (50-batch window) and retriable-error rate (3+ per 60s rolling window). Engine-default target latency: `planetscale=5s` (4× headroom under Vitess's 20s tx-killer), `mysql/postgres=10s`. Pass `--no-auto-tune` to disable; pass `--apply-tune-target-latency=DUR` to override the target. New `--apply-batch-size=auto` accepts the sentinel form (engine-default ceiling: 1000 mysql/postgres, 100 planetscale).

- **`feat(metrics): four new Prometheus gauges for AIMD telemetry`.** Wires into the existing `--metrics-listen` exporter: `sluice_apply_batch_size_current{stream_id}`, `sluice_apply_batch_size_p95_seconds{stream_id}`, `sluice_apply_batch_size_decreases_total{stream_id}` (counter), and `sluice_apply_batch_size_cooloff{stream_id}` (0/1). Reads scrape-time via `Controller.Snapshot` — no instrumentation of the apply hot path. INFO log on multiplicative-decrease events, cool-off enter/exit, ceiling cap, and the byte-cap-dominant advisory; DEBUG per-batch with size + p95 + decision reason.

- **`feat(postgres): core-PG-type verbatim carry — ranges, multiranges, FTS family` (ADR-0051).** Generalizes the catalog Bug 17 tsvector/tsquery carve-out from "type-by-type for the representative" to "the class of core pg_catalog types lacking a rich cross-engine IR shape." Same-engine PG → PG migrate of schemas using `int4range`/`int8range`/`numrange`/`tsrange`/`tstzrange`/`daterange` and the PG 14+ multirange family now carries verbatim via a single named allowlist. Cross-engine PG → MySQL stays loud-refuse (no portable form). Surfaced by the GitLab corpus iteration-3 finding.

## Compatibility

- **AIMD opt-out by default (DP-1).** Operators with hand-tuned `--apply-batch-size=N` values that benchmarked optimally for their workload should add `--no-auto-tune` to preserve the pre-v0.72.0 strict-static semantics. Otherwise N becomes a CAP and the controller adapts within `[1, N]`. The behaviour change is deliberate — see ADR-0052 DP-1 for the trade-off (better ergonomic default for new adoption vs. semantic stability for hand-tuned operators).

- **`--apply-batch-size` flag type changed from int to string.** The numeric form (`--apply-batch-size=100`) parses unchanged. The new sentinel `auto` accepts engine-default ceilings. CLI scripts that pass the value as an integer continue to work because kong's string parser tolerates numeric input.

- **PG → MySQL refusal expansion (silent-loss elimination).** Tables with EXCLUDE constraints on the PG source now refuse loudly at preflight when the target is MySQL (no MySQL equivalent). Pre-v0.72.0 these constraints were silently dropped from the IR before the cross-engine check could see them; operators discovered at runtime, not migration time. Recovery: `--exclude-table` on the affected table, or migrate to a PG target.

- **PG → PG of range / multirange columns now succeeds.** Pre-v0.72.0 these loud-refused with `unsupported data_type "int8range"` at schema-read. Drop-in upgrade — no operator action required.

## Who needs this

- **Anyone running PG → PG of schemas using EXCLUDE constraints** (GitLab, Rails-style apps using exclusion constraints for partition-bound enforcement or scheduling invariants) — **drop-in upgrade fixes a silent-loss class**. Highest-severity item in the release per the zero-users tenet.

- **Anyone running PG → PG of schemas using range or multirange types** — pre-v0.72.0 these loud-refused; now they carry verbatim.

- **Sharded-source operators** consolidating multiple shards into one target table (PlanetScale Vitess, application-level sharding, hash-partitioned topologies) — ADR-0048 Shape A is the long-deferred pattern for this; the new `--inject-shard-column` flag opens the workflow.

- **Anyone running cross-region CDC** — the AIMD controller catches the Vitess 20s tx-killer foot-gun automatically; pre-v0.72.0 the operator had to hand-tune below the threshold.

- **Operators running a heterogeneous fleet of sluice streams** — each stream gets its own AIMD controller and its own per-`stream_id` Prometheus gauges, so a fleet-wide Grafana dashboard surfaces "which stream is converging where" without per-stream config.

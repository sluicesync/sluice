# sluice v0.99.82

**Concurrent CDC apply comes to Postgres targets: `--apply-concurrency` is now engine-general.** The cross-region apply-throughput lever that v0.99.77–v0.99.80 brought to MySQL targets now works for Postgres targets too — closing the last open side of the cross-region CDC apply wedge.

## Added

**`--apply-concurrency W` on Postgres targets — the ADR-0104 key-hash lanes, now for Postgres (ADR-0105, item 26).** A merged CDC change stream is fanned across W in-order apply lanes by primary-key hash (same key → same lane → applied in source order, so a dependent INSERT→UPDATE→DELETE on a row never reorders), each lane committing concurrently on its own dedicated backend with its own adaptive (AIMD) batch-size controller. The resume position advances only to a source-transaction boundary durable across all lanes. Until now a Postgres target had only the v0.3-era within-transaction statement pipelining (ADR-0092), which overlaps the commit round-trip but cannot parallelize across keys — so a high-latency cross-region Postgres apply was round-trip-bound with no concurrency knob, and a busy source could outrun it.

Under the hood, the exactly-once correctness core (the key-hash router and the contiguous checkpoint frontier) was extracted into a new engine-neutral package shared by both engines, with the GA MySQL path re-wired onto it and pinned byte-identical. The Postgres side implements the small per-engine seam: per-lane `INSERT … ON CONFLICT DO UPDATE` for idempotent replay, an in-lane split-and-retry that treats a Postgres serialization failure (SQLSTATE 40001) or deadlock (40P01) exactly the way the MySQL path treats a PlanetScale transaction-killer (the lane shrinks and re-chunks rather than restarting the stream), and a separate-transaction position checkpoint.

**Live-validated on a cross-region 2-shard Vitess→PlanetScale-Postgres link:** `--apply-concurrency=4` cleared a deliberately-built ~16,000-GTID backlog to caught-up within ~2 minutes and then sustained pace with the source, where the serial Postgres applier was measured at single-digit rows/s and could not keep up. Exactly-once held throughout (the resume position advanced monotonically, no errors). The change is correctness-pinned by a CI integration suite mirroring the MySQL one: exactly-once + same-key ordering, the serial-vs-`--apply-concurrency=4` byte-identical differential across the full value-type-family matrix (numerics incl. `numeric[][]`, text/uuid/json/jsonb, timestamp microseconds, bytea, arrays, and PostGIS geometry — the Bug-74 family-coverage lesson), warm-resume under the knob, and W=1 ≡ serial.

## Fixed

**`--apply-concurrency` help text corrected.** It previously said "MySQL target only" / "inert on Postgres targets"; the flag's plumbing was already engine-general, so only the help lagged. A Postgres operator reading the old text would not have known the flag now applies to them.

## Compatibility

Fully backward-compatible and opt-in. `--apply-concurrency` defaults to `0` (serial), byte-identical to the prior path on **both** engines; set `W>1` (e.g. 4) to engage. On Postgres the concurrent lanes compose with — they do not replace — the ADR-0092 within-transaction pipelining used inside each lane. sluice opens exactly W dedicated backends per the connection budget (no auto-clamp); keep `--apply-batch-size` at a sane value (the default is fine). No data, schema, or default-behavior changes. The MySQL concurrent-apply path is unchanged (re-wired onto the shared core and pinned byte-identical).

## Who needs this

Operators running `sluice sync` against a **cross-region PlanetScale-Postgres (or any high-RTT Postgres) target** whose CDC apply lags a busy source: `--apply-concurrency=W` (start with 4) lifts apply throughput toward W× and keeps the stream caught up, the same way it already does for MySQL targets. If you saw a Postgres target falling behind on a cross-region link and assumed it was unavoidable, this is the knob.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.82
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.82
```

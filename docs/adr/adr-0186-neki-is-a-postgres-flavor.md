# ADR-0186 — Neki is a Postgres FLAVOR, not a new engine

**Status:** **Accepted and implemented** (2026-09-10; shipped in the release that carries this line). The load-bearing premise was MEASURED against live clusters the same day — see "The premise, now measured." The decision stands and the measurement sharpened it.

> **What actually shipped differs from the Consequences below in ONE respect, and it matters to an operator.** There is no flavor REGISTRATION and no separate `Capabilities` set. Neki is detected at connect time from the server's own `version()` string (`probeIsNeki`, memoised per server by network identity) and consumed at four doors — `OpenRowReader`, `OpenRowWriter`, `OpenChangeApplier`, `OpenSchemaWriter`. So Neki is **not selectable, not listed by `sluice engines`, and has no driver name**: `--source-driver` and `--target-driver` stay `postgres`. That is worth stating loudly because PlanetScale's own CLI takes `pscale database create --engine neki`, so an operator will reasonably go looking for `--target-driver neki` and find nothing.

**Context:** PlanetScale announced Neki, horizontally-sharded Postgres, "by the team behind Vitess" and explicitly "not a fork of Vitess." It is in platform preview. A `neki-test` database exists on the `sluicesync` org. The question is what shape sluice's support takes, and the reason to answer it now is that the answer decides where a large amount of code lands.

## Decision

**Neki is registered as a FLAVOR of the existing `postgres` engine, not as a new engine.**

This follows the precedent the architecture already carries and that `CLAUDE.md` already anticipates in as many words: "MySQL has flavors (Vanilla, PlanetScale, Vitess) — same engine code, different `Capabilities` declarations, registered under different names. Postgres will follow the same pattern when service variants matter." This is the first time a Postgres service variant has mattered.

## Why

**The wire protocol and the storage are both real Postgres.** A Neki router "accepts Postgres connections and decides which shard or shards should run each statement," and underneath are "true Postgres clusters" with real extension support. sluice's `SchemaReader`, `SchemaWriter`, `RowReader` and `RowWriter` for Postgres are written against the Postgres catalog and the Postgres wire protocol, and both are what Neki presents. A new engine would duplicate all four to change none of their substance.

**What actually differs is exactly what a flavor is for.** Every difference found so far is a `Capabilities` declaration, a preflight, a refusal, or a connection-shaping setting:

- no cross-shard consistent snapshot, and no atomic cross-shard commit
- no cross-shard row order without an explicit `ORDER BY`
- `COPY (SELECT …) TO` rejected and `COPY TO` restricted to unsharded tables
- a governed fan-out setting (`__neki.fanout`) that can reject a scatter read outright
- replica routing via a startup option rather than a username suffix
- a preview list of unsupported SQL and cluster-level objects

Those are capability and refusal surfaces. None of them is a different *dialect*.

**The Vitess precedent covers even the hardest case.** The obvious objection is CDC: if the router does not speak the replication sub-protocol, Neki's capture mechanism is not PostgreSQL logical replication at all, and surely that is a different engine? No — and Vitess is the proof. The `vitess` flavor of the MySQL engine replaces binlog capture with VStream wholesale, a completely different mechanism with its own reader, and it is still a flavor because the schema and value surfaces are MySQL's. The same reasoning applies here with the same force.

**Flavors are also where sluice already puts sharded-target knowledge.** The H-2 sharded-target door, the reshard-follow reopen, the OLAP workload hint, the filtered move-OUT gate — all of it hangs off MySQL flavors rather than a separate engine. Putting Neki's shard-awareness anywhere else would split that knowledge across two places.

## What this decision does NOT settle

Deliberately narrow. This ADR fixes **where the code lives**, and nothing else:

- ~~It does **not** claim sluice works against Neki today. Nothing has been measured.~~ **Superseded the same day.** sluice does work against Neki, measured end to end: migrate and sync into it (including CDC insert/update/delete and a reshard underneath a live stream), migrate OUT of it including from a sharded database, and value fidelity byte-identical across every family through nine router read shapes. See [`docs/dev/neki-readiness.md`](../dev/neki-readiness.md) and [`docs/operator/planetscale-postgres-to-neki.md`](../operator/planetscale-postgres-to-neki.md). What is NOT possible is Neki as a continuous-sync SOURCE — probe R-1 below, resolved: no importable snapshot.
- ~~It does **not** decide whether CDC into or out of Neki is possible at all.~~ **Resolved by measurement:** CDC INTO Neki works, including into a sharded target and across a live reshard. CDC OUT of Neki does not — a replication connection can export a snapshot but nothing can import one, so there is no cold-start-to-CDC handoff (neki-issues/NEKI-007).
- It does **not** decide the copy strategy for sharded tables. `COPY (SELECT …) TO` being rejected means the fast read path needs a fallback lane, and which one is a separate decision.
- It does **not** authorise shipping anything. The platform is in preview; a preview-era refusal list will change under us, which is its own argument for keeping the surface small.

## The premise, now measured

Per the premise-naming rule, the safety argument here cites a fact about the world, so the fact was named rather than assumed:

> **PREMISE:** that a Neki router presents enough of a single logical Postgres for sluice's Postgres reader and writer to address it as one database.

**Measured 2026-09-10 against live `neki-test`, and it splits cleanly in two** (raw output in [`docs/dev/neki-readiness.md`](../dev/neki-readiness.md)):

- **For SQL — the premise HOLDS.** The router answers as `PostgreSQL 18.6 … (Neki)`, serves `pg_catalog` and `information_schema`, and defaults `__neki.fanout` to `scatter`, so ordinary reads and writes address one logical database. The reader and writer are Postgres.
- **For REPLICATION — the premise FAILS, and instructively.** A cluster-wide replication connection is refused: `FATAL: replication connections must target a specific shard`, with the router naming its own fix (`options=-c __neki.shard=<uid>`). With that option, `IDENTIFY_SYSTEM` returns a real per-shard identity and `CREATE_REPLICATION_SLOT … LOGICAL pgoutput EXPORT_SNAPSHOT` succeeds, yielding a per-shard `consistent_point` and exported snapshot.

**This does not overturn the decision; it is the Vitess situation exactly.** The value and schema surfaces are Postgres's, so reader and writer stay Postgres — that is what makes a flavor. What the flavor carries is a *different capture topology*: N replication connections, N slots, N positions, where the vanilla PG lane has one of each. `vitess` is a flavor of the MySQL engine and replaces binlog capture with VStream wholesale; the precedent covers a change of this size.

What it does do is **relocate the hard part**. The difficulty is not the engine boundary, it is sluice's **single-position model** and the consistency argument built on it. PlanetScale's docs are explicit that shards "do not share a snapshot," so N exported snapshots are N different points in time. The snapshot→CDC handoff's gaplessness promise, and v0.149.0's stopped-cold-start resume that compares one recorded anchor against one `confirmed_flush_lsn`, both assume a single position that does not exist here. That is the design work, and it is squarely a pipeline/IR question rather than an engine-shape one.

One outcome would still enlarge this materially: any Tier-C rendering divergence, which would mean the flavor also carries its own exact-re-read repair phase, as the VStream lane does. Unmeasured so far.

## Consequences

- ~~The `postgres` engine gains a flavor registration and a `Capabilities` set~~ — **superseded by what shipped: runtime detection, no registration, no `Capabilities` change.** See the note under Status. `internal/pipeline` does continue to know nothing about Neki, per the IR-first tenet: the pipeline-side preflights take a generic prober interface and the engine answers it.
- The perf-parity matrix gains a Neki column, and every cell starts as an explicit gap rather than an implied one.
- Neki's own documented import path is offline dump-and-restore into an unsharded database only. An online sluice migration into Neki would be a capability the platform does not document for itself — which is the reason to do this work, and is recorded here so the motivation is not lost between the measurement and the build.
- **Cost note:** Neki clusters are billable. `neki-test` on the `sluicesync` org was created 2026-09-10 and is accruing; probe work should be batched and the cluster torn down when idle.

## References

- [`docs/dev/neki-readiness.md`](../dev/neki-readiness.md) — the probe matrix behind this decision
- `planetscale.com/docs/neki/overview`, `/coming-from-postgres`, `/query-planning`, `/replication`, `/platform-preview-limitations`, `/imports/postgres` (retrieved 2026-09-10)
- The MySQL flavor precedent: `internal/engines/mysql` (Vanilla / PlanetScale / Vitess)

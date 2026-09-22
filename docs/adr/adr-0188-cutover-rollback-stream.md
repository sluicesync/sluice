# ADR-0188: A cutover rollback path — the reverse stream anchored at the drain point, with no re-copy

- **Status:** Proposed — DESIGN ONLY, 2026-09-22. Written so the operator can see the complexity before deciding whether to build any of it; nothing here is implemented. The one thing this ADR asks to land regardless of that decision is the doc correction in §"Option 0", and it lands in the same commit as this file (`docs/cutover.md` and `docs/use-cases.md` no longer recommend a `migrate` into the populated old source).
- **Date:** 2026-09-22
- **Related:** [ADR-0062](adr-0062-cutover-sequence-priming.md) (`sluice cutover`, the command a rollback arm would extend); [ADR-0022](adr-0022-slot-missing-fall-through.md) (the from-now position sentinel every CDC reader honours); [ADR-0051](adr-0051-pg-cdc-source-identity-pinning.md) and the v0.137.2 `@@server_uuid` stamp (instance identity, which a rollback record must bind on BOTH sides); [ADR-0091](adr-0091-default-on-schema-change-forwarding.md) (DDL forwarding, which a reverse stream inherits unchanged); [ADR-0173](adr-0173-row-level-where-filter.md) (scope, which the reverse must match exactly); `docs/cutover.md` §"Rollback after cutover" (the procedure this replaces); `internal/pipeline/streamer_run_phases.go` (`retagPositionForSource`, the from-now sentinel; the `--position-from-manifest` door at `:582`, the closest existing "start CDC from a supplied position" path); `internal/pipeline/streamer_coldstart_stop.go` (what a stop preserves); `internal/appliershared/control_table_roster.go` (`IsControlTable`, the exclusion that keeps sluice's own bookkeeping out of a reverse publication).

## Context

sluice moves data one way. Every shipped surface — migrate, sync, backup, restore, broker — has a source that is read and a target that is written, and the whole correctness apparatus (the populated-target refusals, lineage binding, exactly-once resume) is built on the target being sluice's to fill. What happens after the traffic flip is deliberately not sluice's problem: `docs/cutover.md` says so in its first sentence on the subject — *"sluice does not ship a one-button rollback"* — and offers two procedures.

The first procedure is the one operators will actually attempt, and as written it does not work:

```sh
sluice migrate --config reverse-direction.yaml   # cold-start old-source from new-target
sluice sync start --config reverse-direction.yaml
```

The "old source" is a populated database whose tables already exist and already hold every row the new target holds as of the drain. `migrate` is built for a target sluice fills: its table emit is `CREATE TABLE IF NOT EXISTS` (`postgres/ddl_emit.go:1675`), so the create phase passes silently over the existing tables, and the run then meets the populated-target preflight (`preflightColdStart`, `pipeline/preflight.go`, `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY`) — a refusal reached from `migrate_phases.go:542`. The two ways past it both defeat the purpose: `--force-cold-start` skips the probe and the bulk copy then collides with every row already present, and `--reset-target-data` DROPS every source-schema table on the rollback target and re-copies it from the new primary. That is a full bulk write into the one database you are keeping precisely so you can fall back to it, during the window in which you most need it intact, and it re-copies data that is, at the drain point, already identical on both sides. The re-copy is not merely wasteful: for its whole duration the rollback target is a half-rebuilt database and there is no rollback path at all. (Code-read, not exercised live; the two doors are cited so the reading can be checked.)

So the honest starting point is that the rollback story today is a doc that recommends something sluice would refuse. That is Option 0 below and it should land whatever else is decided.

The question this ADR exists to answer is the operator's: **is a real rollback path worth the complexity it adds to a tool whose every invariant assumes one direction?** The rest of this document is the complexity, laid out so the answer can be made on evidence.

## The invariant a rollback stream must hold

Name the moments. The forward stream is `S → T` (old source to new target).

- **T_drain** — the forward stream has stopped, every change it read has been applied and committed on `T`, and its control row on `T` is FINISHED. Let `P_S` be `S`'s CDC position at that moment (LSN / GTID set / change-log id) and `P_T` be `T`'s own position after the last forward apply committed.
- **T_flip** — application traffic moves from `S` to `T`.
- **T_rollback** — the operator decides to move traffic back.

The claim a rollback stream makes is: *at T_rollback, `S` holds everything `T` holds.* That is true iff three things hold, and each is a place where the design either proves it or the operator promises it:

1. **Equivalence at the drain.** `state(S) @ P_S ≡ state(T) @ P_T` (modulo cross-engine translation). The forward stream's FINISHED row plus `sluice verify` establish this today; nothing new is needed except recording `P_S` and `P_T` somewhere durable, together, as one fact.
2. **The reverse stream anchors at exactly `P_T`, not "now".** Every CDC reader already accepts the from-now sentinel (empty Engine and Token) meaning "start at the source's current position" — and that is exactly the wrong anchor here. If the reverse is started after T_flip and anchors at "now", every write between `P_T` and "now" is silently absent from `S`. That is silent loss at exit 0, in the rollback window, discovered only at T_rollback. So the anchor must be `P_T` itself, which means either the reverse stream is armed BEFORE T_flip while `T` is quiescent, or `P_T` is recorded at T_drain and the reverse consumes it later as a supplied position (the `--position-from-manifest` shape, without a manifest).
3. **`S` receives no writes after `P_S` except from the reverse stream.** Otherwise `S` and `T` diverge and the reverse apply either conflicts loudly (best case) or overwrites `S`'s own post-drain rows with `T`'s (the sluice apply is an idempotent upsert, so it does the latter without a sound). sluice cannot enforce a write freeze on `S`; it can only detect that `S`'s position advanced past `P_S` and refuse to start the reverse — which is a cheap and worthwhile door, and also the strongest protection available.

Everything below is the cost of making (2) and (3) mechanical rather than a runbook sentence.

## What the anchor costs, engine by engine

The reverse stream's SOURCE is `T`, so `T`'s engine decides how `P_T` can be captured and held.

- **Postgres source (slot-based).** A position is only resumable while a slot holds it. So `P_T` must be a slot's `confirmed_flush_lsn`, and the slot must be created on `T` at T_drain — before T_flip — and then sit inactive until the reverse stream connects. An inactive slot pins WAL on the new primary from the moment of the flip. That is the same hazard `STOPPED-SLOT-KEPT` already warns about on a stopped cold start (`streamer_coldstart_stop.go`), now on the database that is serving production. It also requires `REPLICATION` on `T`; on a managed Postgres that does not grant it, the reverse must fall to the trigger engine, which means `trigger setup` on the serving primary before the flip — capture triggers on every table of a database that has just become production, with the write amplification that implies (ADR-0066, [adr-0066-postgres-trigger-engine-variant.md](adr-0066-postgres-trigger-engine-variant.md), measured it; it is not free).
- **MySQL / MariaDB / PlanetScale source.** `P_T` is a GTID set (or file/pos on a GTID-less server). No slot is needed, so the reverse can be armed after the fact as long as the binlogs covering `P_T` are still retained — and the existing `GTID_SUBSET(resume, @@gtid_executed)` door with the `@@server_uuid` witness (`cdc_reader.go:2904`) already refuses a purged or foreign position loudly. This is the cheap case. On a Vitess/PlanetScale `T` the anchor is a VGTID and the same retention caveat applies per shard.
- **Trigger engines as source (pgtrigger, sqlite-trigger, d1-trigger).** `P_T` is the change log's `MAX(id)` at T_drain, which is only meaningful if the capture triggers are installed on `T` before any post-flip write — so again, setup on the serving primary before the flip.

So the arm-before-flip requirement is real for two of the three source families, and it puts sluice-owned objects (a slot, or triggers plus a change-log table) onto the production primary at the exact moment it becomes production. That is the first concrete piece of complexity, and it is operational rather than code.

## The reverse target already exists, and every door says so

The reverse stream's TARGET is `S`: populated, schema already present, not created by sluice. Three existing refusals fire on that shape by design:

- `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` (`preflight.go:124`) — a cold start refuses a populated target. The reverse is not a cold start; it must skip the copy entirely. So this door needs a **scoped exemption**, and CLAUDE.md's rule on moving or scoping a refusal applies in full: enumerate every call path that reaches the door today and state whether each still does. The exemption must be keyed on a proof (the rollback record below), never on a flag, or `--skip-copy` becomes the next silent-loss foot-gun (anyone could use it to "resume" a sync into a target that was never copied).
- `SLUICE-E-RESUME-FRESH-TABLE-NOT-EMPTY` — the resume-side twin; same treatment.
- The stream-identity and lineage doors (`streamer_foreign_lineage.go`, the DSN-fingerprint check on `ListStreams`) key on `S`'s control row, which does not exist because `S` was never a target. This is precisely the shape CLAUDE.md's 2026-09-09 entry describes: a new path whose entry condition is the negation of what the guards key on. Every guard in `sync start`'s dispatch has to be re-enumerated for the reverse path and re-implemented where its precondition does not hold.

None of this is hard individually. It is a lot of doors, each one silent-loss-class if missed, and the sibling-sweep record for exactly this fix shape is three consecutive releases with a leak.

## Loops, and sluice's own writes

If the forward stream is still running when the reverse starts, a change echoes `T → S → T`. The reverse must refuse unless `T`'s control row for the forward stream is FINISHED — not stopped, not draining. That check exists as data (`StreamStatus` via `ListStreams`) and needs only a door.

sluice's own control tables on `T` (`sluice_cdc_state`, the migrate breadcrumbs, the schema-history ledger, the heartbeat table) are written by the forward stream and would be read back by the reverse stream's source reader. The `IsControlTable` exclusion already applies at every schema reader (`postgres/schema_reader.go:683`, `mysql/schema_reader.go:253`, `sqlite/schema_reader.go:169`, `sqlite/d1_schema.go:110`), so the publication and the table scope exclude them by construction. This one is free — and it wants a pin, because the reverse is the first path where the exclusion is load-bearing for correctness rather than tidiness.

## The part that does not compose: cross-engine round-trips

Same-engine cutovers (a managed-Postgres version upgrade, a cloud-to-cloud move, MySQL to PlanetScale) have a clean reverse: the type system is the same on both sides, translation is identity, and the reverse stream is the forward stream with the DSNs swapped. Everything above is bookkeeping and doors.

Cross-engine cutovers are where the complexity stops being bookkeeping:

- **Round-trip fidelity was never a tested property.** sluice pins `MySQL → PG` and `PG → MySQL` each as faithful in their own direction. `x → y → x == x` is a different claim, and there are known non-injective translations on the forward legs: unsigned `BIGINT` to `NUMERIC(20)`, `ENUM`/`SET` to `TEXT + CHECK`, display widths and `ZEROFILL`, MySQL `TIME` beyond 24h (Bug 187 — refuses today), zero dates, JSON key normalisation (MySQL reorders keys; the bytes change, the value arguably does not), collation and PAD SPACE semantics. Each becomes a cell in a round-trip matrix that has to be built before a cross-engine rollback can be called lossless — the Bug 74 rule says every family × shape × both directions, ground-truthed on the real engine.
- **The reverse leg is not the inverse of the forward map; it is a fresh translation of app-authored values.** After T_flip the application writes to `T` natively. On a Postgres `T` that means `jsonb`, arrays, `uuid`, `inet`, range types, `timestamptz` with sub-microsecond behaviour the MySQL side never produced. The old MySQL `S` has no column that can hold them, because its columns were designed for the MySQL application. Under the value-fidelity tenet sluice must REFUSE-LOUD rather than coerce — which means **a cross-engine rollback stream can halt on the first post-flip write the new engine can express and the old one cannot, in exactly the window it exists to protect.** sluice cannot promise the application will stay inside the intersection of the two type systems, and neither, realistically, can the operator.
- **Sequences run in both directions.** New rows on `T` carry `T`-generated identity values; the reverse applies them explicitly into `S`'s `AUTO_INCREMENT` columns, so `S`'s counters lag and a rollback needs `sluice cutover` run in reverse at T_rollback. That exists (ADR-0062 is direction-agnostic) and is only a runbook line, but it is another step in a procedure that is already long.

So the honest shape of the complexity is: **moderate and mostly mechanical for same-engine pairs; open-ended for cross-engine pairs, with a failure mode (halt mid-rollback-window) that no amount of engineering removes**, because it is a property of the two type systems, not of sluice.

## Options

**Option 0 — Fix the doc; ship nothing else.** Rewrite `docs/cutover.md` §"Rollback after cutover" so it stops recommending a `migrate` that would refuse or drop. State plainly that a re-copy is the only supported reverse today, that it must complete before the flip to be a rollback path at all, and that the periodic-dump procedure is the honest alternative. Cost: one doc change. **This should land regardless of the decision on the rest**, because a doc that recommends a hazardous command is a finding on its own.

**Option 1 — The primitive: a supplied-position start, plus a stop that prints both positions.** `sync stop` (or the FINISHED transition) records and prints `P_S` and `P_T` with both instances' identity stamps. `sync start --anchor-position <token>` starts CDC from a supplied position with no copy, reusing the `--position-from-manifest` machinery minus the manifest, with the populated-target door scoped by the presence of the anchor AND a matching FINISHED forward row on `T`. The operator composes the reverse from a runbook. Size S–M. It is the building block of Option 2 and is useful on its own (it also gives the backup-chain path a manifest-less sibling). Its weakness is that the freeze discipline (invariant 3) stays a runbook sentence; the door that detects `S` having advanced past `P_S` is cheap and should ship with it.

**Option 2 — `sluice cutover --arm-rollback`: the choreography.** At the forward drain, sluice itself: verifies FINISHED, records a *rollback record* (`P_S`, `P_T`, both identity stamps, the exact table scope and `--where` set, the engine pair) in `S`'s control tables, creates the reverse anchor on `T` (slot / GTID record / trigger install, per engine), and prints the one command that starts the reverse. `sync start --rollback <record>` consumes it behind refusals: forward not FINISHED, `S` advanced past `P_S`, `T` lineage mismatch, scope mismatch, anchor gone (slot dropped / binlog purged). **Scoped to same-engine pairs**, with cross-engine refused loudly at arm time, citing the type-intersection reason above, until a round-trip matrix exists. Size M–L: the record and its doors are new; the anchor, the stop registry, lineage binding, schema-forward and cutover priming are reused. This is the "one-button" the doc says does not exist, for the pairs where it can be made honest.

**Option 3 — Bidirectional / active-active with conflict resolution.** Not this ADR. It is a different product with a different correctness model (last-writer-wins, vector clocks, origin filtering), and every tenet here argues against building it.

## What I would recommend, if asked

Land Option 0 now. If the operator's real cutovers are same-engine — version upgrades, provider moves, MySQL to PlanetScale — Option 2 scoped to same-engine is worth building, on top of Option 1 as its primitive, and the complexity is bounded: the doors are enumerable and the `-race` integration gate covers the concurrency. If the cutovers that matter are cross-engine, do not build the reverse stream; the failure mode is structural, and the honest rollback path there is the periodic dump plus a short freeze, which Option 0 should say in so many words.

The test that decides it is not a code test: **name the cutover this is for.** If it has a name and the pair is same-engine, build. If not, the doc fix is the whole deliverable.

## Loud-failure discipline (applies to any option beyond 0)

- A rollback stream that cannot prove equivalence at the drain refuses; it never starts from "now".
- A supplied-position start is refused unless a FINISHED forward row on the same pair, same scope, exists on the anchor side.
- Every refusal carries a grep-stable marker and a `SLUICE-E-` code; the populated-target exemption is keyed on the rollback record, never on a bare flag.
- The reverse's schema-forward inherits ADR-0091's refuse-on-unrecognised-DDL unchanged.

## Out of scope

Conflict resolution; a reverse stream started while the forward is still running; cross-engine reverse streams (refused, per Option 2); any change to the forward path's semantics.

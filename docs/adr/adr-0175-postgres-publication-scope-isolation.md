# ADR-0175: Postgres publication scope isolation (`--publication-name` + the clobber refusal)

## Status

**Accepted — implemented 2026-07-22.** Filed from a staged-migration design question: whether sluice supports bringing tables across in waves, with several concurrently-running streams scoped to disjoint table sets. It does on a MySQL-family source; on a Postgres source the shared publication made it a **silent-loss** shape.

**The bug was reproduced empirically before the fix landed, not only by code reading.** The Tier-1 gate (`internal/pipeline/publication_scope_conflict_pg_integration_test.go`) was written first and run against deliberately-disabled fix code: `TestPublicationScope_TwoStreams_IsolatedByPublicationName` failed at exactly the load-bearing assertion — *"wave A stopped receiving changes after wave B cold-started"* — and `..._ConflictRefusedWithoutIsolation` failed with *"the guard did not fire"*, while `..._WideningIsNotAConflict` **passed even pre-fix**, confirming the gate is targeted at the narrowing case rather than blanket. All three pass with the fix restored.

Shipped surface: `--publication-name` on `sync start` (default unchanged), `ir.PublicationScoper` + `postgres.Engine.WithPublicationScope`, the narrowing guard in `ensurePublication`, and `SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT`. Regression-checked against the other publication-touching paths: single-stream resume, cold-start (serial + parallel), multi-schema (`FOR ALL TABLES`), live `schema add-table`, and the backup chain-slot/incremental chain.

**Concurrency note:** this touches the streamer cold-start path and source-side catalog DDL, but adds no goroutines or shared mutable state. The `-race`-before-tag rule applies to the integration pins (two concurrent streamers against one source), not to the guard logic itself.

## Context

Postgres logical replication splits two concerns that sluice's naming has quietly conflated.

**The replication slot is the cursor and the WAL-retention lease.** It records `confirmed_flush_lsn` and pins WAL until then. It is per-consumer and durable. sluice treats it as per-stream and exposes it: `--slot-name` on `sync start`, threaded through `Engine.OpenCDCReaderWithSlot(ctx, dsn, slotName)`. The flag's help text explicitly advertises the multi-instance use case — *"Set per-instance to run multiple concurrent sluice instances against the same source — without distinct slot names they collide on the default."*

**The publication is the table filter.** It is a catalog object listing which tables `pgoutput` should emit; it holds no position and no state. sluice names it at stream start as a plugin argument (`cdc_reader.go:470`):

```go
pluginArgs := []string{
    fmt.Sprintf("proto_version '%d'", r.protoVersion),
    fmt.Sprintf("publication_names '%s'", r.publication),
}
```

**The publication half was never given an override.** `defaultPublication = "sluice_pub"` (`cdc_reader.go:43`) is a constant with no CLI flag, no config key, no env var, and no DSN parameter. Every production call site passes it literally — `engine.go:216`, `engine.go:251`, `engine.go:283`, `cdc_snapshot.go:147,152`, `backup_snapshot.go:143`. Neither public method carries a name in its signature:

```go
func (e Engine) EnsurePublication(ctx context.Context, dsn string, tables []string) error
func (e Engine) AddPublicationTables(ctx context.Context, dsn string, tables []string) error
```

The asymmetry is visible inside one struct literal (`engine.go:211-219`): `slotName` is a parameter threaded from the CLI; `publication` is a constant, three lines apart. The constant's own doc comment says the two names are what sluice uses *"when no override is supplied"* — accurate for `defaultSlot`, aspirational for `defaultPublication`. This reads as an oversight, not a decision.

### The silent-loss shape

At cold start the streamer scopes the publication to its own table list (`streamer_coldstart.go:105` → `ensurePublication`), which lands on:

```sql
ALTER PUBLICATION sluice_pub SET TABLE <this stream's tables>
```

`SET TABLE` **replaces the entire table set atomically** — the code documents this at `cdc_reader.go:2121`. So for two waves against one Postgres source:

1. `wave1` cold-starts for `{orders, order_items}`; publication scoped to those. Streams normally.
2. Later, `wave2` cold-starts for `{users, sessions}`; publication becomes `{users, sessions}`.
3. `wave1`'s slot is untouched and healthy. It keeps advancing, because WAL is still consumed and confirmed. But its tables are no longer in the publication, so **`pgoutput` emits nothing for them.**

There is no error and no warning. From Postgres's perspective a stream with no in-scope changes and a stream whose tables were yanked out of the publication are indistinguishable; `sluice sync status` / `sync health` both report a current, advancing stream. The target simply stops receiving changes for wave 1's tables while reporting healthy.

Nothing binds a publication to a slot — sluice's own code relies on this when justifying a drop-and-recreate (`cdc_reader.go:2176`): *"The drop is safe because the publication is metadata only — slots reference WAL by LSN, not by publication name binding."* That independence is exactly what makes the collision both possible and invisible.

The same reasoning applies to the `FOR ALL TABLES` → scoped path (`cdc_reader.go:2216-2229`), which **drops and recreates** the publication; doing so while another stream is reading through it is equally destructive.

Warm resume does not re-call `EnsurePublication` (`streamer_warm_resume.go:28`), so restarting wave 1 does **not** repair it — only a cold start would, which would then break wave 2. The failure is stable and mutually exclusive.

### Why this hasn't bitten yet

The one shipped shape with several PG-source streams on one source is the ADR-0122 fleet cold-start, which the create-race handling at `cdc_reader.go:2148` explicitly anticipates. That is safe *only because fleet streams share an identical scope* — the `SET TABLE` is a no-op rewrite. Differing scopes is the unguarded case, and staged/wave migration is the natural way an operator arrives at it.

MySQL, PlanetScale, and Vitess sources are unaffected: they have no publication, each stream opens its own binlog/VStream reader, and CDC state is keyed `PRIMARY KEY (stream_id)`.

### The shared-publication model has a ceiling (added 2026-07-21)

Postgres 15+ attaches **per-table attributes** to a publication — a column list and a row-filter `WHERE` clause, both part of the table's membership rather than separate objects. sluice uses neither today (`formatPublicationTableList` at `cdc_reader.go:2437` renders bare qualified identifiers), but [ADR-0176](adr-0176-pg-publication-row-filter-pushdown.md) proposes adopting row filters to push `sync --where` down to the source, and [ADR-0177](adr-0177-pg-publication-column-lists.md) records the column-list capability.

That reframes the conflict this ADR guards. Today the only thing two streams can fight over is the table **set**, which is why a narrowing check is sufficient. With per-table attributes in play, two streams with an **identical** table set but different `--where` predicates would clobber each other's filters — and a guard that fires on "tables were removed" would pass both cleanly while one stream silently inherits the other's predicate. Same silent-loss class, one level down.

Two consequences, both folded into the Decision below:

- The guard must key on the **full publication definition** (the table set *and* each table's attributes), not the table set alone — so it stays correct when attributes arrive rather than needing a second fix at that point.
- More strategically: **per-stream publications are the direction of travel, and the shared publication plus a conflict guard is a floor, not a destination.** A shared publication can only ever express one stream's intent per table; the moment intent becomes richer than "in scope / not in scope," sharing stops being expressible at all. This ADR still lands the guard first because it is non-breaking and closes a live silent-loss hole, but ADR-0176 should not be implemented on a shared publication.

## Decision

Two changes, in this order of importance.

### 1. Refuse the clobber (the load-bearing half)

Before a `SET TABLE` (or a `FOR ALL TABLES` drop-and-recreate) that would **narrow the publication's definition**, `ensurePublication` checks whether another sluice stream is reading through it. If so, it refuses with a new coded error rather than performing the rescope.

"Narrowing" is deliberately defined over the **whole definition**, not the table set: a rescope is narrowing if it removes a table, *or* (once ADR-0176/0177 land) if it changes any surviving table's row filter or column list. Today only the first arm is reachable, since sluice emits no per-table attributes — but the check is specified this way now so adopting attributes doesn't require re-deriving the guard's correctness. Concretely, the comparison is against the current definition read back from `pg_publication_rel` (plus `prqual` / `prattrs` when attributes exist), not against a remembered table list.

The detection signal is **other active sluice replication slots on the source**:

```sql
SELECT slot_name FROM pg_replication_slots
WHERE slot_name LIKE 'sluice\_%' AND active AND slot_name <> $1
```

This is a sound layer for the check because the cold-start ordering makes it clean: `EnsurePublication` runs **before** `OpenSnapshotStream` (deliberately, so the snapshot's slot pins a catalog snapshot that already has the scoped publication), so the calling stream's own slot does not yet exist at check time. The `<> $1` exclusion covers cold-start recovery shapes where it does.

The refusal fires only on **narrowing** — an incoming set that is a superset of, or equal to, the current set removes nothing and passes untouched. That keeps the fleet shape (identical scopes) and the additive `AddPublicationTables` path (`schema add-table`) working exactly as they do today.

Error message names the tables about to be dropped from scope, the active slot(s) implying another reader, and both remedies: give this stream its own `--publication-name`, or stop the other stream first.

New code: **`SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT`**, class `ClassRefusal`.

### 2. `--publication-name` (the escape hatch)

A `sync start` flag mirroring `--slot-name` exactly:

- Default unchanged (`sluice_pub`), so upgrades are not breaking.
- Same `sluice_` prefix convention — `--publication-name wave1` yields `sluice_wave1`, keeping every sluice publication findable with `pg_publication WHERE pubname LIKE 'sluice\_%'`.
- Inert on engines without publications (MySQL family), matching how `--slot-name` behaves there.

Threading: `Engine` already carries exactly one configurable field (`appID`) set through the copy-and-return `WithConnectionLabel` method, and the CLI already has an engine-option seam (`applySourceEngineOptions`). A matching `WithPublication(name)` follows that established pattern and **avoids changing the three method signatures**, which is why it is preferred over adding a name parameter to `EnsurePublication` / `AddPublicationTables`.

The documented pattern for staged migration becomes: pass `--publication-name` and `--slot-name` together, per wave.

### Why the default is not derived from the stream id

Deriving the default (e.g. `sluice_pub_<stream-id>`) would isolate streams automatically and need no flag. It is rejected because it is a **breaking upgrade**: `r.publication` is sent on every `START_REPLICATION`, including warm resume, so an existing stream would restart against a publication name that does not exist on the source. That fails loudly rather than silently, but it breaks every running PG deployment on upgrade to buy isolation that the refusal already provides safely. Operators who want per-stream isolation opt in with the flag.

## Consequences

- **The staged-migration shape stops being a silent-loss trap.** An operator cold-starting a second differently-scoped wave against one PG source gets a refusal naming the conflict, instead of a first wave that goes quietly dark while reporting healthy.
- **Concurrent PG-source streams with disjoint scopes become supportable** — with `--publication-name` per stream, which the refusal's message points at.
- **No behavior change for any current shipped shape.** Single streams, the ADR-0122 fleet (identical scopes), `schema add-table` (additive), and backup's `FOR ALL TABLES` path all either widen or leave scope unchanged, so the narrowing guard never fires. This is the property to pin hardest in tests.
- **One new source-side query per cold start** (`pg_replication_slots`, only on the narrowing branch) — negligible, off the streaming hot path.
- **`docs/operator/staged-wave-migration.md` loses its warning box** and gains the supported per-wave recipe. The sluicesync.com guide (`/docs/staged-wave-migration/`) mirrors it.

## Alternatives considered

- **Union semantics — never remove from the publication, only add.** Rejected. It is *correct* (the client-side `filterChanges` layer drops out-of-scope events anyway), but it makes the publication a monotonically growing union of every scope that has ever run against the source, which surprises anyone reading `pg_publication_tables`, silently inflates WAL decode volume, and erodes the per-table scoping Bug 13 / ADR-0021 introduced to keep a mid-stream `CREATE TABLE` out of the stream.
- **Per-stream publication as the default (no flag).** Rejected *for this ADR* as a breaking upgrade — see above. But note this is a deferral, not a dismissal: per "The shared-publication model has a ceiling," it is where the design ends up once per-table attributes ([ADR-0176](adr-0176-pg-publication-row-filter-pushdown.md)) make a shared publication structurally unable to express two streams' intent. The migration path that avoids the breaking upgrade is to ship `--publication-name` now (this ADR), adopt per-stream publications as the default *for streams that use per-table attributes only*, and leave the plain shared-`sluice_pub` default intact for streams that don't.
- **Detect conflicts from `sluice_cdc_state` instead of slots.** Rejected: that control table lives on the **target**, and `EnsurePublication` runs against the **source** with no target handle. Worse, waves may target different databases entirely, so no single target's state is authoritative about who is reading the source. Slot activity is the only signal available at the right layer.
- **Persist an explicit stream → publication-scope binding in control state and check that.** Originally deferred as the "airtight" follow-up; **now REJECTED (2026-07-23) in favour of existence semantics (see the residual-risk closure below)**. The binding table would have to live on the SOURCE (the target-split argument above), which (a) adds a persisted codec surface + lease/staleness semantics so a crashed stream doesn't wedge future cold starts, (b) breaks the "a publication and a slot, nothing else" source contract, and (c) — decisive — is `CREATE TABLE`-permission-gated on exactly the restricted managed sources where the guard matters most, so the guarantee would silently weaken where it is most needed. Slot EXISTENCE turns out to be the registry that was wanted all along: the slot is already the durable, source-side, per-stream object a stream owns for precisely as long as it intends to resume.
- **Document the hazard and change no code.** Rejected on tenets: a silent-loss shape reachable by a reasonable operator sequence is exactly what the project's loud-failure discipline exists to prevent. Documentation is the mitigation of last resort, not the fix.

## Testing

The pins that matter, in priority order:

- **The regression itself, end to end (integration, PG source).** Two streamers, disjoint `--include-table` scopes, one source. Today wave 1 goes silently dry; after the fix the second cold start refuses with the coded error, and with `--publication-name` set on each, **both** streams deliver their own tables' changes to their own targets. This is the non-vacuous pin — it must fail on the pre-fix code.
- **The no-op cases stay green** (the widest-blast-radius risk of this change): single-stream cold start; a re-scope with no other active slot; the ADR-0122 fleet shape with identical scopes; `FOR ALL TABLES` → scoped on a fresh source; `AddPublicationTables` via `schema add-table`. None may refuse.
- **Narrowing detection unit pins**: incoming ⊃ current (no fire), incoming == current (no fire), incoming ⊂ current (fire), disjoint (fire), plus the `FOR ALL TABLES` drop-and-recreate arm.
- **Own-slot exclusion** — the `slot_name <> $1` arm, so a cold-start recovery whose own slot is still registered doesn't self-refuse.
- **CLI-layer pin for `--publication-name` through the real kong parser**, including the `sluice_` prefix normalization and the MySQL-family inert case — per the "pin a value-gated fix THROUGH the CLI layer" lesson (Bug 180): a flag whose new branch is reachable only for a particular parsed value can be unreachable in practice while a direct-call unit test greens it.
- **`TestRegistryDocSync`** (`internal/sluicecode/sluicecode_test.go:128`) enforces the bidirectional registry ↔ `docs/operator/error-codes.md` table, so the new code requires its doc row in the same commit.

**Residual risk — CLOSED (2026-07-23, existence semantics).** As shipped in v0.99.287 the conflict signal was slot *activity*, a proxy: a conflicting stream whose slot was momentarily inactive — stopped mid-migration, or between retry attempts — was not detected, and the rescope proceeded. The closure changes the signal to slot **existence** (`otherSluiceSlots` — the `AND active` predicate dropped): a slot's existence IS the durable, source-side claim that a stream holds a scope and intends to resume, and "momentarily inactive" was precisely the window the activity predicate left open. A stream with NO slot must cold-start, and cold start re-asserts scope under this same guard — so the window is closed for every stream that can still be starved. The cost is a conservative refusal against a genuinely *abandoned* slot; that is accepted deliberately: an abandoned slot pins WAL and deserves operator attention anyway, the refusal labels each conflicting slot active/inactive, and it names all three escapes (`--publication-name`, drain the other stream, or drop the abandoned slot). Pinned by `TestPublicationScope_InactiveSlotStillConflicts` (an inactive slot alone must refuse, refuse-before-mutate asserted on `pg_publication_rel`). The explicit binding-registry alternative is now recorded as rejected (see Alternatives).

## Gate proposal (Tier 1)

Per the pre-release QA tiering in `CLAUDE.md`, the durable outcome of this finding is not the fix but a permanent gate. The candidate: **an integration pin that runs two concurrently-streaming PG-source streamers with disjoint scopes and asserts both targets receive their own changes.** It is cheap (one PG container, two streamers), it ground-truths the exact property that broke, and it belongs in the per-PR shard rather than an extended suite — the shared-catalog-object class it guards is not Postgres-specific in spirit, and any future engine that grows a shared source-side filter object should be added to it.

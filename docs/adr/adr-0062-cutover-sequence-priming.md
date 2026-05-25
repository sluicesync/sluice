# ADR-0062 — Two-phase cutover sequence priming (F10)

* Status: Accepted (2026-05-25)
* Severity: A (silent PK-collision class on the first post-cutover INSERT)
* Reddit-research finding: F10 (2026-05-22 run)
* Siblings: [ADR-0061 — Source-side heartbeat writer (F17)](adr-0061-source-side-heartbeat-writer.md); [ADR-0004 — Three-phase apply](adr-0004-three-phase-apply.md) — bulk-copy-time identity sync, the same class but a different lifecycle moment

## Context

When migrating from Postgres → Postgres (or Postgres → MySQL, or MySQL → MySQL), sluice copies the source's sequence / AUTO_INCREMENT state at bulk-copy time through the existing `SchemaWriter.SyncIdentitySequences` path (ADR-0004 §7) — the target's sequence gets advanced past `MAX(id)` of the bulk-copied rows so the next user-initiated INSERT on the target doesn't collide with bulk-copied IDs.

That covers the snapshot moment, but **not the cutover moment**. The mid-migration timeline looks like this:

1. **Snapshot** — sluice reads `MAX(id)` from the source's tables and bulk-copies the rows; `SyncIdentitySequences` advances the target's sequence past the snapshot's `MAX(id)`. Target sequence value: `S0` (the snapshot's last issued ID).
2. **CDC catch-up** — the source continues advancing sequences as new rows are inserted; sluice's CDC stream replicates each row (including its explicit id) to the target. **The CDC events carry the *values*; they do not advance the target's *sequence*.** Source sequence value: `Sn` (last issued at catch-up's end). Target sequence value: still `S0`.
3. **Cutover** — operator flips application traffic from source to target.
4. **First post-cutover INSERT on target** — the next sequence value the target hands out is `S0 + 1`. But row `S0 + 1` was already inserted on the source during catch-up and replicated via CDC to the target. **PK collision.** Operator surprise; looks like data corruption when it's actually a sequence-priming gap.

The operator-facing symptom: the application's first post-cutover write fails with `duplicate key value violates unique constraint` (PG) / `Duplicate entry for key 'PRIMARY'` (MySQL). The application owner has no obvious recovery path — manually running `setval(seq, MAX(id))` on each table is doable but error-prone, and there is no sluice-managed surface for it.

F10's promise: at cutover (operator-signaled — `sluice cutover`), sluice re-reads the source's sequence / AUTO_INCREMENT states and applies them to the target with a safety margin. The first post-cutover INSERT lands at `source_value + margin + 1`, well clear of any in-flight catch-up activity.

## Decision

A new `sluice cutover` subcommand drives a two-phase sequence priming pass:

* **Phase 1 — read.** Open the source's `SchemaReader`, type-assert to `ir.SequenceStateReader`, and read the current `last_value` / `AUTO_INCREMENT` for every identity-tagged column in the source schema.
* **Phase 2 — apply.** Open the target's `SchemaWriter`, type-assert to `ir.SequencePrimer`, and apply each state with a safety margin (default 1000). The primer guards each apply with a target-side read so re-runs are idempotent — see "Idempotency contract" below.

The two phases run in a single command invocation; the operator drives `sluice cutover` once. There is no daemon / no background re-run loop / no auto-detection of "the cutover moment". F10 is **strictly operator-invoked**, sibling to F17 in that respect (and unlike F13, which is passive and on-by-default).

### Engine surface (IR)

Two new optional interfaces, both implemented on existing engine surfaces:

```go
// On SchemaReader (source side):
type SequenceStateReader interface {
    ReadSequenceState(ctx context.Context, schema *Schema) ([]SequenceState, error)
}

// On SchemaWriter (target side):
type SequencePrimer interface {
    PrimeSequences(ctx context.Context, schema *Schema, sourceStates []SequenceState, margin int64) (*SequencePrimeReport, error)
}

var ErrCutoverSequenceTargetAhead = errors.New("cutover: target sequence is ahead of source ...")
```

`SequenceState` carries `(Table, Column, Value)`. `Value` is canonicalised to **last-issued** across engines — PG's `pg_sequences.last_value` (when `is_called=true`) maps directly; MySQL's `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` (which reports the *next* value) is decremented by 1.

`SequencePrimeReport` is the per-table outcome shape: each entry has a `(Table, Column, SourceValue, TargetBefore, TargetAfter, Outcome, Reason)`. Outcomes are `primed` / `noop` / `skipped` / `refused`. The CLI renders the report to stdout in text or JSON.

The optional shape lets engines without a sequence concept silently omit both methods; the orchestrator surfaces a clear error ("engine X does not implement SequenceStateReader") at cutover-invocation time rather than mid-flight.

### Pipeline wiring

A new `pipeline.Cutover` type drives the orchestration:

* Opens source `SchemaReader`, type-asserts `SequenceStateReader`, reads source schema, reads sequence states.
* Opens target `SchemaWriter`, type-asserts `SequencePrimer`, threads `--target-schema` via the existing `ir.SchemaSetter` (ADR-0031).
* Applies the operator's `--include-table` / `--exclude-table` filter to scope the priming pass — the standard sluice filter, same shape `sluice migrate` uses.
* Returns the per-table `SequencePrimeReport` and the engine's top-level error (non-nil when at least one table refused loudly).

The CLI subcommand validates flags, opens engines via the registry, calls `Cutover.Run`, renders the report, and propagates any refusal exit code.

### Idempotency contract

Running `sluice cutover` twice does NOT regress sequence values. The contract is load-bearing — operators recovering from a partial network failure during the first invocation must be able to re-run without thinking, and the tool must not silently roll a sequence backwards.

The engine implementation's decision tree (per column):

* `targetBefore ≥ applyValue + margin` → "refused" — target is ahead of source by more than the idempotency tolerance. This catches the "operator already INSERTed post-cutover" scenario where a forward bump would risk a collision.
* `targetBefore ≥ applyValue`          → "noop" — target is already at or above the would-be apply point. Idempotent re-run lands here on the second invocation (where the first invocation set the target to `source + margin`).
* `targetBefore < applyValue`          → "primed" — apply via `setval` (PG) / `ALTER TABLE AUTO_INCREMENT = N` (MySQL).

The tolerance equals the operator-supplied margin. An idempotent re-run within `margin` rows of the first run does NOT trigger the refusal; operator INSERTs that advanced the target by more than `margin` rows since the last priming pass do.

### Safety margin

Default: 1000.

The margin gives operator headroom against two timing windows:

1. **Source-side INSERT activity between Phase 1's read and Phase 2's apply.** Even with a quiesced source, a few in-flight transactions can advance the source's sequence between the catalog read and the target's `setval`. 1000 absorbs any realistic peri-cutover write rate.
2. **Operator's first post-cutover INSERT.** Some catch-up activity may still be in flight at the moment the operator flips traffic; the margin guarantees the first new INSERT lands well clear of the highest CDC-replicated id.

Operators driving cutover under significant write load can raise the margin via `--cutover-sequence-margin=N`. The same value doubles as the idempotency tolerance — operators raising it explicitly accept that the refusal class triggers later.

## Loud-failure discipline

Four classes have clean handling:

1. **Source engine doesn't implement `SequenceStateReader`** (or target doesn't implement `SequencePrimer`). Action: refuse loudly at command invocation with a clear "engine X does not support cutover sequence priming" error, naming the missing surface.
2. **Source column declares identity but the catalog has no owning sequence.** Action: emit a "skipped" action with a reason ("source has no owning sequence — composite PK / UUID PK / manually-managed identifier"). The orchestrator does NOT refuse; the column is simply not eligible.
3. **Target sequence is ahead of source by more than margin.** Action: emit a "refused" action with an operator-actionable reason ("manual re-snapshot recommended"), surface `ErrCutoverSequenceTargetAhead` at the top level, exit non-zero. The CLI renders the report's per-table detail before exiting so the operator sees exactly which tables refused.
4. **Catalog query fails** (network, permissions, etc.). Action: propagate the wrapped error up the orchestrator; the partial report (whatever did succeed) is still rendered to stdout so operators piping to a metrics scraper see the per-table detail of the partial pass.

## Alternatives considered

* **Auto-detect cutover via time-based heuristic or CDC-position-vs-source-position gap.** Rejected: F10 is a strictly-operator-controlled lifecycle moment. An auto-detect would trade operator-clarity for a fragile signal that depends on the CDC stream's apply rate, which is exactly what's varying during cutover. Operators want predictable, scripted behaviour.
* **Replicate sequence advances through CDC.** Postgres' logical replication carries row-level changes, not sequence-DDL events. Adding a sequence-watch loop to sluice's CDC reader is a much larger change (per-engine catalog poll + apply event type + idempotency on the apply path) for a problem the simple periodic prime solves with much less surface area. Documented as future work: a continuous prime loop could remove the operator-invoked moment entirely, but v1 is opt-in.
* **Apply the prime as part of `sync stop --wait`.** Rejected: `sync stop` is a CDC drain signal; folding sequence priming into it conflates two operator-visible lifecycle moments. The two-step shape (`sync stop --wait` then `cutover`) makes the prime explicit in operator runbooks; operators who don't want it just don't run it.
* **Make the safety margin configurable per-table.** Rejected for v1. The 1000 default is generous enough for any realistic per-table write rate; operators on a single table with an extreme write rate can raise the global flag. A per-table knob can land later if operator demand surfaces.
* **PG: use `pg_sequence_last_value(regclass)` instead of `pg_sequences`.** `pg_sequence_last_value` is documented but lives outside the standard catalog views; `pg_sequences` is the user-facing view since PG 10 and matches MySQL's `INFORMATION_SCHEMA` shape. The reader uses `pg_get_serial_sequence` to resolve column → sequence and `pg_sequences` to read state.

## Implementation notes

* **Postgres engine** (`internal/engines/postgres/cutover_sequence.go`): the reader walks each identity column, resolves the owning sequence via `pg_get_serial_sequence(qualified_table, column)`, splits the qualified result via `splitQualifiedSequence`, and reads `last_value` / `is_called` from `pg_sequences`. The primer guards `setval` with a target-side read; `setval` itself is not monotonic, so the guard is load-bearing.
* **MySQL engine** (`internal/engines/mysql/cutover_sequence.go`): the reader queries `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` with `SET SESSION information_schema_stats_expiry = 0` to bypass the catalog cache. The primer uses `ALTER TABLE ... AUTO_INCREMENT = N` — MySQL clamps backwards-going values to the current counter, so even without the noop guard the primer cannot regress; the explicit guard is for predictable report output.
* **The PG sequence-name parser** (`splitQualifiedSequence`) handles the four shapes pg_get_serial_sequence produces: bare lowercase (`public.users_id_seq`), both-quoted (`"Public"."Widgets_id_seq"`), one-side-quoted (`public."Widgets_id_seq"`), and the pathological dot-in-quoted-name case. Pinned via a unit-test table to make adding new cases easy.

## Tests

* **Unit tests** (`internal/pipeline/cutover_test.go`) — validate-first ordering, capability-gating refusals (source missing `SequenceStateReader` / target missing `SequencePrimer`), state routing through the orchestrator, margin normalisation, target-schema plumbing, table-filter scoping, refusal-error propagation.
* **IR-level unit tests** (`internal/ir/cutover_test.go`) — `HasRefusals` shape, default-margin pin.
* **PG engine unit tests** (`internal/engines/postgres/cutover_sequence_test.go`) — sequence-name parser table, quote-unescape edge cases.
* **PG integration tests** (`internal/engines/postgres/cutover_sequence_integration_test.go`) — cold-prime + idempotent re-run + refusal + composite-PK skip. Verifies the next-issued ID matches `source + margin + 1` via probe `nextval()`.
* **MySQL integration tests** (`internal/engines/mysql/cutover_sequence_integration_test.go`) — same matrix as PG against `AUTO_INCREMENT`.
* **Cross-engine integration test** (`internal/pipeline/cutover_cross_integration_test.go`) — PG → MySQL handoff, pins the canonicalisation contract (PG `last_value` → MySQL `AUTO_INCREMENT`) end-to-end through the cutover orchestrator.

The "pin the class, not the representative" lesson from Bug 74 applies: the integration matrix exercises **both engines × {cold prime, idempotent re-run, refusal, skip}**. The cross-engine pin exercises the canonicalisation seam (PG → MySQL); the converse direction (MySQL → PG) shares code paths through the same orchestrator and is not separately pinned for v1.

## Out of scope

* **Auto-detect cutover.** Strictly operator-invoked.
* **Sequence changes detected mid-stream (CDC sequence-advance events).** Out-of-scope for v1; the simple periodic prime closes the practical gap.
* **Per-table safety margin.** v1 has one global knob.
* **Reverse-direction cutover (target's sequence ≫ source's, operator wants to *roll back* target to source).** F10 explicitly refuses this direction (it's the "target ahead" refusal class); operators wanting a roll-back run a full re-snapshot.
* **Composite-key tables with a non-identity component.** The skipped path is correct: if the IR doesn't tag identity on any column, there's nothing to prime. Operators using surrogate identity columns separately from a multi-column PK get the standard primed path on the surrogate.

## Roll-out

Minor release (v0.82.x). Operators see the new `sluice cutover` subcommand in `--help`; nothing changes for operators who don't run it. The release notes call out F10 as the operator-invoked cutover safety net, sibling to F13/F17 in the Reddit-research severity-A close-out arc.

The existing `SyncIdentitySequences` (ADR-0004 §7) is unchanged — it continues to handle the bulk-copy-time identity sync. F10 is **additive**: a second lifecycle moment, deliberately invoked by the operator, that closes the catch-up-window gap the bulk-copy-time sync can't see.

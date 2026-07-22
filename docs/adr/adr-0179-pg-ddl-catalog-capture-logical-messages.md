# ADR-0179: Source-side DDL catalog capture for Postgres CDC (event trigger → logical message)

## Status

**Proposed (Discovery — design of record, NOT adopted).** Filed 2026-07-22 after an investigation into why column-DEFAULT changes are not forwarded mid-sync established that the cause is structural, not a missing branch: **pgoutput's `Relation` message does not carry the catalog fields sluice would need.**

Nothing here is built, and on current demand nothing should be. This ADR exists for two reasons: to record the **failed approach** in enough detail that nobody repeats it, and to record the **known-correct design** so that if demand arrives the work starts from the right mechanism rather than the obvious-but-wrong one.

The design is not original — it is the approach the [supabase/etl](https://github.com/supabase/etl) project uses, read at commit `6d21f3f`. Credited throughout; the analysis of how it would land in sluice is ours.

## Context

### The gap

`sluice sync` forwards source DDL to the target by default (ADR-0091). The shape catalog covers ADD / DROP / RENAME COLUMN, ALTER COLUMN type, ALTER COLUMN nullability, CREATE/DROP INDEX, and the CHECK family. It does **not** cover a column's `DEFAULT`.

A source-side `ALTER TABLE t ALTER COLUMN c SET DEFAULT 5` is *silently skipped*: `diffAlteredColumn` (`internal/pipeline/shard_consolidation_probe.go`) compares exactly two per-column fields — `Type` and `Nullable` — so a default-only delta fires no delta class, `ClassifyShape` returns `ShapeKindNone`, and the intercept takes its no-op pass-through. The target keeps the stale default indefinitely.

**Severity, stated precisely.** This is *not* silent data loss. CDC carries explicit row values, so replicated rows are unaffected, and `sluice schema diff` already detects the drift (`ExpectedDefault` / `ActualDefault` / `DefaultLowConfidence`). The divergence materialises **at cutover**: once the application writes to the target, an `INSERT` omitting the column gets the *old* default. That matters because cutover is sluice's primary use case. The tenets-relevant part is the **silence** — `ShapeKindNone` asserts "nothing changed" when something did, while every other unsupported shape refuses loudly.

### The approach that does not work (recorded so it is not retried)

The obvious fix is to add `Default` to `diffAlteredColumn`, introduce `ShapeKindAlterColumnDefault`, and dispatch to a new applier. This was implemented and then **reverted**, because it is simultaneously dangerous and ineffective.

The Postgres CDC path builds its schema snapshot from `projectRelation(rel)` over pgoutput's `RelationMessage`. That projection is:

```go
cols = append(cols, relationColumn{
    Name: c.Name, OID: c.DataType, TypeMod: c.TypeModifier,
    Type: t, KeyColumn: c.Flags&1 != 0,
})
```

There is no `Default` field, because the wire message has no default to carry. Consequently:

- **Dangerous.** At the seed→CDC boundary the `pre` comes from the full `SchemaReader` catalog read (real defaults) and the `post` from pgoutput (nil defaults). Every column-bearing table classifies as a default change, and the applier emits `DROP DEFAULT` — destroying the target's defaults on the first boundary of every sync.
- **Ineffective.** At CDC→CDC boundaries both sides are nil, so a real `SET DEFAULT` is still never detected.

This is the same trap as Bug 83, which the `AlterAddColumn` implementation already documents: pgoutput does not carry `pg_attribute.attnotnull` either, so every CDC-projected column has `Nullable=false` by zero value, and the PG writer force-sets `Nullable=true` on ADD COLUMN to compensate.

Corroborating evidence was already in the tree: `schema_forward_engage.go`'s `defaultProber` queries the **source** for a column's canonical default, called from the intercept when `def == nil` — it exists precisely because the CDC IR's defaults are empty.

MySQL is the same story from the other direction: its forward-path IR is built from the binlog table map plus a type/nullability signature read, with no `Default` anywhere in its CDC reader.

**The generalisable lesson:** a shape classifier can only classify what the change stream actually carries. Before extending the shape catalog with a new column attribute, confirm that attribute survives into the CDC-projected IR on every source that would use it.

### How supabase/etl solves it

They hit the same wall — which is *why* the machinery exists — and did **not** reach for a client-side catalog probe. They push catalog state into the WAL.

`crates/etl/migrations/source/20260415100000_schema_change_messages.up.sql` installs a DDL event trigger on the source:

```sql
create event trigger supabase_etl_ddl_message_trigger
    on ddl_command_end
    when tag in ('ALTER TABLE')
    execute function etl.emit_schema_change_messages();
```

The function reads the real catalog — `pg_attribute` joined to `pg_attrdef`, with `pg_get_expr(ad.adbin, ad.adrelid)` for the default body — builds a JSONB snapshot (`attname`, `atttypid`, `atttypmod`, `attnotnull`, `atthasdef`, `default_expression`, `attidentity`, `atthasmissing`), and emits it **in-band**:

```sql
perform pg_catalog.pg_logical_emit_message(
    true, 'supabase_etl_ddl', convert_to(v_msg_json::text, 'utf8'));
```

The leading `true` is the load-bearing detail: a **transactional** logical message. Their own comment explains the discipline around it — the function *"intentionally avoids PL/pgSQL EXCEPTION handlers to keep `pg_logical_emit_message()` in the top-level transaction and preserve the expected ordering of DDL messages relative to relation and DML events."*

The consumer decodes it as a logical-replication `Message` and feeds the same `build_table_schema` used for bootstrap, so the catalog-read path and the DDL path produce identical schema values by construction.

## Decision

**Do not build this now.** The gap is real but narrow (cutover-time only, already detectable via `schema diff`), and the fix carries a privilege and source-footprint cost that is disproportionate to it on current demand.

**Record the event-trigger design as the design of record**, and explicitly **reject the client-side catalog probe** as the alternative.

### Why the probe is rejected

A client-side probe — query the source catalog on each DDL boundary — needs no source-side objects and no elevated privilege, which makes it superficially attractive. It is rejected because it **cannot be ordered**. By the time the probe runs, the source may have applied further DDL, and the observed state has no position in the change stream: there is no way to say *"this default took effect between these two changes."* It buys real complexity and still cannot answer the question correctly.

The in-band transactional message has exactly the property the probe lacks: it is positioned where the DDL happened, relative to the DML around it.

### The design, if demand arrives

1. **Opt-in capability, never default.** A new source-side surface on the slot-based `postgres` engine, off unless the operator enables it. The engine's current contract is "a publication and a slot, nothing else"; this changes that and the operator must choose it.
2. **Loud refusal when unavailable.** `CREATE EVENT TRIGGER` requires superuser (or a platform-granted equivalent such as `rds_superuser`). Where the privilege is absent, refuse with a coded error naming the remedy — never degrade silently to the current no-op.
3. **Reuse the existing capture shape.** One transactional `pg_logical_emit_message` per affected table per `ALTER TABLE`, carrying a catalog snapshot; keep the payload deliberately broader than what is consumed so the message can grow without a redesigned trigger.
4. **One builder, two paths.** The bootstrap catalog read and the DDL message must construct the same IR through the same code, so they cannot drift — the property supabase/etl gets from a shared `build_table_schema`.
5. **Borrow their scope discipline.** Only `ALTER TABLE`; exclude dropped and generated columns; treat the captured statement text as debug-only, never as replayable DDL; ship an emergency opt-out so a session can bypass logging during recovery.
6. **Join the object inventory.** Every object installed lands in `docs/database-objects.md`, with teardown — supabase/etl's `etl-api` has explicit event-trigger drop logic and sluice's `trigger teardown` precedent applies.

### It closes more than defaults

Worth weighing if this is ever costed: the same payload carries `attnotnull`, which is the gap behind Bug 83 — the PG writer force-sets `Nullable=true` on forwarded ADD COLUMN *because* pgoutput omits it. One mechanism would close the DEFAULT gap and the nullability-fidelity gap together, and `attidentity` would follow. That materially improves the return, and argues for costing it as "PG DDL fidelity" rather than "the default gap."

## Consequences

- **The gap stays open and documented.** `sluice schema diff` remains the detection path; the operator reconciles defaults at cutover. This must be said plainly in the operator docs rather than left as an unstated limitation.
- **The dead end is recorded.** The next person to notice that defaults aren't forwarded finds the reason and the reverted approach, instead of re-deriving both.
- **The precedent is not free.** sluice already installs source-side objects for the trigger-CDC engines, so this is not unprecedented — but it *is* a change to the slot-based engine's contract, and "contain Postgres complexity" argues for surfacing it explicitly rather than auto-handling it.
- **If built, the `sluice`-side consumer is the smaller half.** The streamer already handles a schema-snapshot boundary; the work is the source-side SQL, the privilege preflight, the capability declaration, and the teardown story.

## Alternatives considered

- **Extend the shape classifier with `Default`.** Implemented and reverted — dangerous and ineffective (§ "The approach that does not work"). Retained here as the record.
- **Client-side source-catalog probe on each boundary.** Rejected: unorderable (§ "Why the probe is rejected").
- **Refuse loudly on a suspected default change instead of forwarding.** Attractive on tenets — silence is the defect — but unreachable for the same structural reason: sluice cannot *detect* the change from the CDC stream, so there is no point at which to refuse. A refusal needs a signal, and there isn't one.
- **Forward defaults only on sources where the IR carries them.** No such source exists today on the CDC path — neither PG nor the MySQL family carries defaults into the forward-path IR — so this reduces to "do nothing" with extra machinery.
- **Document + `schema diff` (status quo).** Chosen, for now.

## Testing (if built)

- **Non-vacuity first.** A pin that fails on pre-fix code: source `SET DEFAULT`, assert the target's default follows. Today it fails because nothing forwards; it must fail for that reason and not because the test never ran.
- **The seed→CDC boundary explicitly**, since that is where the reverted approach would have emitted `DROP DEFAULT` on every table. Assert no DDL is emitted when nothing changed.
- **Literal and expression defaults**, plus `DROP DEFAULT`, plus a default whose expression has no portable target form — which must refuse rather than silently drop a default the source still has.
- **Privilege-absent path** refuses with the coded error instead of silently no-op'ing.
- **Teardown** removes every installed object, verified against the catalog.

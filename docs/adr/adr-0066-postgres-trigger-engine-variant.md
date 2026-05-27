# ADR-0066: `postgres-trigger` engine variant for slot-restricted PG sources

## Status

**Proposed (2026-05-26).** Design-pass before code. Establishes the engine
contract and the operator surface so the implementation chunk can be sized
without re-litigating the shape. Depends on no other unmerged ADR; composes
with the existing `postgres` engine via the embedding pattern this ADR
specifies (§9). Pairs with ADR-0010 (idempotent applier — the polling
applier inherits its replay tolerance directly) and ADR-0007 (position-
and-data atomicity — the polling reader's "watermark" is a `Position` in
the same shape every other engine uses).

## Context

The existing `postgres` engine consumes change events via PG's logical
replication: pgoutput plugin, replication slot, REPLICATION role, and
`wal_level=logical` on the source (ADR-0006). That is the correct
mechanism for self-hosted PG and for managed services that expose it
(RDS Postgres, CloudSQL Postgres, Aurora Postgres, Crunchy, Supabase
Pro, Neon).

It is the wrong mechanism — because it does not work at all — for a
class of managed PG offerings that deliberately lock down replication
slots. Heroku Postgres Essential, Render Postgres Basic, Supabase's
free tier, several DigitalOcean managed-database tiers, and any shared
PG instance whose tenants share `wal_level` configuration fall in this
class. The operator on such a tier has standard DML privileges and a
connection string, and nothing else. They cannot:

- Create a replication slot (`pg_create_logical_replication_slot()`
  refuses without the REPLICATION role attribute, or the tier denies
  it outright).
- Run `ALTER SYSTEM SET wal_level=logical` (no superuser).
- Install third-party plugins (`pglogical`, `wal2json`, `decoderbufs`).
- Use `pg_dump`/`pg_basebackup` for replica-class operations (the tier
  may or may not allow `pg_dump` for ad-hoc export, but it does not
  yield a replication stream).

For these users sluice today says "your source does not support CDC,
use a tier that does." That is a real operator who would otherwise be
in our addressable market — a SaaS team on Heroku Postgres Essential
that wants to migrate to RDS, or a side-project on Render Basic
graduating to managed PG. Bucardo (Perl, trigger-based, Endpoint-
maintained since 2007) has been the de-facto answer for this scenario
for fifteen years. Bucardo works — but its operational surface (Perl
runtime + Bucardo daemon process + a control database separate from
the source + manual sync configuration via the `bucardo` CLI) is the
standard complaint in every operator account this author has read.
Operators describe it as "set up once, never touch, rebuild from
scratch if it breaks."

The opportunity is a Go-native, sluice-integrated equivalent. The IR
contract already accommodates trigger-based CDC: `ir.Capabilities.CDC`
has a `CDCTriggers` enum value reserved for exactly this case (see
`internal/ir/capabilities.go`), and `ir.CDCReader` is a generic
`StreamChanges(ctx, Position) → chan Change` — the source mechanism
is private to the engine. A new engine package that polls a sluice-
managed change-log table can satisfy the same contract that pgoutput
satisfies, and slot into the orchestrator, applier, position-store,
schema-history, and resume machinery sluice already has, with no
changes to the orchestrator.

The "Contain Postgres complexity" tenet (CLAUDE.md) cuts both ways
here: the existing PG engine is opinionated about *not* propagating
replication-slot quirks to users who don't need them, and a trigger
engine has its own opinions to declare (about DDL, polling cadence,
change-log retention) that should be surfaced loudly via Capabilities
and pre-flight checks, not silently auto-handled. The bar is "this
engine refuses cleanly the moment it cannot guarantee correctness,"
not "this engine accepts everything pgoutput accepts."

## Decision

A new engine package `internal/engines/pgtrigger` implements `ir.Engine`
under the registered name **`postgres-trigger`**. It composes the
existing `postgres` engine for the schema-read, schema-write, row-read,
and row-write surfaces (§9) and supplies its own `CDCReader` and
`ChangeApplier` for the trigger-based capture and the polling-driven
apply path. The engine is registered with `ir.Capabilities.CDC =
ir.CDCTriggers` and a tightened feature surface (§8) that refuses the
features it cannot honor.

The full decision is the sum of §1–§15 below. Where a sub-decision
involves a tradeoff against an alternative, the alternative is
explicitly named and rejected with a stated reason.

### 1. Engine name and registration

**One engine, one name: `postgres-trigger`.** No flavor sub-variants
in v1.

Rejected alternatives:

- `postgres-bucardo`: ties our name to a foreign tool. Operators who
  find sluice via "Bucardo alternative" will type `bucardo` anyway;
  we don't need it in the engine name.
- `pg-trigger` / `pgtrigger`: ambiguous with our internal package name.
  Operators have to type the engine name on every `sluice` invocation
  via `--source-driver`; the longer-but-unambiguous form is worth the
  characters.
- Service flavors (`postgres-trigger-heroku`, `postgres-trigger-render`):
  the MySQL flavor split exists because PlanetScale exposes a
  *different SQL surface* (no `LOAD DATA INFILE`, VStream instead of
  binlog). Heroku and Render expose the *same SQL surface* — they
  differ only in what's locked down. The lockdowns affect the
  `postgres` engine's eligibility, not `postgres-trigger`'s
  capabilities. One name suffices; if a specific service surfaces a
  meaningful capability difference later, the ADR-0005 flavor split
  is the established pattern to reach for.

The engine self-registers via `init()` and is wired into `cmd/sluice/main.go`
with a blank import. The package depends on `internal/engines/postgres`
(composition) and on `internal/ir`. No other dependency is permitted
under the architecture's dependency-direction rule.

### 2. Change-log table schema

A single sluice-managed table holds every captured change:

```sql
CREATE TABLE sluice_change_log (
    id            BIGSERIAL PRIMARY KEY,
    txid          BIGINT NOT NULL,                        -- pg_current_xact_id()::text::bigint
    committed_at  TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    schema_name   TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    op            CHAR(1) NOT NULL,                       -- 'I' / 'U' / 'D' / 'T' (truncate)
    pk_jsonb      JSONB NOT NULL,                         -- {"col": value, ...}
    before_jsonb  JSONB,                                  -- old row (UPDATE / DELETE)
    after_jsonb   JSONB                                   -- new row (INSERT / UPDATE)
);
CREATE INDEX sluice_change_log_id_idx ON sluice_change_log (id);
CREATE INDEX sluice_change_log_table_idx ON sluice_change_log (schema_name, table_name, id);
```

Column-by-column rationale:

- **`id BIGSERIAL`** is the polling watermark. The engine's
  `ir.Position` for this engine wraps the most-recently-committed
  `id` value as the durable bookmark. Postgres `BIGSERIAL` is
  monotonically allocated per `nextval()` call inside the trigger,
  which means **`id` order is allocation order, not commit order** —
  two overlapping transactions can allocate `id=5` and `id=6`
  respectively, then commit in `6, 5` order, and a poll observing
  `id=6` before the `id=5` row is durable would skip the `id=5`
  row forever. The polling reader handles this with a
  configurable safety lag (§6) that holds back any `id` strictly
  newer than `MAX(id) WHERE txid IN (SELECT * FROM pg_xact_committed_xmin())`
  — concretely, `SELECT id FROM sluice_change_log WHERE id <= last_acked
  AND xmin < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY id`. This
  is the same allocation-vs-commit hazard Debezium's PG engine documents
  for its "incremental snapshot" mode and the trigger engine adopts
  the same mitigation.

- **`txid BIGINT`** captures `pg_current_xact_id()::text::bigint`.
  Stored for two reasons: (a) operators debugging mid-stream loss can
  group rows by source transaction; (b) the safety-lag query above
  reads it. Stored as BIGINT not XID8 so the same shape works against
  PG 12 sources (XID8 is PG 13+).

- **`committed_at TIMESTAMPTZ`** is `statement_timestamp()` at trigger
  fire (not `clock_timestamp()` — we want statement-stable timestamps
  within a transaction). Operator-readable; not load-bearing for
  correctness.

- **`schema_name`, `table_name`** identify the source table. The
  trigger function reads them from `TG_TABLE_SCHEMA` / `TG_TABLE_NAME`
  (§3 below).

- **`op CHAR(1)`** is one of `I`/`U`/`D`/`T` for Insert/Update/Delete/
  Truncate. Single-char to keep the table small; the trigger function
  writes the literal, no enum type indirection (avoids a `CREATE TYPE`
  step that some restricted tiers may refuse).

- **`pk_jsonb JSONB`** is the primary key as a JSON object. Carried
  separately from `before_jsonb` / `after_jsonb` because Delete events
  on tables with sluice's `OLD-style` behavior (REPLICA IDENTITY DEFAULT)
  give us only the PK, not the whole row — keeping PK separate means
  Delete events with only PK info don't have to claim a `before_jsonb`
  shape they couldn't fill in. For composite PKs the object holds
  multiple keys.

- **`before_jsonb JSONB`** is the row in its pre-update form (UPDATEs
  and DELETEs). NULL for INSERTs.

- **`after_jsonb JSONB`** is the row in its post-update form (INSERTs
  and UPDATEs). NULL for DELETEs and TRUNCATEs.

The `(schema_name, table_name, id)` covering index makes the operator-
facing diagnostic queries (`SELECT * FROM sluice_change_log WHERE
table_name = 'orders' ORDER BY id DESC LIMIT 100`) fast on a busy
log; the primary `(id)` index is what the polling reader scans.

**Integration with `ir.ChangeEvent`.** The engine's `CDCReader.StreamChanges`
returns `chan ir.Change` (`Insert`/`Update`/`Delete`/`Truncate` from
`internal/ir/change.go`). The reader's projection from `sluice_change_log`
rows to `ir.Change` values is straightforward — the JSONB columns
decode via PG's `jsonb` driver path into Go `map[string]any`, which
is exactly the `ir.Row` shape. The `pk_jsonb` field is merged into
`Before` / `After` for Delete / Update events that lack the full row
image (operators with REPLICA IDENTITY DEFAULT see PK-only Delete
events; that's the existing pgoutput behavior too).

**Schema migration if sluice updates the change-log shape.** The
table is sluice-owned; sluice can do its own `ALTER TABLE
sluice_change_log ADD COLUMN ... DEFAULT ...` on a future major bump.
A separate `sluice_change_log_meta` one-row table tracks the schema
version of the change-log itself, on the same pattern `sluice_cdc_state`
uses for engine bookkeeping. A version mismatch between the engine
binary and the on-source table refuses loudly at startup with the
sluice-version pin needed for compatibility.

### 3. Trigger function emission

**One shared `plpgsql` function dispatched by `TG_TABLE_NAME` / `TG_TABLE_SCHEMA`,
plus one trigger per replicated table referencing that function.**

Rejected alternative: per-table trigger functions. A 200-table source
would have 200 functions to keep in sync; any future change to the
capture shape would require regenerating all of them. One function
keeps the surface tiny and the migration story trivial.

The function shape (representative; the production version will use
identifiers via `format(%I, ...)` and quote-safety helpers):

```sql
CREATE OR REPLACE FUNCTION sluice_capture_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER       -- so source tables can be replicated by a role that
                       -- doesn't own them, as long as the function-owning
                       -- role has INSERT on sluice_change_log
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_pk JSONB;
    v_before JSONB;
    v_after JSONB;
BEGIN
    -- PK column-set discovered at CREATE TRIGGER time and stored in
    -- TG_ARGV[0] as a JSON array of column names; the function reads
    -- it back here to project just the PK columns out of OLD/NEW.
    --
    -- TG_ARGV[0] = '["id"]' for single-PK tables;
    -- TG_ARGV[0] = '["tenant_id", "order_id"]' for composite PKs;
    -- TG_ARGV[0] = '[]' for tables without a PK (refused at trigger-
    --              attach time per §14).
    IF TG_OP = 'INSERT' THEN
        v_pk     := jsonb_strip_nulls(to_jsonb(NEW) - 'will-be-set-by-mask');
        v_after  := to_jsonb(NEW);
        v_before := NULL;
    ELSIF TG_OP = 'UPDATE' THEN
        v_pk     := to_jsonb(NEW);  -- PK masked at trigger-attach time
        v_before := to_jsonb(OLD);
        v_after  := to_jsonb(NEW);
    ELSIF TG_OP = 'DELETE' THEN
        v_pk     := to_jsonb(OLD);
        v_before := to_jsonb(OLD);
        v_after  := NULL;
    END IF;

    INSERT INTO sluice_change_log
        (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
    VALUES
        (pg_current_xact_id()::text::bigint,
         TG_TABLE_SCHEMA,
         TG_TABLE_NAME,
         CASE TG_OP WHEN 'INSERT' THEN 'I' WHEN 'UPDATE' THEN 'U' WHEN 'DELETE' THEN 'D' END,
         v_pk,
         v_before,
         v_after);

    RETURN NULL;  -- AFTER triggers ignore return value
END;
$$;
```

Real production version: the PK projection uses
`jsonb_object_agg(k, v) FILTER (WHERE k = ANY(pk_columns))` to extract
PK columns only — the snippet above elides that for readability. The
production version also has a TRUNCATE-handling sibling
`sluice_capture_truncate()` because TRUNCATE triggers are FOR EACH
STATEMENT, not FOR EACH ROW, and need separate signatures.

Per-table triggers are:

```sql
CREATE TRIGGER sluice_capture
AFTER INSERT OR UPDATE OR DELETE
ON <schema>.<table>
FOR EACH ROW
EXECUTE FUNCTION sluice_capture_change('["pk_col_1", "pk_col_2"]');

CREATE TRIGGER sluice_capture_truncate
AFTER TRUNCATE
ON <schema>.<table>
FOR EACH STATEMENT
EXECUTE FUNCTION sluice_capture_truncate();
```

**REPLICA IDENTITY assumption.** The trigger reads `OLD` and `NEW`
via `to_jsonb`, which captures whatever Postgres makes available
under the table's REPLICA IDENTITY setting. The trigger engine
documents `REPLICA IDENTITY FULL` as the recommended setting for
UPDATE-heavy workloads (full before-image captured); `DEFAULT` (PK
only on UPDATE/DELETE) works but `Update.Before` events on the IR
stream will carry only PK columns, matching the existing pgoutput
behavior under the same setting.

### 4. JSON shape: `jsonb` only

**Decision: `JSONB`, full stop.** Reject `row_to_json` (returns
`json`, not `jsonb`), reject `hstore` (extension dependency we
cannot assume on restricted tiers), reject column-per-value with
type-tagged subtables (architecturally overkill for v1).

`JSONB` is built into every PG 9.4+ server — no extension, no
plugin. Its binary representation is decoded by pgx natively into
Go `map[string]any` on the read side. Compared to `json` (text),
`jsonb` saves the read-side parse cost on every poll cycle and
deduplicates whitespace + key ordering, which matters when the
change-log table grows past 100K rows.

Type-fidelity caveats accepted with this choice:

- **Numeric precision is preserved.** PG's `to_jsonb` on a `numeric`
  column emits the value as a JSON number with the full digit count
  (PG's `jsonb` numeric is unbounded, unlike JSON's IEEE-754 default
  in most languages). The Go decoder receives it as `json.Number`
  if sluice uses `Decoder.UseNumber()` — which the projection layer
  in the engine MUST set, or lossy decimal is the silent-loss
  failure mode this design must not have.

- **Temporal types** round-trip cleanly as ISO-8601 strings. `time
  with time zone` (timetz) is the one ugly case sluice already has
  custom handling for elsewhere (see `internal/engines/postgres/timetz_codec.go`);
  the trigger engine reuses that codec at decode time.

- **Bytea** is captured by `to_jsonb` as a base64-decoded JSON string
  using PG's `\\x` hex form. The decode side strips the leading
  `\\x` and hex-decodes back to `[]byte`. Loud refusal at engine
  startup if a target column type would not survive the round-trip
  (the verbatim-extension path from ADR-0047 is the established
  pattern for this kind of refusal).

- **Arrays, hstore, jsonb itself, custom enums** all round-trip as
  their natural `jsonb` representation. The decoder layer reuses the
  same value-decoder catalog the existing `postgres` engine uses for
  its row-reader, so any cross-engine value translation already
  catalogued in `docs/value-types.md` applies unchanged.

- **NULL elements in arrays.** The pgoutput engine has the Bug-74
  family matrix (CLAUDE.md "Pin the class, not the representative"
  rule) for array-element handling; the trigger engine inherits the
  same matrix as a v1 test requirement (§15), with the cells re-run
  against the JSONB-round-trip path rather than the pgoutput binary
  path.

### 5. Change-log retention and pruning

**Polling-watermark deletion: `DELETE FROM sluice_change_log WHERE id <=
$last_acked_id`.** Run as a background statement on a configurable
cadence (default: every 30 seconds, configurable via `--prune-interval`),
not on every poll. Pruning runs in its own transaction on the source
DB; failures log at WARN and retry — pruning is not on the critical
path of apply correctness, only of source disk space.

The `last_acked_id` value is the `id` of the most-recently-applied
change whose target-side `sluice_cdc_state` row commit has durably
landed. This is the same atomicity contract ADR-0007 already
documents: the position is written in the same target transaction as
the data, so the source pruner can trust that any `id` at-or-below
the persisted position is fully replicated.

**VACUUM pressure.** A high-throughput source (1000 writes/sec
sustained) will produce a change-log table that gets DELETE-heavy.
PG's autovacuum will keep up at the default settings *if* the table
gets adequate vacuum time, which on a restricted tier with shared
autovacuum workers is exactly the unreliable assumption. The engine
mitigates this with:

- A documented per-source-table `ALTER TABLE sluice_change_log SET
  (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_delay = 0)`
  applied at engine setup time. Restricted tiers that disallow
  `ALTER TABLE ... SET (autovacuum_*)` log a one-time WARN and fall
  through to whatever the tier provides.

- An operator-facing pre-flight at engine startup that runs `SELECT
  pg_total_relation_size('sluice_change_log')` and refuses-loudly if
  the table is over a configurable size threshold (default: 1 GiB),
  with the operator-actionable error "change-log table is large; run
  `sluice trigger reconcile --vacuum-full` or shrink your --prune-
  interval."

- An optional `--use-partitioning` flag that creates the change-log
  as a daily-range-partitioned table on `committed_at`. Pruning then
  drops whole partitions instead of running per-row DELETEs, which
  is the recommended path for sources doing more than ~100 writes/sec
  sustained. Off by default to keep the v1 setup story simple.

### 6. Polling cadence and batch size

**Default: poll every 1 second, batch up to 10000 rows per poll, with
the §2 safety-lag query filtering out not-yet-durable `id`s.**

The cadence is configurable in koanf:

```yaml
source:
  driver: postgres-trigger
  dsn: "postgres://..."

postgres_trigger:
  poll_interval: 1s          # default 1s
  batch_size: 10000          # default 10000
  prune_interval: 30s        # default 30s
  prune_safety_margin: 5s    # min age before a row is eligible to prune;
                             # not strictly required for correctness, but
                             # gives operators a window to inspect mid-flight
  use_partitioning: false    # default false; per-day partitions when true
  safety_lag_check: true     # default true; the xmin-based commit-order
                             # filter described in §2. Disable only for
                             # source DBs where pg_snapshot_xmin is denied.
```

The defaults match the ceiling cited in §11 (a few thousand
changes/sec under typical workload). At 10000 rows/poll × 1
poll/second, the engine can sustain 10K rows/sec without burning
through batches — and the polling loop adapts: if a poll returns
exactly `batch_size` rows, the next poll fires immediately rather
than waiting the full interval (back-pressure pull, not push).

**`koanf` integration.** The config schema lives in
`internal/config/postgres_trigger.go`; flag bindings live in the
kong command struct. The same per-source-engine config pattern the
MySQL flavor split uses is reused (the `mysql:` / `planetscale:`
config blocks are the prior art).

### 7. DDL detection

**Hybrid: event triggers (PG 9.3+) write a sentinel `op = 'D' on a
synthetic table `sluice_ddl_marker`, AND the engine pre-flights every
poll with a schema-fingerprint check against
`sluice_change_log_meta.tracked_schema_version`. On any mismatch the
engine refuses loudly with the drained-model recovery hint.**

Postgres event triggers ARE available on every PG 9.3+ source, including
restricted tiers (they're a built-in feature, not an extension). The
engine creates one event trigger at setup:

```sql
CREATE OR REPLACE FUNCTION sluice_capture_ddl()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.schema_name IS NULL OR r.object_identity IS NULL THEN
            CONTINUE;
        END IF;
        INSERT INTO sluice_change_log
            (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
        VALUES
            (pg_current_xact_id()::text::bigint,
             COALESCE(r.schema_name, 'public'),
             COALESCE(r.object_identity, 'unknown'),
             'X',  -- DDL marker
             jsonb_build_object('command_tag', r.command_tag, 'object_type', r.object_type),
             NULL,
             NULL);
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER sluice_capture_ddl_trg
ON ddl_command_end
WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE', 'DROP TABLE', 'CREATE INDEX', 'DROP INDEX')
EXECUTE FUNCTION sluice_capture_ddl();
```

Event-trigger creation requires the SUPERUSER or `pg_create_event_trigger`
role on PG 14+. On tiers that deny event-trigger creation (Heroku
Essential is the known case), engine setup falls through to a
**polled-fingerprint-only** mode: `sluice_change_log_meta` stores a
hash of every tracked table's column-list+type-tuple, and every
polling cycle re-reads the source's catalog projection and refuses
loudly on any mismatch. This is more expensive than the event-trigger
path (an extra catalog query per poll) but catches DDL within one
poll interval and works on the most restricted tiers.

**Refusal shape, not auto-apply.** Both code paths refuse, not auto-
apply. The drained-model recovery hint (ADR-0054's established
pattern) is the canonical recovery path: operator stops the stream,
runs `sluice migrate` to land the schema change on target, restarts
the stream from a fresh position. Auto-apply of source-side DDL via
the trigger plane is explicitly out of scope for v1; the complexity
budget required to do it correctly (matching the schema-history /
position-anchored decode model the pgoutput engine has via ADR-0049)
is multiples of the rest of this ADR.

Rejected alternative: operator-coordinated DDL via `sluice migrate`
only, with no detection. The failure mode is silent: an operator
runs `ALTER TABLE source ADD COLUMN x` and forgets to coordinate
sluice. The trigger keeps firing, `to_jsonb(NEW)` includes the new
column, and the applier silently fails (column not found on target).
The classifier would catch the eventual divergence but might take
hours; meanwhile every UPDATE silently re-attempts. Refuse-loudly on
DDL is the loud-failure tenet's direct application here.

### 8. Capabilities surface

```go
var capabilities = ir.Capabilities{
    BulkLoad:                 ir.BulkLoadCopy,      // inherited from postgres
    CDC:                      ir.CDCTriggers,       // NEW: not CDCLogicalReplication
    SchemaScope:              ir.SchemaScopeNamespaced,
    SupportedTypes:           ir.NewTypeSet(
        ir.ExtEnum, ir.ExtUUID, ir.ExtArray, ir.ExtInet, ir.ExtCidr, ir.ExtMacaddr,
    ),
    SupportsCheckConstraint:  true,
    SupportsGeneratedColumns: false,                // see below
    SupportsPartitioning:     true,
    EnumSupport:              ir.EnumTypeLevel,
    JSONSupport:              ir.JSONBoth,
    UnsignedIntegers:         false,
}
```

Differences from the `postgres` engine's capabilities, with rationale:

- **`CDC: ir.CDCTriggers`** — the central declaration. The orchestrator
  reads this to skip the slot-management, publication-management, and
  REPLICATION-role pre-flights that the pgoutput path uses.

- **`SupportsGeneratedColumns: false`** — PG generated columns
  (`GENERATED ALWAYS AS ... STORED`) work with the trigger function
  fine, but the engine refuses to *replicate* them onto a target via
  the trigger pipeline. The reasoning: a generated column's value on
  the target is computed from the target's expression; replicating
  the source value would write a value the target's expression then
  silently overwrites — at best confusing, at worst (if the target
  expression doesn't match the source's) silent divergence. The
  trigger engine refuses-loudly at setup if the source schema
  declares any `GENERATED ALWAYS AS ... STORED` column on a
  replicated table, with the recommendation "exclude this column or
  use the `postgres` engine which handles generated columns via
  pgoutput's column-list."

- **Everything else** matches the `postgres` engine. The schema-read,
  schema-write, row-read, and row-write paths are reused unchanged
  (§9), so the type system surface is identical.

The orchestrator's existing capability-driven dispatch handles the
rest. Features that gate on `Capabilities.CDC == CDCLogicalReplication`
(SlotManager, publication management, the live add-table flow's
slot-LSN floor check from ADR-0030) automatically no-op or refuse
when run against `postgres-trigger`. The CLI's `sluice slot list`
returns "engine does not support slot management" cleanly.

**Features blocked by the trigger engine:**

- `sluice slot list` / `sluice slot drop` — no slots exist.
- Live add-table without drain (ADR-0030) — the trigger plane has no
  LSN-equivalent invariant to enforce. Operator must use the drained
  add-table flow.
- `--position-from-manifest` (ADR-0049 Chunk D) — works conceptually
  (the manifest position is just an `id` value), but v1 defers the
  integration until the chain-restore path has a v1 user.

### 9. Composition with existing PG engine

**Embed the existing `postgres.Engine` struct in `pgtrigger.Engine`,
override the CDC-related methods, and delegate everything else.**

```go
// internal/engines/pgtrigger/engine.go
package pgtrigger

import (
    "github.com/orware/sluice/internal/engines"
    "github.com/orware/sluice/internal/engines/postgres"
    "github.com/orware/sluice/internal/ir"
)

type Engine struct {
    postgres.Engine  // composed: SchemaReader, SchemaWriter, RowReader, RowWriter, etc.
}

func (Engine) Name() string                  { return "postgres-trigger" }
func (Engine) Capabilities() ir.Capabilities { return capabilities }
func (Engine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error)       { ... }
func (Engine) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error){ ... }

// Explicitly do NOT implement OpenSlotManager — the engine does not have slots.
// Explicitly do NOT implement OpenCDCReaderWithSlot — same reason.

func init() {
    engines.Register(Engine{})
}
```

The composition is intentional, not embedding-with-promotion: every
delegated method is forwarded explicitly so the doc comments stay
local to this package. Type assertions in the orchestrator (e.g.
`engine.(ir.SlotManagerOpener)`) cleanly miss against `pgtrigger.Engine`
because the embedded struct's `OpenSlotManager` is shadowed by the
deliberate omission.

Rejected alternative: fork the postgres package. The package is ~10K
LOC of schema-reader / writer / row-reader / row-writer; forking it
would create a long-tail maintenance burden where every PG type
catalog change has to land in two places. Composition keeps the
type / value translation surface in exactly one package.

### 10. Setup UX

**One-shot `sluice trigger setup --dsn=...` runs the DDL; the operator
can preview the SQL with `--dry-run`; the operator-side flow is the
same as `sluice migrate` plus one extra setup step.**

```bash
# Inspect the DDL that would be applied (no changes to source DB):
sluice trigger setup --dsn=$SOURCE_DSN --dry-run

# Apply the DDL (creates sluice_change_log, sluice_capture_change(),
# sluice_capture_ddl(), per-table triggers for every table matching
# the configured include/exclude filter):
sluice trigger setup --dsn=$SOURCE_DSN

# Remove all sluice state from the source (drops triggers, drops the
# capture functions, drops sluice_change_log):
sluice trigger teardown --dsn=$SOURCE_DSN
```

Required source-side permissions:

- `CREATE` on the target schema (for `sluice_change_log` and the
  capture functions).
- `TRIGGER` on every replicated table (for `CREATE TRIGGER ...`).
- INSERT / SELECT / DELETE on `sluice_change_log` (granted automatically
  by the setup command if the role owns the table).
- `pg_create_event_trigger` role (PG 14+) OR superuser (PG ≤ 13) for
  the DDL-detection event trigger. On tiers that grant neither,
  fall through to polled-fingerprint mode (§7).

Notably **NOT required:**

- REPLICATION role attribute.
- Slot-creation permission.
- `wal_level=logical`.
- Superuser (in the non-event-trigger fallback).

These are the operator-actionable requirements; the engine pre-flights
each one and refuses-loudly with the exact `GRANT` statement needed
when something is missing.

### 11. Performance ceiling

**Design target: 5000 changes/sec sustained on a default-tuned source,
1000 changes/sec sustained on Heroku Essential / Render Basic-class
restricted tiers, p99 source-to-target latency of 2 seconds at the
sustained rate.**

Above these rates, the engine refuses-loudly with the recommendation:
"this workload exceeds the trigger engine's design ceiling. Migrate to
a PG service tier that supports logical replication and use the
`postgres` engine."

The ceiling rationale:

- The trigger function adds one INSERT per source row event to the
  source-side write path. That's a 2x write amplification at sustained
  load — measurable, real, and the operator should know about it. The
  Bucardo community's benchmark numbers (mostly anecdotal, 100–500
  changes/sec on default settings, 1000+ with tuning) are in the same
  ballpark for the same reason.

- The polling-and-prune path adds source-side query load. At 1
  poll/second with a 10K-row batch on a busy log, the source sees
  one `SELECT ... ORDER BY id LIMIT 10000` per second plus the periodic
  pruning DELETE. Negligible on a beefy source; non-trivial on a
  shared-instance tier.

- The applier side has no special ceiling — it's the same
  `ir.ChangeApplier` shape the pgoutput engine uses, so target-side
  throughput is whatever the target engine declares.

**Loud refusal mechanism.** The engine measures sustained
changes/sec via a 60-second rolling window on the polling-loop and
logs at WARN when above the ceiling. At 2× the ceiling sustained
for 5 minutes, the engine refuses-loudly and exits with an
operator-actionable message. The 2× threshold is a guard against
spurious bursts; the message names the `postgres` engine as the
intended successor.

### 12. Cross-engine compatibility

**`postgres-trigger` composes with any sluice target, same as
`postgres` does.** The CDC reader produces `ir.Change` values; the
target's `ir.ChangeApplier` consumes them. There is no special-case
code in the orchestrator for source-trigger-engine.

**v1 release scope** ships:

- `postgres-trigger → postgres` (cross-mode within PG).
- `postgres-trigger → mysql` (cross-engine; the headline migration
  story this engine enables — "move my Heroku Postgres to MySQL").
- `postgres-trigger → postgres-trigger` (same-engine round-trip; the
  integration-test sanity check).
- `postgres-trigger → planetscale` (cross-engine to the PlanetScale
  flavor; the "Heroku Postgres Essential → PlanetScale" narrative is a
  distinct customer story from "→ AWS RDS MySQL" and worth pinning in
  v1 since the cost is one additional integration-test cell, not a
  different design).

### 13. Migration off this engine

**Operator playbook (documented in `docs/migration-paths.md`):**

1. Stop the sluice sync (`sluice sync stop --wait`).
2. Run `sluice trigger teardown --dsn=$SOURCE_DSN` to remove all
   trigger-engine state from the source (idempotent).
3. The operator upgrades their PG service tier (or migrates to a
   tier that supports logical replication).
4. The operator runs `sluice sync start --source-driver=postgres
   --reset-position`, which starts a fresh cold-start + cutover
   via the pgoutput engine.

The cutover is necessarily a cold re-snapshot at the boundary
between the two engines — there's no shared position-encoding
between trigger-`id`s and pgoutput LSNs. The playbook documents this
explicitly; the operator who wants zero-downtime cutover between the
two engines is using sluice's existing tooling for that (active
sluice sync from new engine cut over to active sluice sync from old
engine via a brief read-only window on the source), not a sluice
black-box.

### 14. Refuse-loudly boundaries

The engine refuses (at setup time or at engine-open time, never
mid-stream) the following shapes:

- **Tables without a PRIMARY KEY** — the trigger needs a deterministic
  key to capture into `pk_jsonb`. Without one, `Update.Before`
  events have nothing to identify the affected row by, and the
  applier's idempotent-replay contract (ADR-0010) breaks. Refuse with
  the operator-action "add a PRIMARY KEY to <schema>.<table> before
  including it in the trigger engine's replication set."

- **Tables declared UNLOGGED** — they're invisible to recovery and
  invisible to the replication semantics the engine claims to provide.
  Refuse with "exclude unlogged tables explicitly via
  `--exclude-table` or convert them to LOGGED."

- **Tables with any `GENERATED ALWAYS AS ... STORED` column** — see
  §8. Refuse with "the trigger engine does not replicate generated
  columns; use the `postgres` engine or exclude the column via
  `--exclude-column`."

- **Tables with custom domain types whose underlying type is
  unrecognized** — the `jsonb` round-trip works for built-in types
  but custom domains over built-in types are fine; custom domains
  over user-defined types are refused at engine startup pending a
  v1.5 catalog-walk.

- **Source PG version < 9.4** — `JSONB` was added in 9.4. PG 9.3 has
  `JSON` (text) only, which would force a different (worse) encoding.
  Refuse at connect time.

- **Source missing the `pg_create_event_trigger` role AND superuser
  AND `--allow-polled-fingerprint`** — the operator opts in to the
  weaker DDL detection mode via flag, rather than have it silently
  degrade. The default behavior on a tier that grants neither is to
  refuse-loudly with the flag suggestion in the error message.

Versus the `postgres` engine, which accepts each of these (in some
form), this is a deliberately tighter surface. The trigger engine's
audience is "I want a working migration with the smallest possible
operational surface"; the refusals are aligned with that goal.

### 15. Test surface

Per CLAUDE.md "pin the class, not the representative" and ADR-0001
"validate end-to-end before building more," the trigger engine's
v1 test matrix is non-negotiable. Estimate: ~80 integration tests +
~40 unit tests, sized in the same range as the pgoutput engine's
initial test cycle.

**Same-engine round-trip (`postgres-trigger ↔ postgres-trigger`):**
- Basic INSERT/UPDATE/DELETE on PK'd tables (5 tests covering the
  cardinality matrix).
- Multi-column PK (3 tests covering composite PK projection).
- The Bug-74 family matrix applied to the JSONB round-trip: each
  element family (int/float/bool/string-leaf/temporal) × each shape
  (scalar / 1-D array / multi-dim array / NULL-element) × commitment
  on `to_jsonb` round-trip fidelity. ~20 tests; non-negotiable per
  the CLAUDE.md "pin the class, not the representative" tenet — the
  array-element decoding path has a different driver code path under
  JSONB than under pgoutput.
- TRUNCATE round-trip (3 tests covering per-table truncate, multi-
  table truncate-in-one-statement, and truncate-during-active-stream).
- The DDL refuse-loudly path (5 tests: ADD COLUMN, DROP COLUMN, ALTER
  TYPE, RENAME, mixed-DDL — each one fires the event trigger, the
  engine refuses-loudly, the operator-action hint is in the error).

**Cross-engine (`postgres-trigger → postgres`, `postgres-trigger → mysql`):**
- The standard cross-engine value-translation matrix from
  `docs/value-types.md`, run through the trigger reader instead of
  the pgoutput reader. ~25 tests; ensures the JSONB-mediated decode
  produces the same `ir.Row` values pgoutput does.
- One end-to-end PG-to-MySQL test demonstrating "Heroku Postgres
  Essential to AWS RDS MySQL" as a sluice-supported migration path
  (the headline use case).

**Setup / teardown (unit + integration):**
- `sluice trigger setup --dry-run` emits the expected DDL (5 tests).
- `sluice trigger setup` is idempotent (re-running on an already-
  setup source no-ops cleanly) (3 tests).
- `sluice trigger teardown` removes every artifact the engine
  created and leaves the source's user tables untouched (5 tests).
- Permissions pre-flight refuses cleanly when each required
  permission is missing (5 tests, one per permission).

**Throughput / latency:**
- A `_integration_test.go` benchmark drives 1000 changes/sec at the
  source and asserts p99 source-to-target latency < 2s. Single test;
  it pins the design-ceiling claim from §11.

The full matrix lives in `internal/engines/pgtrigger/*_integration_test.go`
under the `//go:build integration` tag; the cross-engine subset lives
under `internal/pipeline/migrate_pgtrigger_cross_integration_test.go`
to fit the existing cross-engine test layout.

## Consequences

The trigger engine ships sluice to a new user class: operators on
slot-restricted managed PG who today have Bucardo as their only
option. The implementation cost is ~6 weeks of focused work (engine
package + setup CLI + test matrix + docs), bounded by the explicit
v1 scope; the maintenance cost is the new `_integration_test.go`
suite running on every PR and the obligation to keep the JSONB
round-trip layer in sync with the existing value-translation catalog.

The engine's design ceiling (§11) is real and operator-facing — a
load test deliberately above 5000 changes/sec WILL fail. This is the
right tradeoff for the engine's audience (operators who are on
restricted tiers precisely because their workload is small enough to
not need a bigger tier), but it must be loud, not silent.

The DDL refusal semantics (§7) match the existing drained-model
recovery pattern (ADR-0054 et al). Operators familiar with how sluice
handles schema change on the pgoutput engine will find the trigger
engine's behavior consistent.

The setup-step UX (§10) is a regression vs the pgoutput engine
(which requires no source-side DDL beyond the implicit publication +
slot creation). The author considers this acceptable because the
explicit one-shot `sluice trigger setup` command leaves the
operator with full visibility into what landed on their source, and
the same visibility on teardown. The implicit slot-and-publication
creation of the pgoutput engine has historically been a source of
operator confusion (slots left behind after stream death, ADR-0011),
and this engine's explicitness is the design lesson from that.

## Alternatives

**Don't ship a trigger engine.** Direct operators on slot-restricted
tiers to Bucardo, or to "upgrade your tier." This is the de-facto
state and is what every other migration tool the author surveyed
does. Rejected because the addressable-market signal from the
maintainer's own customer conversations is non-trivial and the
implementation cost is bounded — and because Bucardo specifically
is the operational tax we can avoid by shipping a sluice-native path
that reuses the IR/applier/state-store/CLI surface.

**Ship a forked PG engine instead of composing.** Considered and
rejected in §9. The forking cost is multi-month and the long-tail
type-catalog drift is the predictable failure mode.

**Use `pg_partman`-style chunking for the change-log table without
a dedicated polling reader.** The chunking is fine; the polling reader
is the IR-contract-bearing piece. The optional `--use-partitioning`
flag (§5) keeps this in scope as a tunable, not as a replacement
architecture.

**Capture-as-`row_to_json`/text instead of `JSONB`.** Rejected in §4.
The performance and type-fidelity tradeoffs all favor `JSONB`.

**Stream the change-log via PG's LISTEN/NOTIFY** (Bucardo's mechanism)
**instead of polling.** Considered and rejected because:

- NOTIFY payloads are size-limited (8KB by default) — a row update on
  a wide table would exceed it and the engine would need a fallback
  path anyway.
- NOTIFY is connection-bound — a network blip costs every queued
  notification. The polling-watermark design is crash-tolerant by
  construction: any change with `id <= watermark` is "definitely
  applied," any change with `id > watermark` is "to-be-applied," and
  there is no third state.
- The pgoutput engine handles back-pressure via the replication
  protocol's slot-LSN ACK; the trigger engine handles it via the
  watermark. NOTIFY would be a third semantic to implement and to
  reconcile.

A future v1.5 could add `LISTEN sluice_change_log_notify` as a
**hint** for the polling loop to reduce idle-wakeups (notify wakes the
poller; the poller still uses the watermark for correctness). Not
in v1.

**Auto-apply source-side DDL via the trigger plane.** Rejected in §7.
The complexity budget is multiples of the rest of this ADR and the
schema-history machinery needed to do it correctly (position-anchored
decode per ADR-0049) is engine-specific work that hasn't been done
for the trigger engine. Refuse-loudly + drained-model recovery is the
right v1 floor.

## References

- [ADR-0001](adr-0001-ir-first-translation.md) — the IR-first tenet
  that lets a new engine slot in without orchestrator changes.
- [ADR-0005](adr-0005-mysql-flavors.md) — the flavor-as-capability-
  variant pattern this ADR explicitly chooses NOT to follow at v1
  (one engine name, no flavor split).
- [ADR-0006](adr-0006-pgoutput.md) — the existing PG CDC engine
  this ADR explicitly does NOT subsume; both engines coexist.
- [ADR-0007](adr-0007-position-persistence.md) — the position-and-
  data atomicity contract the trigger engine's polling reader
  inherits directly.
- [ADR-0010](adr-0010-idempotent-applier.md) — the idempotent-applier
  semantics the trigger engine's `ChangeApplier` honors unchanged.
- [ADR-0011](adr-0011-slot-manager-optional-surface.md) — the
  optional-surface pattern; the trigger engine deliberately does not
  implement `SlotManagerOpener` (no slots to manage).
- [ADR-0022](adr-0022-slot-missing-fall-through.md) — the cold-start
  fall-through behavior on invalid position. The trigger engine's
  equivalent is "the change-log table was dropped and recreated" —
  same recovery path.
- [ADR-0030](adr-0030-mid-stream-live-add-table.md) — the live add-
  table flow; refused on the trigger engine in v1, drained-model
  recovery only.
- [ADR-0047](adr-0047-verbatim-extension-passthrough.md) — the
  verbatim-tier passthrough; type-fidelity refusal pattern reused at
  trigger setup time.
- [ADR-0049](adr-0049-cdc-schema-history.md) — the position-anchored
  schema-history machinery; the trigger engine could grow a v1.5
  integration but does not have one in v1 (DDL → refuse, not
  schema-version-aware decode).
- [ADR-0054](adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) —
  the drained-model recovery hint pattern the trigger engine reuses
  for its DDL refusal path.
- Bucardo, the prior-art Perl-based trigger replication system.
  Concepts borrowed: trigger-based capture into a sluice-managed
  change-log table; controller daemon polling the log. Concepts
  explicitly NOT borrowed: separate control database (sluice's
  applier is target-local), Perl runtime, `bucardo` CLI surface
  (sluice's `sluice trigger` subcommand is a different shape),
  multi-master / conflict-resolution machinery (out of scope per
  CLAUDE.md "active-active replication" deferral).

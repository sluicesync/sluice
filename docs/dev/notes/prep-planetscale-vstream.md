# Prep: PlanetScale MySQL CDC via Vitess VStream

> **Status: SHIPPED in v0.12.x.** `FlavorPlanetScale` now declares `CDCVStream`; the multi-shard reader with auto-discovery and reshard detection is the canonical PlanetScale CDC path. See [ADR-0034](../../adr/adr-0034-mysql-phase-2-live-add-table.md) for the follow-on filter-flip mechanism.

Roadmap reference: not in the original roadmap. Surfaces from the §8 wrap-up: PlanetScale's `FlavorPlanetScale` declares `CDC: ir.CDCNone` today, and the `// PlanetScale's own change-feed mechanisms could be added as a separate option later` comment in `flavor.go` is the deferred work this chunk picks up.

## Goal

Implement CDC for PlanetScale MySQL via Vitess's VStream API, so sluice's continuous-sync mode works against PlanetScale sources. PlanetScale doesn't expose binlog directly (the Vitess proxy intercepts); the supported path is VStream, a gRPC streaming protocol that surfaces row-level changes from the underlying tablets.

This is the largest of the four follow-up chunks — comparable in scope to §2 (MySQL CDC reader). It introduces a new dependency (Vitess Go client), a new CDC method type, and a new event-decoding pipeline.

Out of scope:

- **Snapshot+CDC handoff for PlanetScale.** §4-shape work for PS specifically; can ship as a follow-up once the basic CDC reader is in place. PlanetScale supports `START TRANSACTION WITH CONSISTENT SNAPSHOT` per their docs, but VStream's snapshot-coordination semantics may differ from binlog's; needs design work parallel to §4.
- **Sharded sources.** PlanetScale's killer feature is Vitess sharding. v1 of the VStream reader targets unsharded keyspaces; sharded sources need explicit shard-coordination work and ship later.
- **Alternative PS-MySQL CDC paths.** PlanetScale offers a "boost" feature with its own change-feed, and historically `pscale connect` exposed binlog-ish data through a tunnel. We're picking VStream as the canonical path; alternatives are a future capability flag.

## Library choice

**`github.com/planetscale/vitess-types`** + **`vitess.io/vitess/go/vt/proto/...`** — Vitess's Go client and protobuf bindings. PlanetScale forks Vitess; the upstream Vitess client connects to PS endpoints natively because the wire protocol is the same.

Alternative considered: write a thin gRPC client against the published VStream proto definitions, avoiding the full Vitess Go module. Doable but reinvents the wheel — Vitess already maintains the proto bindings and a connection helper.

Decision: import the relevant Vitess sub-packages (proto bindings + the `vtgateconn` client). The Vitess module is large (~MB of generated proto), but Go's tree-shaking + module graph make this acceptable.

## Capabilities surface

Add a new CDC method to `internal/ir/capabilities.go`:

```go
const (
    CDCNone               CDCMethod = iota
    CDCBinlog                       // MySQL row-based binary log
    CDCLogicalReplication           // PostgreSQL logical replication
    CDCTriggers                     // Trigger-based CDC (e.g. SQLite future)
    CDCVStream                      // Vitess VStream gRPC (PlanetScale)
)
```

Update `FlavorPlanetScale`'s capability declaration:

```go
FlavorPlanetScale: {
    BulkLoad:    ir.BulkLoadBatchedInsert,
    CDC:         ir.CDCVStream,    // was: ir.CDCNone
    SchemaScope: ir.SchemaScopeFlat,
    // ... rest unchanged
},
```

## Files to add / touch

New files in `internal/engines/mysql/`:

- `cdc_vstream.go` — `vstreamCDCReader` struct, the gRPC connection management, the VEvent → ir.Change dispatch table.
- `cdc_vstream_position.go` — encode/decode VStream positions (per-shard `VGtid` proto messages) into `ir.Position.Token` JSON.
- `cdc_vstream_test.go` — unit tests for position encoding and event dispatch (no Docker; mock VEvent inputs).
- `cdc_vstream_integration_test.go` — `//go:build integration`. Uses Vitess's `vttestserver` (a single-binary test harness Vitess publishes) for a containerised PS-shaped target. Exercises end-to-end change capture.

Modify:

- `internal/engines/mysql/engine.go` — `OpenCDCReader` dispatches on flavor: `FlavorVanilla` → existing binlog reader, `FlavorPlanetScale` → new VStream reader.
- `internal/engines/mysql/flavor.go` — capability declaration update.
- `internal/ir/capabilities.go` — `CDCVStream` constant + `String()` case.
- `go.mod` / `go.sum` — Vitess imports.

## Data flow sketch

```
[CDC user]
  StreamChanges(ctx, ir.Position{Engine:"planetscale", Token:"<vgtid json>"})
    │
    ▼
[vstreamCDCReader]
  decode token → VGtid proto (shard → starting position)
  vtgateconn.Dial(...)
  conn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
    │
    ▼ (events)
  for vevent in stream.Recv():
      switch vevent.Type:
          BEGIN              → start tx scope
          FIELD              → cache table-field metadata (parallel to RelationMessage on PG side)
          ROW                → emit ir.Insert / ir.Update / ir.Delete based on RowChange.RowEvent
          COMMIT / GTID      → advance position
          DDL                → invalidate field cache; future hook for ir.Truncate parsing
          OTHER              → ignore
    │
    ▼
[out chan ir.Change]
```

## Position encoding

VStream positions are per-shard (`VGtid` proto contains repeated `ShardGtids`, each with `keyspace`, `shard`, `gtid`). For an unsharded keyspace there's a single shard entry. The position token serialises the proto via JSON or its binary form, wrapped in `ir.Position.Token`.

```go
type vstreamPos struct {
    Keyspace string             `json:"keyspace"`
    Shards   []shardGtid        `json:"shards"`
}
type shardGtid struct {
    Shard string `json:"shard"`
    Gtid  string `json:"gtid"`  // canonical Vitess GTID string
}
```

Sharded sources have multiple `shardGtid` entries; unsharded has exactly one.

## Anticipated rough edges

- **Field cache vs. schema cache.** Vitess VStream sends `FIELD` events when it sees a table for the first time, with column-name + column-type metadata. This is the field-cache; sluice's existing schema cache (informationschema-driven) doesn't apply because the Vitess proxy filters/transforms. Wire the field-cache as the source-of-truth for column names during VStream events.
- **Type fidelity.** VStream's column types are MySQL types but transmitted through the Vitess wire format; sub-types like `JSON` come through as TEXT/BLOB. The IR-typed value may need fewer engine-side type assertions than the binlog path (Vitess does some normalisation). Test surface here is high.
- **Sharded vs. unsharded.** v1 explicitly targets unsharded keyspaces. Detection: the keyspace has exactly one shard (`-`). Detect at startup, error clearly if the user points sluice at a sharded keyspace, document the limitation.
- **Vitess module size.** Importing `vitess.io/vitess/go/vt/proto/...` pulls in a large dependency tree. Worth measuring the effect on `go build` time and final binary size; if it's prohibitive, consider a build-tag-gated approach where vanilla MySQL builds skip the Vitess dep.
- **Authentication.** PlanetScale uses service tokens; the connection-string format is non-standard. The DSN parser will need to recognise PS connection strings or operate on a separate config layer.
- **Schema-read against PS.** Even with VStream CDC working, the schema reader still uses `database/sql` against PS's pgwire-style endpoint. That should work via the regular MySQL driver; verify alongside the simple-mode flow.
- **Testing infrastructure.** `vttestserver` (Vitess's all-in-one test harness) runs in a Docker container and exposes both pgwire and VStream on the same port. Integration test harness mirrors the existing PG/MySQL container patterns.

## Phased delivery

Given the size, this chunk is a candidate for splitting:

**Phase A: Skeleton + position encoding** (~400 lines).
- `cdc_vstream_position.go` + tests
- `vstreamCDCReader` struct skeleton
- `OpenCDCReader` flavor dispatch
- Capability declaration update
- No actual streaming — just the wire-up and a stub `StreamChanges` returning `ErrNotImplemented`.

**Phase B: Streaming spine** (~600 lines).
- gRPC connection management
- Event dispatch loop (BEGIN / FIELD / ROW / COMMIT / GTID)
- Position bookkeeping
- ir.Insert / ir.Update / ir.Delete emission for unsharded keyspaces
- Integration test against `vttestserver` (~300 lines).

**Phase C: Field-cache + DDL handling** (~200 lines).
- FIELD event caching
- DDL → field-cache invalidation (mirroring the binlog path's QueryEvent treatment)
- Optional: TRUNCATE detection if it's symmetric with the binlog reader's §3a handling.

Each phase is independently shippable and testable.

## Open questions for the user

1. **Phased vs. monolithic shipping.** Above I'm leaning phased. *Recommendation:* phase A and B together (the spine), phase C as a follow-up. Confirm?
2. **vttestserver vs. real PlanetScale account for tests.** vttestserver is reproducible in CI but not the real thing; a real PS account validates against actual quirks. *Recommendation:* vttestserver as the canonical CI test, plus a manual-verification checklist for a real account (mirrors the §3a PG approach). Confirm?
3. **Vitess module size impact.** If the dep adds significant build time / binary size, gate behind a build tag (`//go:build vitess`) so vanilla MySQL builds don't carry the cost. *Recommendation:* measure first, decide based on numbers. Confirm?
4. **Scope of value-decoding fidelity.** VStream's column types may need their own decoder layer parallel to `value_decode.go`. *Recommendation:* extend the existing decoder where shapes match (which is most of them); add a thin `vstream_decode.go` for the genuinely-Vitess-specific shapes. Confirm?
5. **Sharded keyspaces.** v1 unsharded only, with a startup error if the user points at a sharded keyspace. *Recommendation:* yes, defer sharded support to a separate post-v1 chunk. Confirm?

## Suggested first-cut prompt

> "Read CLAUDE.md, docs/dev/notes/prep-planetscale-vstream.md, the existing MySQL CDC reader in internal/engines/mysql/cdc_reader.go, and ADR-0008 (go-mysql library choice). Propose the design before writing: (1) the vstreamCDCReader struct shape, (2) the position encoding format (JSON over the VGtid proto), (3) the VEvent type → ir.Change dispatch table, (4) the phased delivery plan (which files in Phase A vs B vs C). Note any deviation from the prep doc with a why. Stop after the design for review."

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package pgtrigger implements the sluice [ir.Engine] for trigger-based
// Postgres change-data-capture. It targets the class of managed PG
// offerings that lock down logical replication (Heroku Postgres
// Essential, Render Postgres Basic, Supabase free tier, several
// DigitalOcean managed-database tiers) — operators on these tiers have
// standard DML privileges but cannot create replication slots, alter
// `wal_level`, or install third-party plugins.
//
// The engine is registered automatically when this package is imported:
//
//	import _ "sluicesync.dev/sluice/internal/engines/pgtrigger"
//
// See ADR-0066 for the full design. Phase 1 (v0.85.0) shipped:
//
//   - The engine type, composed by embedding [postgres.Engine] (§9), so
//     schema-read / schema-write / row-read / row-write surfaces stay
//     in one place.
//   - The `postgres-trigger` capabilities surface (§8).
//   - [Setup] / [Teardown] for the `sluice trigger setup/teardown` CLI
//     to call (§10).
//   - A polling [CDCReader] (§2, §6) that scans `sluice_change_log`
//     using the §2 xmin safety-lag query.
//   - Refuse-loudly preflights for the §14 boundaries.
//   - Same-engine `postgres-trigger → postgres-trigger` bulk-copy + CDC.
//
// Phase 2 (task #72) shipped cross-engine targets:
//
//   - `postgres-trigger → mysql` / `postgres-trigger → planetscale` for
//     bulk-copy + CDC. The cross-engine supportability gate treats a
//     `postgres-trigger` source as a PG source (PostGIS Geometry,
//     pg_trgm opclass indexes, EXCLUDE constraints refuse loudly the
//     same as a `postgres` source).
//   - The full §15 Bug-74 value-family matrix pinned cross-engine via a
//     `postgres-trigger`-vs-`postgres` MySQL-target congruence test —
//     the trigger reader's JSON-scalar value shapes (bytea `\x`-hex,
//     jsonb nested map, timestamptz ISO+offset, numeric `json.Number`)
//     land byte-correct on MySQL through the applier's value-prepare
//     path.
//   - Sequence/AUTO_INCREMENT cutover priming (PG IDENTITY → MySQL
//     AUTO_INCREMENT), working via the delegated SchemaReader's
//     [ir.SequenceStateReader] surface.
//
// v0.86.1 (Bug 94) made [Engine.OpenSnapshotStream] trigger-native (see
// cdc_snapshot.go): the orchestrator's engine-neutral `sync start`
// cold-start now drives a REPEATABLE READ bulk-copy snapshot anchored at
// the capture log's contiguous committed-prefix high-water, handed off
// to the trigger CDC poller — with NO replication slot. Previously it
// delegated to the composed slot-based pgoutput path, which silently
// created a slot the managed tier forbids and never engaged the poller.
//
// `--use-partitioning` remains deferred to a follow-up phase per the
// task #62 plan.
package pgtrigger

import (
	"context"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

// EngineName is the short identifier the engine self-registers under.
// Exported so the setup / teardown CLI and integration tests don't
// have to repeat the literal.
const EngineName = "postgres-trigger"

// Engine is the trigger-based PG engine. It composes the vanilla
// [postgres.Engine] for the schema / row read+write surfaces (§9) and
// supplies its own [OpenCDCReader] for trigger-table polling.
//
// Composition is by **delegation**, not embedding-with-promotion: the
// postgres engine's optional-surface methods (`OpenSlotManager`,
// `OpenCDCReaderWithSlot`, the `pglogrepl`-touching helpers used by
// `sluice schema add-table --no-drain`) would be inherited verbatim
// under an `Engine struct { postgres.Engine }` shape, and the
// orchestrator's type-assertion on `ir.SlotManagerOpener` would
// silently route the operator through a slot-management path the
// engine does not actually support. Forwarding only the required
// methods keeps the type-assertion surface narrow — the trigger
// engine cleanly fails the SlotManagerOpener / CDCReaderWithSlotOpener
// assertions, so the CLI's `sluice slot list` and the streamer's
// `--slot-name` flag both surface the right "engine does not support
// this" error rather than dialing a non-existent slot.
type Engine struct {
	pg postgres.Engine
}

// Name reports the engine's short identifier. Registered as
// "postgres-trigger"; the literal lives in [EngineName] so the CLI
// and integration tests don't repeat it.
func (Engine) Name() string { return EngineName }

// OpenSchemaReader delegates to the composed [postgres.Engine] — the
// trigger engine's schema surface is byte-equivalent to vanilla PG.
func (e Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	return e.pg.OpenSchemaReader(ctx, dsn)
}

// OpenSchemaWriter delegates to the composed [postgres.Engine].
func (e Engine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	return e.pg.OpenSchemaWriter(ctx, dsn)
}

// OpenRowReader delegates to the composed [postgres.Engine].
func (e Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	return e.pg.OpenRowReader(ctx, dsn)
}

// OpenRowWriter delegates to the composed [postgres.Engine].
func (e Engine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	return e.pg.OpenRowWriter(ctx, dsn)
}

// OpenChangeApplier delegates to the composed [postgres.Engine]. The
// applier's INSERT / UPDATE / DELETE shape is identical between the
// pgoutput and trigger paths — only the source-side capture differs.
func (e Engine) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	return e.pg.OpenChangeApplier(ctx, dsn)
}

// OpenSnapshotStream is implemented trigger-natively in cdc_snapshot.go
// (Bug 94): a REPEATABLE READ bulk-copy snapshot anchored at the
// capture log's contiguous committed-prefix high-water, handed off to
// the trigger CDC poller — NO replication slot, NO pgoutput. It does
// NOT delegate to [postgres.Engine].OpenSnapshotStream (the slot-based
// path the slot-less managed tier forbids).

// Capabilities returns the trigger-engine capability surface (§8).
// Differs from the vanilla PG engine in three places:
//
//   - CDC: ir.CDCTriggers (not ir.CDCLogicalReplication). The
//     orchestrator's capability dispatch skips slot-management and
//     publication-management on this signal.
//   - SupportsGeneratedColumns: false. The trigger engine refuses to
//     replicate STORED generated columns (§14) because the target's
//     own expression would silently overwrite the captured value.
//   - SupportedTypes: same set as vanilla PG — the JSONB-mediated
//     decode path reuses the same value-decoder catalog.
//   - PGExtensionCatalog / VerbatimExtensionTypes: false. Extension
//     passthrough (ADR-0032) and verbatim uncatalogued types
//     (ADR-0047) are unvalidated through the trigger capture path;
//     the orchestrator's capability gates refuse them, exactly as the
//     pre-capability engine-name gates did.
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenCDCReader returns a polling reader against `sluice_change_log`
// on the source identified by dsn (§2 / §6). Refuses with a clear
// error when the change-log table is absent — the operator must run
// `sluice trigger setup` before the first sync. The caller closes the
// returned reader (via its Close method) to release the underlying
// connection pool.
func (Engine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return openCDCReader(ctx, dsn)
}

// capabilities is the static [ir.Capabilities] value the engine
// declares. Spelled out as a package-level var so the engine's
// Capabilities() method is a simple read.
var capabilities = ir.Capabilities{
	BulkLoad:    ir.BulkLoadCopy, // inherited shape from postgres
	CDC:         ir.CDCTriggers,  // NEW: not CDCLogicalReplication
	SchemaScope: ir.SchemaScopeNamespaced,
	SupportedTypes: ir.NewTypeSet(
		ir.ExtEnum,
		ir.ExtUUID,
		ir.ExtArray,
		ir.ExtInet,
		ir.ExtCidr,
		ir.ExtMacaddr,
	),
	SupportsCheckConstraint:  true,
	SupportsGeneratedColumns: false, // §8 — refuse-loudly per §14
	SupportsPartitioning:     true,
	EnumSupport:              ir.EnumTypeLevel,
	JSONSupport:              ir.JSONBoth,
	UnsignedIntegers:         false,
	DDLDialect:               ir.DDLDialectANSI,

	// The engine fronts a genuine PG server — PG catalog probes, the
	// XID-wraparound preflight, and the declarative-partitioning
	// refusal all apply (the schema surface delegates to the vanilla
	// postgres engine).
	PostgresBackend: true,

	// Conservatively NOT declared (preserving the pre-capability
	// engine-name refusals): `--enable-pg-extension` resolution and
	// ADR-0047 verbatim passthrough are unvalidated through the
	// JSONB-mediated trigger capture path. Flip on evidence.
	PGExtensionCatalog:     false,
	VerbatimExtensionTypes: false,
}

// init registers the engine with the engines registry. The blank
// import in cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package d1trigger implements the sluice [ir.Engine] for trigger-based
// Cloudflare D1 change-data-capture over the D1 HTTP query API (ADR-0136). It
// is Phase 2 of the trigger-CDC work: the SAME trigger + change-log + polling
// design the local-file `sqlite-trigger` engine shipped (ADR-0135, v0.99.148),
// run against a LIVE D1 database instead of a local *sql.DB. Phase 2 is mostly
// TRANSPORT SUBSTITUTION — the setup DDL, the trigger bodies, the change-log/
// meta/fingerprint schema, the poll SQL, the watermark, the MAX(id) snapshot
// anchor, and the schema-drift fingerprint are all the shared sqlite-trigger
// logic; only the executor differs (the D1 `/query` HTTP API vs a *sql.DB).
//
// The engine self-registers as "d1-trigger" when imported:
//
//	import _ "sluicesync.dev/sluice/internal/engines/d1-trigger"
//
// # Composition by delegation
//
// Like `sqlite-trigger` composes the `sqlite` file engine, `d1-trigger` composes
// the `d1` cold-start engine (the lossless HTTP-API reader, ADR-0132): it
// forwards OpenSchemaReader / OpenRowReader to it (so the snapshot reuses the
// validated big-int-exact D1 reader) and supplies the trigger-native CDC reader
// + snapshot stream via the shared sqlite-trigger D1 entry points
// ([sqlitetrigger.OpenD1CDCReader] / [sqlitetrigger.OpenD1SnapshotStream]). The
// write / target / change-apply surfaces stay not-implemented — D1-trigger is a
// CDC SOURCE only.
//
// # DSN + secrets
//
// The DSN is the `d1` form (`d1://<account_id>/<database_id>` or
// `d1://<database_id>` + CLOUDFLARE_ACCOUNT_ID); the API token is ENV-ONLY
// (CLOUDFLARE_API_TOKEN), never a flag. A missing token/account/database is
// refused loudly at Open*, before any HTTP request — reusing the `d1` engine's
// DSN/token parsing.
//
// # Consistency (load-bearing, ADR-0136 §4)
//
// The CDC poll uses D1's DEFAULT primary (strongly-consistent) query path, NOT
// the Sessions/read-replica routing: the exactly-once `id > watermark` invariant
// rests on commit-order = id-order, which holds at the write-serialised primary
// but can wobble against a lagging replica.
package d1trigger

import (
	"context"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/sqlite"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrNotImplemented is returned by the write / target / change-apply Open*
// methods: the D1-trigger engine is a CDC SOURCE only (ADR-0136). Callers should
// check for it with [errors.Is].
var ErrNotImplemented = sqlite.ErrD1NotImplemented

// EngineName is the short identifier the engine self-registers under. It is the
// canonical spelling owned by the sqlite-trigger package (so the backend's
// operator recovery hints and this registration can never drift).
const EngineName = sqlitetrigger.EngineNameD1

// Engine is the trigger-based Cloudflare D1 engine. It is stateless: the cold-
// start schema/row reads delegate to the `d1` engine ([sqlite.NewD1Engine]) and
// the CDC/setup surfaces delegate to the shared sqlite-trigger D1 entry points,
// so every call resolves the d1:// DSN + env token fresh. Composition is by
// DELEGATION, not embedding-with-promotion — the engine intentionally does NOT
// expose the `d1` writer/slot surfaces (it is a CDC source only).
type Engine struct{}

// Name reports the engine's short identifier ("d1-trigger").
func (Engine) Name() string { return EngineName }

// Capabilities returns the `d1` cold-start capability surface (D1 is SQLite over
// HTTP: no extension types, flat namespace, CHECK + generated columns carried by
// the reader) with [ir.Capabilities.CDC] flipped to [ir.CDCTriggers] so the
// orchestrator's capability dispatch engages the CDC path. Reusing the composed
// engine's value means the trigger engine can never drift from the cold-start
// engine on the shared type/feature surface.
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader delegates to the composed `d1` engine — the cold-start schema
// reader, which already skips the engine's own change-log/meta/fingerprint
// tables and D1's internal `_cf_*` tables (ADR-0130/0135).
func (Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	return sqlite.NewD1Engine().OpenSchemaReader(ctx, dsn)
}

// OpenRowReader delegates to the composed `d1` engine — the lossless cold-start
// bulk-copy reader (CAST/typeof projection so integers > 2^53 survive, ADR-0132).
func (Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	return sqlite.NewD1Engine().OpenRowReader(ctx, dsn)
}

// OpenSchemaWriter is not implemented: D1-trigger is a CDC SOURCE only.
func (Engine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, ErrNotImplemented
}

// OpenRowWriter is not implemented: D1-trigger is a CDC source only.
func (Engine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, ErrNotImplemented
}

// OpenChangeApplier is not implemented: D1-trigger is a CDC source only.
func (Engine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, ErrNotImplemented
}

// OpenCDCReader returns a polling reader against `sluice_change_log` on the live
// D1 database identified by dsn, over the HTTP query API. Refuses loudly when the
// change-log table is absent (run `sluice trigger setup --source-driver
// d1-trigger` first) or when the source schema has drifted since setup.
func (Engine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return sqlitetrigger.OpenD1CDCReader(ctx, dsn)
}

// OpenSnapshotStream opens the trigger-native consistent snapshot + CDC handoff
// against the live D1 database (cold-start bulk copy via the lossless `d1`
// reader, CDC tail over the same HTTP transport).
func (Engine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	return sqlitetrigger.OpenD1SnapshotStream(ctx, dsn)
}

// init registers the engine with the engines registry. The blank import in
// cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}

// capabilities is the static [ir.Capabilities] value the engine declares: the
// composed `d1` engine's shape with CDC flipped to trigger-based. Computed once
// at init so the only intentional difference from the cold-start engine is
// spelled out here.
var capabilities = func() ir.Capabilities {
	c := sqlite.NewD1Engine().Capabilities()
	c.CDC = ir.CDCTriggers
	return c
}()

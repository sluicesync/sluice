// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package sqlitetrigger implements the sluice [ir.Engine] for trigger-based
// SQLite change-data-capture (ADR-0135). SQLite has no logical replication and
// no decodable change stream (its WAL is a physical page log), so the ONLY way
// to get logical row changes out of it is the trigger pattern the `pgtrigger`
// engine already proves for managed Postgres without replication slots
// (ADR-0066): per-table AFTER INSERT/UPDATE/DELETE triggers write before/after
// images into a `sluice_change_log` table, and a CDC reader polls that table on
// a cadence, emitting [ir.Change] events with a monotonic-id watermark for
// exactly-once resume.
//
// The engine self-registers as "sqlite-trigger" when imported:
//
//	import _ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
//
// # The value-fidelity crux (the load-bearing decision)
//
// The capture trigger must NOT use SQLite's `json_object()` on raw columns: it
// serialises an INTEGER as a JSON double (silently rounding any integer > 2^53 —
// snowflake IDs, ns timestamps) and cannot represent a BLOB. Instead each column
// is captured as a `(typeof, text/hex)` pair using the SAME proven encoding as
// the file/D1 reader (`sqlite.CapturedValueExpr`): `typeof(col)` for the storage
// class and `CASE typeof(col) WHEN 'blob' THEN hex(col) WHEN 'real' THEN
// format('%.17g', col) ELSE CAST(col AS TEXT) END` for the value. The CDC reader
// reconstructs the exact int64/float64/text/[]byte via `sqlite`'s shared
// [sqlite.CapturedCellDecoder] (reconstruction + the storage-class-faithful
// `decodeCell` + the ADR-0129 date/bool policy) — ONE faithful-decode
// implementation, so a captured change decodes byte-identically to a cold-start
// snapshot row. Big integers and blobs round-trip EXACT through capture→reader.
//
// # Composition by delegation (narrowness is load-bearing)
//
// The engine composes [sqlite.Engine] by DELEGATION (not embedding-with-
// promotion), exactly like pgtrigger composes postgres: it forwards only
// OpenSchemaReader / OpenRowReader (the cold-start snapshot reuses the validated
// SQLite reader incl. within-table chunking and the date/bool policy) and
// supplies its own trigger-native [Engine.OpenCDCReader] / [Engine.OpenSnapshotStream].
// The write/target surfaces stay not-implemented — SQLite-trigger is a CDC
// SOURCE only in Phase 1.
//
// # Phase 1 scope and deferred follow-ups (ADR-0135)
//
// Phase 1 is continuous logical CDC from a LOCAL SQLite FILE to Postgres/MySQL.
// Deferred: the D1-over-HTTP variant (poll the change-log over the D1 query
// API), schema-change forwarding (SQLite has no DDL triggers, so a source ALTER
// TABLE requires re-setup — documented, not auto-captured), and capture-payload
// trimming / change-log retention.
package sqlitetrigger

import (
	"context"
	"errors"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrNotImplemented is returned by the write / target / change-apply Open*
// methods: the trigger engine is a CDC SOURCE only in Phase 1 (ADR-0135).
// Callers should check for it with [errors.Is].
var ErrNotImplemented = errors.New("sqlite-trigger engine: not implemented (CDC source only; use the `sqlite` engine for a SQLite target)")

// EngineName is the short identifier the engine self-registers under. Exported
// so the trigger setup/teardown CLI and integration tests don't repeat the
// literal.
const EngineName = "sqlite-trigger"

// Engine is the trigger-based SQLite engine. It composes the vanilla
// [sqlite.Engine] for the schema / row read surfaces (the cold-start snapshot)
// and supplies its own trigger-native CDC reader + snapshot stream.
//
// Composition is by **delegation**, not embedding-with-promotion: forwarding
// only the required methods keeps the optional-surface type-assertion surface
// narrow. The trigger engine intentionally does NOT inherit any slot-flavoured
// openers (SQLite has no such concept) and does NOT expose the sqlite engine's
// writer surfaces — it is a CDC source only. See pgtrigger's Engine doc for the
// full rationale on why this narrowness is load-bearing.
type Engine struct {
	sq sqlite.Engine
}

// Name reports the engine's short identifier ("sqlite-trigger").
func (Engine) Name() string { return EngineName }

// Capabilities returns the trigger-engine capability surface: the vanilla
// [sqlite.Engine] capabilities (migrate source shape, flat namespace, no
// extension types, CHECK + generated columns carried by the cold-start reader)
// with [ir.Capabilities.CDC] flipped to [ir.CDCTriggers] so the orchestrator's
// capability dispatch engages the CDC path. Reusing the composed engine's value
// (rather than re-declaring it) means the trigger engine can never drift from
// the cold-start engine on the type/feature surface they share.
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader delegates to the composed [sqlite.Engine]. The reader skips
// the trigger engine's own change-log/meta tables (the sqlite reader's exact-name
// skip set, ADR-0135) so a cold-start never copies them.
func (e Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	return e.sq.OpenSchemaReader(ctx, dsn)
}

// OpenRowReader delegates to the composed [sqlite.Engine] — the cold-start
// bulk-copy reader, inheriting its storage-class fidelity, within-table chunking,
// and the ADR-0129 date/bool policy.
func (e Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	return e.sq.OpenRowReader(ctx, dsn)
}

// OpenSchemaWriter is not implemented: SQLite-trigger is a CDC SOURCE only
// (Phase 1). A target migration into SQLite uses the `sqlite` engine (ADR-0134).
func (Engine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, ErrNotImplemented
}

// OpenRowWriter is not implemented: SQLite-trigger is a CDC source only.
func (Engine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, ErrNotImplemented
}

// OpenChangeApplier is not implemented: SQLite-trigger is a CDC source only —
// it never applies changes (a SQLite TARGET would use the `sqlite` engine).
func (Engine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, ErrNotImplemented
}

// OpenCDCReader returns a polling reader against `sluice_change_log` on the
// SQLite file identified by dsn. Refuses with a clear error when the change-log
// table is absent — the operator must run `sluice trigger setup` before the
// first sync. The caller closes the returned reader to release its connection
// pool.
func (Engine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return openCDCReader(ctx, dsn)
}

// init registers the engine with the engines registry. The blank import in
// cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}

// closeReader closes an [ir.SchemaReader] / [ir.RowReader] via the io.Closer
// probe the orchestrator uses: neither IR interface declares Close, but the
// SQLite concrete readers implement it to release their pool (and remove any
// materialized temp DB). A reader without a Close is a no-op.
func closeReader(r any) error {
	if c, ok := r.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// capabilities is the static [ir.Capabilities] value the engine declares: the
// composed sqlite engine's shape with CDC flipped to trigger-based. Computed
// once at init so the engine's Capabilities() method is a simple read and so the
// only intentional difference from the cold-start engine is spelled out here.
var capabilities = func() ir.Capabilities {
	c := sqlite.Engine{}.Capabilities()
	c.CDC = ir.CDCTriggers
	return c
}()

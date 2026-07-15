// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package mydumper implements a read-only sluice [ir.Engine] for mydumper
// output directories — the per-table flat-file dump format produced by
// mydumper/myloader and, byte-compatibly, by `pscale database dump`
// (PlanetScale's exporter is a mydumper-format writer; verified against the
// planetscale/cli source in docs/research/flat-file-sources.md). ADR-0161;
// roadmap item 55 Phase 1.
//
//	sluice migrate --source-driver mydumper --source /path/to/dumpdir \
//	  --target-driver postgres --target '<pg-dsn>'
//
// # Directory contract
//
// The --source is a DIRECTORY containing:
//
//   - `metadata` — dump-wide bookkeeping incl. the source's binlog
//     position / GTID set (parsed and logged at open; a future dump→CDC
//     handoff hook, ADR-0161 §8 — recorded, not built).
//   - `<db>.<table>-schema.sql[.gz|.zst]` — exactly ONE CREATE TABLE each.
//   - `<db>.<table>.<NNNNN>.sql[.gz|.zst]` — data chunks holding only
//     extended INSERT statements (~1 MB per statement).
//
// Auxiliary schema-only files mydumper may also emit (`<db>-schema-create`,
// `-schema-view`, `-schema-triggers`, `-schema-post`, per-table `-metadata`
// row-count files, checksums) are skipped with a WARN naming each — they
// carry no row data. Anything else in the directory is refused loudly:
// this reader never guesses at a file it does not recognise.
//
// # The contained DDL-parse exception (ADR-0161 §3)
//
// sluice's IR-first tenet forbids regex/grammar over DDL strings — engine
// schema knowledge belongs in catalog readers. A flat-file source has no
// catalog, so this package carries a DELIBERATELY BOUNDED exception (the
// ADR-0133 precedent: SQLite's sqlite_master DDL text): each schema file
// must contain optional comments/SET statements plus EXACTLY ONE
// CREATE TABLE, and the parser handles exactly that shape. Any other
// statement — a second CREATE, an ALTER, an INSERT, arbitrary SQL — is a
// loud refusal naming the file. The type mapping behind the parse is NOT
// forked: parsed column metadata funnels through the live MySQL engine's
// own translator ([mysql.TranslateColumnType]).
//
// # Value fidelity
//
// Data chunks decode through a faithful MySQL string-literal lexer (the
// full backslash escape set, quoted-quote doubling, `0x…` hex literals,
// `b'…'` bit literals) and the live MySQL engine's value decoder
// ([mysql.DecodeRowValue]), so every value family lands in the [ir.Row]
// contract (docs/value-types.md) byte-identical to a live-MySQL read of
// the same table. Integer literals are parsed as int64/uint64 directly
// from their decimal text — never through a float — so BIGINT UNSIGNED
// and int64 > 2^53 round-trip exactly (the D1 lesson). Binary columns
// work in BOTH dump shapes: vanilla mydumper's hex-blob (`0x…`) and the
// pscale writer's backslash-escaped strings (no hex-blob — binary
// fidelity there rides entirely on the escape decoder).
//
// Charset posture: values are carried as the dump's raw bytes, assumed
// UTF-8. A data chunk that declares a non-UTF-8 `SET NAMES`, or a
// table/column whose declared charset is outside the UTF-8-compatible set
// (utf8mb4/utf8/utf8mb3/ascii/binary), is REFUSED loudly rather than
// silently transcoded (ADR-0161 §5).
//
// # Capability shape
//
// A migrate SOURCE only — the d1 registry shape: Capabilities.CDC =
// CDCNone, BulkLoad = BulkLoadNone, and every write/CDC/snapshot Open*
// returns a wrapped [ErrNotImplemented]. `sluice verify --depth count`
// works (the schema reader implements [ir.Verifier] by chunk re-scan);
// sample depth is refused — file chunks have no cheap row addressing
// (ADR-0161 §9).
package mydumper

import (
	"context"
	"errors"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrNotImplemented is returned by the write/CDC/change-apply/snapshot
// Open* methods: a mydumper directory is a migrate source only (the same
// posture as the sqlite/d1 engines). Callers should check for it with
// [errors.Is].
var ErrNotImplemented = errors.New(
	"mydumper engine: not implemented (a mydumper dump directory is a migrate source only)",
)

// Engine is the mydumper implementation of [ir.Engine]. It holds no state;
// each Open* validates the dump directory afresh. The DSN is the dump
// directory's filesystem path.
type Engine struct{}

// Name returns the engine's CLI identifier (`--source-driver mydumper`).
func (Engine) Name() string { return "mydumper" }

// Capabilities declares the honest source-only shape: the dump carries a
// MySQL schema (flat namespace, unsigned integers, column-level ENUM/SET,
// binary JSON, native spatial types) but offers no CDC and is never a
// target (BulkLoadNone).
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader returns a [SchemaReader] over the dump directory named
// by dsn. The directory layout is validated here — a path that is not a
// mydumper dump (missing metadata file, no schema files, unattributable
// files) is refused loudly before anything is parsed. The metadata file's
// binlog position / GTID set is logged at INFO (the future CDC-handoff
// hook, ADR-0161 §8).
func (Engine) OpenSchemaReader(_ context.Context, dsn string) (ir.SchemaReader, error) {
	dir, err := openDumpDir(dsn)
	if err != nil {
		return nil, err
	}
	// Logged from the schema-reader open only (migrate/verify open both
	// readers; one position line per run is enough).
	dir.logSourcePosition()
	return &SchemaReader{dir: dir}, nil
}

// OpenRowReader returns a [RowReader] over the dump directory named by
// dsn, streaming each table's data chunks in order.
func (Engine) OpenRowReader(_ context.Context, dsn string) (ir.RowReader, error) {
	dir, err := openDumpDir(dsn)
	if err != nil {
		return nil, err
	}
	return &RowReader{dir: dir}, nil
}

// OpenSchemaWriter is not implemented: a dump directory is a source only.
func (Engine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, ErrNotImplemented
}

// OpenRowWriter is not implemented: a dump directory is a source only.
func (Engine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, ErrNotImplemented
}

// OpenCDCReader is not implemented: the engine declares CDCNone. The
// metadata file's binlog position is surfaced at open for a future
// dump→CDC handoff (ADR-0161 §8), but no CDC path exists here.
func (Engine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, ErrNotImplemented
}

// OpenChangeApplier is not implemented: a dump directory is a source only.
func (Engine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, ErrNotImplemented
}

// OpenSnapshotStream is not implemented: with no CDC there is no
// snapshot→CDC handoff to capture. Migrate (cold-copy only) is the
// supported path.
func (Engine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, ErrNotImplemented
}

// capabilities declares what this engine supports. The dump's schema and
// values are MySQL-dialect, so the type-facing declarations mirror the
// vanilla MySQL flavor; the operational declarations are the source-only
// flat-file shape (no bulk-load path — never a target; no CDC).
var capabilities = ir.Capabilities{
	BulkLoad:    ir.BulkLoadNone,
	CDC:         ir.CDCNone,
	SchemaScope: ir.SchemaScopeFlat,
	SupportedTypes: ir.NewTypeSet(
		ir.ExtEnum,     // column-level ENUM
		ir.ExtSet,      // column-level SET
		ir.ExtGeometry, // built-in spatial types (hex-blob dumps)
	),
	SupportsCheckConstraint:  true,
	SupportsGeneratedColumns: true,
	SupportsPartitioning:     false, // partition clauses are versioned comments; not carried
	EnumSupport:              ir.EnumColumnLevel,
	JSONSupport:              ir.JSONBinary,
	UnsignedIntegers:         true,
	DDLDialect:               ir.DDLDialectMySQL,
}

// init registers this engine with the engines registry. A blank import
// in cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}

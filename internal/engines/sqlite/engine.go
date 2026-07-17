// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package sqlite implements a read-only sluice [ir.Engine] for SQLite
// database files — and, by extension, Cloudflare D1 (ADR-0128/0130). The
// --source may be EITHER a binary SQLite `.db` OR a `.sql` TEXT dump (what
// `wrangler d1 export` emits: CREATE TABLE + INSERTs): the open path sniffs
// the first 16 bytes of the file and, when they are NOT the SQLite magic
// header, treats the file as a dump and MATERIALIZES it in-process into a
// temp SQLite database (via modernc — no `sqlite3` CLI), then reads that.
// So the Cloudflare D1 import is now a single command —
//
//	sluice migrate --source-driver sqlite --source dump.sql \
//	  --target-driver postgres --target <pg-dsn>
//
// — with no `sqlite3 app.db < dump.sql` step and no `_cf_KV` cleanup: D1's
// internal `_cf_*` tables are auto-skipped by the schema reader (ADR-0130).
// The temp DB is owned by the reader and removed on Close; a malformed dump
// fails loudly at materialize, naming the dump, before any data moves.
// (Validated end-to-end against real D1: current `d1 export` already omits
// `_cf_KV` and wraps nothing in BEGIN/COMMIT.) A native D1 HTTP-API reader
// remains a deferred follow-up.
//
// It is both a migrate SOURCE and a migrate TARGET: it implements
// [ir.SchemaReader] + [ir.RowReader] (a SQLite/D1 file imports into
// Postgres or MySQL) and [ir.SchemaWriter] + [ir.RowWriter] (any source —
// PG, MySQL, or another SQLite/D1 — migrates INTO a SQLite file, ADR-0134),
// both via the standard `sluice migrate` pipeline. The target write side is
// the faithful inverse of the reader, enabling X→SQLite→Cloudflare-D1 via
// `wrangler d1 import`. The CDC / change-apply Open* methods return
// [ErrNotImplemented]: SQLite is not a CDC source or target in this
// prototype (Capabilities.CDC = CDCNone). A native D1 HTTP-API reader
// (paginated REST + token auth) and trigger-based CDC are deferred
// follow-ups.
//
// The engine self-registers as "sqlite" when imported:
//
//	import _ "sluicesync.dev/sluice/internal/engines/sqlite"
//
// Driver: modernc.org/sqlite — the pure-Go, no-CGO driver — so the
// engine stays inside sluice's CGO_ENABLED=0 posture. The CGO driver
// (mattn/go-sqlite3) is deliberately NOT used.
//
// # Type-affinity mapping (the value-fidelity heart)
//
// SQLite is dynamically typed: a column has a declared type with an
// AFFINITY (one of INTEGER / TEXT / BLOB / REAL / NUMERIC, derived from
// the declared type per https://www.sqlite.org/datatype3.html §3.1),
// but each individual ROW stores its value in one of five STORAGE
// CLASSES (NULL / INTEGER / REAL / TEXT / BLOB) independent of the
// column's affinity. The schema reader maps the column's affinity to an
// IR type:
//
//	INTEGER affinity → ir.Integer (64-bit)
//	TEXT    affinity → ir.Text
//	BLOB    affinity → ir.Blob   (also the "no declared type" case)
//	REAL    affinity → ir.Float  (double precision)
//	NUMERIC affinity → ir.Decimal(unconstrained)
//
// SQLite has NO native DATE / TIME / BOOLEAN storage — dates are stored
// as TEXT/INTEGER/REAL by application convention and booleans as 0/1
// INTEGERs. The affinity mapping above is overridden for columns whose
// DECLARED type names a temporal/boolean shape (ADR-0129): a column
// declared DATETIME/TIMESTAMP → ir.Timestamp, DATE → ir.Date, TIME →
// ir.Time, BOOL/BOOLEAN → ir.Boolean (case-insensitive substring, in that
// precedence). An INTEGER-declared 0/1 column is NOT guessed as bool; only
// the explicit BOOL/BOOLEAN spelling triggers it.
//
// # Declared date/bool value decode (ADR-0129)
//
// The IR temporal *type* is unambiguous from the declared type, but the
// VALUE encoding (ISO text vs unix int vs julian real) is app-specific, and
// guessing wrong silently yields a wrong date — a value-fidelity violation.
// So the encoding is an EXPLICIT operator choice, --sqlite-date-encoding
// (DSN param sqlite_date_encoding), one of:
//
//	iso (DEFAULT) — temporal values are ISO-8601 TEXT; a non-TEXT storage
//	  class, or text matching no layout, is REFUSED LOUDLY.
//	unixepoch / unixmillis — INTEGER (or REAL) unix seconds / milliseconds.
//	julian — REAL/INTEGER Julian day number.
//
// A non-matching storage class for the active encoding is refused loudly
// (naming table/column/rowid + the value), never a guessed value. A
// BOOLEAN-declared column accepts INTEGER 0/1 and TEXT true|false|t|f|yes|
// no|1|0 (case-insensitive); any other value is refused. The documented
// escape hatch for an outlier column is `--type-override <col>=text`, which
// carries the raw SQLite value verbatim. See value_decode.go for the full
// encoding × storage-class matrix.
//
// PRECISION NOTE: a large integer stored in a REAL / DOUBLE-affinity column
// was already converted to float64 by SQLite AT STORAGE TIME (REAL affinity
// forces float; magnitudes > 2^53 lose precision there). sluice carries the
// already-lossy float64 faithfully — the loss is SQLite's, not sluice's.
// Big integers in INTEGER- or NUMERIC-affinity columns are exact (int64 /
// exact decimal string).
//
// # Per-row storage-class fidelity (loud failure beats silent corruption)
//
// The row reader decodes each cell by its ACTUAL storage class. When a
// value's storage class cannot be faithfully represented in the column's
// resolved IR type — e.g. a TEXT value in an INTEGER-affinity column, or
// a BLOB where text is expected — the read is REFUSED LOUDLY (a clear
// error naming the table, column, rowid, and the offending storage
// class) rather than silently coerced to a wrong-but-plausible value.
// See value_decode.go for the full affinity × storage-class matrix. A
// future opt-in override may relax this; the prototype refuses.
package sqlite

import (
	"context"
	"errors"
	"fmt"

	// Pure-Go (no-CGO) SQLite driver; registers database/sql driver
	// name "sqlite" via its init(). Confined to this package.
	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrNotImplemented is returned by the CDC / change-apply / snapshot-stream
// Open* methods, which have no SQLite implementation in this prototype —
// SQLite is a migrate source and target but not a CDC source or target
// (Capabilities.CDC = CDCNone). Callers should check for it with [errors.Is].
var ErrNotImplemented = errors.New("sqlite engine: not implemented (SQLite has no CDC / change-apply)")

// Engine is the SQLite implementation of [ir.Engine]. Each Open* call opens
// an independent *sql.DB against the file (read-only for the source reader
// surfaces, writable for the target writer surfaces).
//
// dateEncoding carries the operator's --sqlite-date-encoding default (ADR-0129),
// set via [Engine.WithDateEncoding] BEFORE the CLI opens any reader — the
// per-instance replacement for the former process-wide SetDefaultDateEncoding
// global (task 2.5 / finding A-4). The zero value dateEncodingInherit means
// "unset" and resolves to the ISO default, so a bare Engine{} (registry, tests,
// non-CLI callers) reads temporal columns as ISO-8601 text exactly as before.
type Engine struct {
	dateEncoding dateEncoding

	// stageDir is where a materialized `.sql`-dump temp database is created
	// (--stage-dir / SLUICE_STAGE_DIR, set via [Engine.WithStageDir]). Empty —
	// the zero value — keeps the os.TempDir default; the materialized copy is
	// roughly the database's size, which overwhelms a tmpfs /tmp on large
	// dumps (the ADR-0145 hazard class the flatfile/parquet stage paths
	// already honor the flag for). Irrelevant to binary `.db` sources.
	stageDir string
}

// Name returns the engine's short identifier as used in configuration
// and on the command line (`--source-driver sqlite`).
func (Engine) Name() string { return "sqlite" }

// WithDateEncoding returns a copy of the engine carrying the operator's
// --sqlite-date-encoding default (ADR-0129; task 2.5, replacing
// SetDefaultDateEncoding). It validates the value (kong already enum-checks it;
// this re-checks defensively) and refuses loudly on a bad one. The per-source
// `sqlite_date_encoding` DSN param still wins over this default at OpenRowReader.
// An empty string keeps the iso default. Returns a configured copy — the
// registry's engine value stays default-free — mirroring [ir.ConnectionLabeler].
func (e Engine) WithDateEncoding(enc string) (ir.Engine, error) {
	d, err := parseDateEncoding(enc)
	if err != nil {
		return nil, fmt.Errorf("sqlite: invalid --sqlite-date-encoding %q (%w)", enc, err)
	}
	e.dateEncoding = d
	return e, nil
}

// WithStageDir returns a copy of the engine carrying the operator's
// --stage-dir override for the `.sql`-dump materialize (ADR-0130). No
// validation here — a missing directory refuses loudly at open, naming
// the flag (the same posture as the flatfile staging path). Empty keeps
// the os.TempDir default. Mirrors [Engine.WithDateEncoding]'s
// configured-copy shape — the registry's engine value stays default-free.
func (e Engine) WithStageDir(dir string) ir.Engine {
	e.stageDir = dir
	return e
}

// Capabilities returns the static capability declaration for the SQLite
// migrate source. Declared honestly: no CDC, no extension types, a flat
// table namespace, and no bulk-load target path (it is never a target).
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader returns a [SchemaReader] bound to the SQLite file
// identified by dsn (a filesystem path, `file:` URI, or `sqlite://`
// URL). The caller is responsible for closing the returned reader.
func (e Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	// Schema reading infers the IR type from the declared type alone
	// (ADR-0129), so the per-source date encoding is irrelevant here and the
	// resolved value is discarded. tempPath is the materialized dump DB (empty
	// for a real `.db`); the reader removes it on Close.
	db, path, _, tempPath, err := openReadOnly(ctx, dsn, e.stageDir)
	if err != nil {
		return nil, err
	}
	return &SchemaReader{db: db, path: path, tempPath: tempPath}, nil
}

// OpenRowReader returns a [RowReader] bound to the SQLite file
// identified by dsn. The caller is responsible for closing the returned
// reader. The per-source date encoding (the `sqlite_date_encoding` DSN
// param, or the engine --sqlite-date-encoding default folded at OpenRowReader — ADR-0129 / task 2.5) is resolved here and
// carried on the reader for value decode.
func (e Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	db, path, enc, tempPath, err := openReadOnly(ctx, dsn, e.stageDir)
	if err != nil {
		return nil, err
	}
	// The per-source DSN param wins; absent, the engine's --sqlite-date-encoding
	// default applies (task 2.5). Both may be inherit → decode resolves to ISO.
	return &RowReader{db: db, path: path, dateEnc: foldDateEncoding(enc, e.dateEncoding), tempPath: tempPath}, nil
}

// OpenSchemaWriter returns a [SchemaWriter] bound to the SQLite target
// file identified by dsn (created if absent). The connection is writable
// with FK enforcement off for the inline-FK / unordered bulk-copy model
// (ADR-0134). The caller is responsible for closing the returned writer.
func (Engine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	db, path, err := openWritable(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &SchemaWriter{db: db, path: path}, nil
}

// OpenRowWriter returns a [RowWriter] bound to the SQLite target file
// identified by dsn (created if absent). The caller is responsible for
// closing the returned writer.
func (Engine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	db, path, err := openWritable(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &RowWriter{db: db, path: path}, nil
}

// OpenCDCReader is not implemented: SQLite declares CDCNone in this
// prototype (trigger-based CDC is a deferred follow-up, ADR-0128).
func (Engine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, ErrNotImplemented
}

// OpenChangeApplier is not implemented: SQLite is a migrate source only.
func (Engine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, ErrNotImplemented
}

// OpenSnapshotStream is not implemented: SQLite has no CDC, so there is
// no snapshot→CDC handoff to capture. Migrate (cold-copy only) is the
// supported path.
func (Engine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, ErrNotImplemented
}

// capabilities declares what this engine supports. SQLite is a migrate
// source AND target with no CDC. BulkLoad is BatchedInsert: SQLite's
// fast-load path is a multi-row parameterised INSERT inside one
// transaction (there is no COPY/LOAD DATA), which makes it a valid target
// (ADR-0134). No extension types. SchemaScope is flat (a single table
// namespace per database file). JSON is stored as TEXT (no distinct type)
// and read by affinity, so JSONSupport is None. CHECK constraints and
// generated columns ARE read and carried (ADR-0133), so both are declared
// true; partitioning has no SQLite equivalent and stays false.
var capabilities = ir.Capabilities{
	BulkLoad:                 ir.BulkLoadBatchedInsert,
	CDC:                      ir.CDCNone,
	SchemaScope:              ir.SchemaScopeFlat,
	SupportedTypes:           ir.NewTypeSet(), // no extension types
	SupportsCheckConstraint:  true,
	SupportsGeneratedColumns: true,
	SupportsPartitioning:     false,
	EnumSupport:              ir.EnumNone,
	JSONSupport:              ir.JSONNone,
	UnsignedIntegers:         false,
	DDLDialect:               ir.DDLDialectANSI,
}

// init registers this engine with the engines registry. A blank import
// in cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}

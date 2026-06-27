// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package sqlite implements a read-only sluice [ir.Engine] for SQLite
// database files — and, by extension, Cloudflare D1 (ADR-0128). Note
// `wrangler d1 export` emits a `.sql` TEXT dump (CREATE TABLE + INSERTs),
// NOT a binary SQLite file, so the D1 flow is: export to dump.sql, then
// materialize a file with `sqlite3 app.db < dump.sql` (strip D1's internal
// `_cf_KV` table, or `--exclude-table _cf_KV`), then point sluice at
// app.db. Accepting a `.sql` dump directly (materialized in-process) is a
// deferred ergonomic follow-up.
//
// It is a MIGRATE SOURCE only: it implements [ir.SchemaReader] and
// [ir.RowReader] so a SQLite/D1 file can be imported into Postgres or
// MySQL via the standard `sluice migrate` pipeline. The write-side and
// CDC Open* methods return [ErrNotImplemented]: SQLite cannot be a
// sluice target or a CDC source in this prototype (Capabilities.CDC =
// CDCNone). A native D1 HTTP-API reader (paginated REST + token auth)
// and trigger-based CDC are deferred follow-ups.
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

	// Pure-Go (no-CGO) SQLite driver; registers database/sql driver
	// name "sqlite" via its init(). Confined to this package.
	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrNotImplemented is returned by the Open* methods that have no SQLite
// implementation in this prototype — SQLite is a migrate SOURCE only, so
// the write-side, CDC, and snapshot-stream surfaces are intentionally
// absent. Callers should check for it with [errors.Is].
var ErrNotImplemented = errors.New("sqlite engine: not implemented (SQLite is a migrate source only)")

// Engine is the SQLite implementation of [ir.Engine]. It holds no
// connection state; the zero value is fully usable. Each Open* call
// opens an independent read-only *sql.DB against the file.
type Engine struct{}

// Name returns the engine's short identifier as used in configuration
// and on the command line (`--source-driver sqlite`).
func (Engine) Name() string { return "sqlite" }

// Capabilities returns the static capability declaration for the SQLite
// migrate source. Declared honestly: no CDC, no extension types, a flat
// table namespace, and no bulk-load target path (it is never a target).
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader returns a [SchemaReader] bound to the SQLite file
// identified by dsn (a filesystem path, `file:` URI, or `sqlite://`
// URL). The caller is responsible for closing the returned reader.
func (Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	// Schema reading infers the IR type from the declared type alone
	// (ADR-0129), so the per-source date encoding is irrelevant here and the
	// resolved value is discarded.
	db, path, _, err := openReadOnly(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &SchemaReader{db: db, path: path}, nil
}

// OpenRowReader returns a [RowReader] bound to the SQLite file
// identified by dsn. The caller is responsible for closing the returned
// reader. The per-source date encoding (the `sqlite_date_encoding` DSN
// param, or the process-global default — ADR-0129) is resolved here and
// carried on the reader for value decode.
func (Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	db, path, enc, err := openReadOnly(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &RowReader{db: db, path: path, dateEnc: enc}, nil
}

// OpenSchemaWriter is not implemented: SQLite is a migrate source only.
func (Engine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, ErrNotImplemented
}

// OpenRowWriter is not implemented: SQLite is a migrate source only.
func (Engine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, ErrNotImplemented
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

// capabilities declares what this engine supports. SQLite is a
// read-only migrate source: no CDC, no bulk-load target, no extension
// types. SchemaScope is flat (SQLite has a single table namespace per
// database file). JSON is stored as TEXT (no distinct type) and is read
// by affinity, so JSONSupport is None. CHECK constraints, generated
// columns, and partitioning are not read by the prototype reader and so
// are declared false rather than over-promising.
var capabilities = ir.Capabilities{
	BulkLoad:                 ir.BulkLoadNone,
	CDC:                      ir.CDCNone,
	SchemaScope:              ir.SchemaScopeFlat,
	SupportedTypes:           ir.NewTypeSet(), // no extension types
	SupportsCheckConstraint:  false,
	SupportsGeneratedColumns: false,
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

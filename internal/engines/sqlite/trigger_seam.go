// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// This file is the minimal exported seam the sibling `sqlite-trigger` CDC engine
// (ADR-0135) reuses so there is exactly ONE faithful-capture/decode
// implementation across the file reader, the D1 reader, and the trigger reader:
//
//   - [ChangeLogTable] / [ChangeLogMetaTable] — the trigger engine's bookkeeping
//     table names, owned here because [SchemaReader] must skip them (so they are
//     never themselves migrated or captured).
//   - [CapturedValueExpr] / [CapturedTypeofExpr] — the (typeof, text/hex) capture
//     SQL, shared with [buildD1Projection] so the capture trigger and the readers
//     can never drift on the encoding.
//   - [ReconstructStorageValue] + [CapturedCellDecoder] — the reconstruction
//     ([d1StorageValue]) and the storage-class-faithful [decodeCell] + ADR-0129
//     date/bool policy, so a captured change decodes byte-identically to a
//     cold-start row.
//   - [OpenFile] — open a real SQLite file (no dump sniff) sharing this package's
//     DSN normalization.

// ChangeLogTable and ChangeLogMetaTable are the source-side bookkeeping tables
// the `sqlite-trigger` CDC engine (ADR-0135) installs. They are defined here —
// not in the sqlite-trigger package — because [SchemaReader.tableNames] must
// EXCLUDE them so a cold-start (or a plain `sluice migrate`) never copies the
// change-log, and the trigger setup never installs a recursive capture trigger
// on the log itself. The sqlite-trigger package references these constants so
// the spelling is shared (no drift between the skip set and the installer).
const (
	ChangeLogTable     = "sluice_change_log"
	ChangeLogMetaTable = "sluice_change_log_meta"
)

// CapturedTypeofExpr returns the storage-class half of the faithful-capture pair
// for a column reference: `typeof(<colExpr>)`. colExpr is the already-quoted
// reference (e.g. `NEW."id"` in a trigger body, or `"id"` in a SELECT).
func CapturedTypeofExpr(colExpr string) string {
	return "typeof(" + colExpr + ")"
}

// CapturedValueExpr returns the VALUE half of the faithful-capture pair: the
// SQL scalar expression that encodes one column as the EXACT text/hex the
// (typeof, text/hex) contract requires (ADR-0132 §4 / ADR-0135 §crux):
//
//	blob → hex(c)                  — recovered with hex.DecodeString
//	real → format('%.17g', c)      — 17 sig-digits = IEEE-754 round-trip exact
//	else → CAST(c AS TEXT)         — integers carry EXACT decimal text (> 2^53
//	                                 included, where a bare JSON number rounds);
//	                                 NULL stays NULL
//
// It is the SINGLE definition of the faithful encoding, shared by the file/D1
// reader projection ([buildD1Projection]) and the sqlite-trigger capture trigger
// body, so capture and read can never drift. colExpr is the already-quoted
// column reference.
func CapturedValueExpr(colExpr string) string {
	return "CASE typeof(" + colExpr + ") WHEN 'blob' THEN hex(" + colExpr +
		") WHEN 'real' THEN format('%.17g', " + colExpr + ") ELSE CAST(" + colExpr + " AS TEXT) END"
}

// ReconstructStorageValue reconstructs the SAME Go storage-class value the
// modernc file path hands back (int64 / float64 / string / []byte / nil) from a
// captured (typeof, text/hex) pair. valueRaw is the JSON value carried under the
// capture object's `v` key (a JSON string for a non-NULL cell, JSON null for a
// NULL cell). It is the exported handle to [d1StorageValue] so the sqlite-trigger
// reader inherits the proven, big-int-exact reconstruction (ADR-0135 §crux)
// rather than duplicating it. The reconstructed value must still be passed to
// [decodeCell] (via [CapturedCellDecoder.Decode]) to apply the column's IR type
// and the date/bool policy.
func ReconstructStorageValue(typeofText string, valueRaw json.RawMessage) (any, error) {
	return d1StorageValue(typeofText, valueRaw)
}

// CapturedCellDecoder decodes faithfully-captured (typeof, text/hex) cell pairs
// for the sqlite-trigger CDC reader. It bundles the two reuse points the task
// requires — the [ReconstructStorageValue] reconstruction AND the shared
// storage-class-faithful [decodeCell] with the ADR-0129 date/bool policy — so a
// captured change decodes byte-identically to a cold-start snapshot row. The
// date encoding is resolved ONCE at construction (per-source DSN param, else the
// process-global --sqlite-date-encoding default).
type CapturedCellDecoder struct {
	enc dateEncoding
}

// NewCapturedCellDecoderForDSN builds a [CapturedCellDecoder] whose date/bool
// policy matches what the sqlite file reader would use for the same --source
// DSN: the per-source `sqlite_date_encoding` param if present, else the
// process-global default (ADR-0129). Reusing [dsnFormParts] keeps the resolution
// identical to [Engine.OpenRowReader], so the sqlite-trigger CDC path's temporal
// and boolean decode is byte-identical to the cold-start snapshot reader.
func NewCapturedCellDecoderForDSN(dsn string) (*CapturedCellDecoder, error) {
	_, _, enc, err := dsnFormParts(dsn)
	if err != nil {
		return nil, err
	}
	return &CapturedCellDecoder{enc: enc}, nil
}

// Decode reconstructs one captured cell into its faithful IR Row value: it
// rebuilds the Go storage-class value from the (typeof, valueRaw) pair, then
// applies the column's resolved IR type t and the resolved date/bool policy via
// the shared [decodeCell] — inheriting its refuse-not-coerce loud-failure
// contract (a storage class that cannot be faithfully held in t is an error, not
// a silent coercion). The caller wraps the returned error with table/column.
func (d *CapturedCellDecoder) Decode(typeofText string, valueRaw json.RawMessage, t ir.Type) (any, error) {
	storage, err := d1StorageValue(typeofText, valueRaw)
	if err != nil {
		return nil, err
	}
	return decodeCell(storage, t, resolveDateEncoding(d.enc))
}

// OpenFile opens a REAL SQLite database file (a persistent `.db`, NOT a `.sql`
// dump — there is no dump sniff/materialize, because the sqlite-trigger CDC
// source must be a durable writable file the app and the poller both connect to)
// and returns a verified *sql.DB plus the display path. readOnly applies the
// query_only + busy_timeout pragmas (the CDC poller's read connection);
// otherwise busy_timeout only (the trigger setup/teardown DDL connection — NO
// query_only so CREATE TABLE/TRIGGER can run, and NO journal-mode change so the
// operator's WAL/rollback choice is left untouched, ADR-0135 §5). The
// `sqlite_date_encoding` DSN param is stripped before the DSN reaches the driver.
// Reuses this package's [dsnFormParts] so the sqlite-trigger engine shares the
// exact DSN normalization rather than reimplementing it.
func OpenFile(ctx context.Context, dsn string, readOnly bool) (db *sql.DB, path string, err error) {
	base, path, _, err := dsnFormParts(dsn)
	if err != nil {
		return nil, "", err
	}
	pragmas := busyTimeoutPragma
	if readOnly {
		pragmas = readOnlyPragmas
	}
	db, err = sql.Open("sqlite", appendPragmas(base, pragmas))
	if err != nil {
		return nil, "", fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, "", fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	return db, path, nil
}

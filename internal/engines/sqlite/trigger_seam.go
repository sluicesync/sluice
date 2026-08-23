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
	// ChangeLogColumnsTable records, per replicated table, the EXACT
	// non-generated column set the capture triggers were built against
	// (ADR-0135). The CDC reader compares it against the live schema at stream
	// START to detect an un-re-setup source schema change in EITHER direction —
	// in particular an ADD COLUMN, whose new values the stale trigger would
	// otherwise SILENTLY drop. Also skipped by the schema reader.
	ChangeLogColumnsTable = "sluice_change_log_columns"
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
//	real → format('%!.20g', c)     — see below; the `!` flag is load-bearing
//	else → CAST(c AS TEXT)         — integers carry EXACT decimal text (> 2^53
//	                                 included, where a bare JSON number rounds);
//	                                 NULL stays NULL
//
// The REAL arm's history is a corrected premise (found by the 2026-08-22
// adversarial-corpus sweep; mechanism re-verified by direct experiment on
// bundled SQLite 3.53.3, 2026-08-23). The original `format('%.17g', c)` relied
// on the C-printf guarantee that 17 significant digits round-trip an IEEE-754
// double — but SQLite's printf is NOT C's printf: its `%g`/`%.Ng` conversion
// CAPS output at 16 significant digits (raised to 26 only by the `!`
// alternate-form-2 flag), so `%.17g` silently clamps to 16 — and 16 digits do
// not round-trip every binary-64 (17 are needed in the worst case). Precision
// IS honoured below the cap and clamped at it: `%.15g`→15 digits, but
// `%.16g`/`%.17g`/`%.20g`/`%.25g` ALL emit the same 16-digit render
// (`0.30000000000000004`→"0.3"; `0.12345678901234568`→"0.1234567890123457", a
// different double), so every REAL captured or projected through `%.17g`
// silently lost low bits at exit 0. This is NOT a SQLite 3.43 change — the
// 3.43.0 release notes carry no such printf rewrite (their only float change
// is to the `decimal` extension); the 16-digit cap is longstanding printf
// behaviour, so `%.17g` was never lossless here — the pre-corpus test passed
// only because its seed (π) round-trips at ≤16 digits. MEASURED on modernc's
// SQLite 3.53.3 (2295 of 5007 swept doubles fail through `%.17g`/`%.20g`/
// `%.25g` ALIKE — the 16-cap signature, not a variable shortest-form) and on
// real Cloudflare D1 (same 16-digit render, d1verify 2026-08-22). The fix is
// the alternate-form-2 flag `%!.20g`, which lifts the cap so 17+ digits emit —
// LOSSLESS (0 misses over the same sweep) on modernc 3.53.3 and real D1.
// UNVERIFIED PREMISE (third-party SQLite predating the `!` flag, e.g. an app
// library firing a local capture trigger): if `!` is absent/ignored there,
// `%!.20g` ALSO clamps to 16 and the fix is INEFFECTIVE — not merely
// "degraded". sluice's own connections and D1 are measured; older third-party
// SQLite is not.
// TestCapturedValueExpr_RealRenderRoundTripsExactly is the per-PR gate on the
// bundled SQLite; the d1verify adversarial matrix is the live-D1 pin.
//
// It is the SINGLE definition of the faithful encoding, shared by the file/D1
// reader projection ([buildD1Projection], which the --stage-local staging copy
// also rides) and the sqlite-trigger / d1-trigger capture trigger body, so
// capture and read can never drift. colExpr is the already-quoted column
// reference. NOTE: capture triggers INSTALLED by an older sluice keep the old
// lossy expression until `trigger setup` is re-run — the CDC reader refuses at
// stream start when an installed capture trigger body does not match this
// expression (the sqlite-trigger capture-shape door), so that stale-install
// case is loud, never silent.
func CapturedValueExpr(colExpr string) string {
	return "CASE typeof(" + colExpr + ") WHEN 'blob' THEN hex(" + colExpr +
		") WHEN 'real' THEN format('%!.20g', " + colExpr + ") ELSE CAST(" + colExpr + " AS TEXT) END"
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

// JSONStringValue is the exported handle to the GUARDED jsonString decode
// (audit SQT-1): it refuses the two shapes encoding/json silently rewrites to
// U+FFFD — raw invalid-UTF-8 bytes in the string token, and lone-surrogate
// \u escapes — instead of delivering rewritten text. The d1-trigger CDC
// transport extracts its change-log cells through this so the guard sits at
// EVERY string extraction, not only the storage-value decode one layer down:
// the before/after payloads it pulls are the captured row images, and a
// mangle there would arrive at the (guarded) inner decode already valid
// UTF-8 and pass — the exact one-layer-up sibling the pre-v0.122.0
// value-fidelity review caught.
func JSONStringValue(raw json.RawMessage) (s string, ok bool, err error) {
	return jsonString(raw)
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
// policy is the per-source `sqlite_date_encoding` param if present, else ISO
// (ADR-0129). NOTE (task 2.5): the trigger-CDC decoder does NOT receive the
// engine's --sqlite-date-encoding default — only the `sqlite`/`d1` migrate-source
// readers fold that per-instance default (finding A-4 removed the process global).
// A `sqlite-trigger` source relying on the CLI default WITHOUT the DSN param now
// decodes as ISO (a loud storage-class refusal on mismatch, never silent-wrong);
// set the DSN param to carry it. Folding the engine default through the trigger
// backends is a scoped follow-up.
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

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/orware/sluice/internal/ir"
)

// loadDataReaderPrefix names the handler-registry slots this engine
// installs into go-sql-driver/mysql for LOAD DATA LOCAL INFILE
// streaming. Each WriteRows call mints a unique suffix so concurrent
// writers don't collide on the package-level registry.
const loadDataReaderPrefix = "sluice_loaddata_"

// writeLoadData streams rows to MySQL via LOAD DATA LOCAL INFILE using
// the go-sql-driver Reader-handler protocol. The serializer encodes
// rows as TSV with MySQL's default escape rules (tab/newline/backslash
// escaped, NUL → \0, NULL → \N) and pipes them to the driver, which
// forwards bytes to the server. No real file is ever written.
//
// On any failure (server with local_infile=OFF, geometry column
// present, runtime serialization error) the caller falls through to
// writeBatched. The fallback is best-effort: if writeBatched then
// fails too, that error wins.
func (w *RowWriter) writeLoadData(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	cols := nonGeneratedColumns(table.Columns)
	if len(cols) == 0 {
		return fmt.Errorf("mysql: LOAD DATA: table %q has no insertable columns", table.Name)
	}
	// LOAD DATA can't natively round-trip MySQL's geometry wire format
	// (SRID-prefixed WKB through TSV would need ST_GEOMFROMWKB on the
	// server side, which isn't expressible in column-only LOAD DATA).
	// Fall back to BatchedInsert when geometry is present.
	for _, c := range cols {
		if _, isGeom := c.Type.(ir.Geometry); isGeom {
			slog.WarnContext(ctx, "mysql: LOAD DATA: falling back to batched INSERT (table has geometry column)",
				slog.String("table", table.Name),
				slog.String("column", c.Name),
			)
			return w.writeBatched(ctx, table, rows)
		}
	}

	enabled, err := w.checkLocalInfile(ctx)
	if err != nil {
		return fmt.Errorf("mysql: LOAD DATA: probe @@local_infile: %w", err)
	}
	if !enabled {
		slog.WarnContext(ctx, "mysql: LOAD DATA: server has local_infile=OFF; falling back to batched INSERT",
			slog.String("hint", "set local_infile=ON on the server (or pass --local-infile=ON to mysqld) for ~5–10x faster bulk load"),
			slog.String("table", table.Name),
		)
		return w.writeBatched(ctx, table, rows)
	}

	name, err := mintReaderName()
	if err != nil {
		return fmt.Errorf("mysql: LOAD DATA: generate handler name: %w", err)
	}

	pr, pw := io.Pipe()
	driver.RegisterReaderHandler(name, func() io.Reader { return pr })
	defer driver.DeregisterReaderHandler(name)

	// Producer: serialize rows to TSV bytes on the pipe writer.
	// CloseWithError propagates serializer failures to the driver-
	// side read so the LOAD DATA statement aborts cleanly instead of
	// hanging on a half-written stream.
	encErr := make(chan error, 1)
	go func() {
		err := encodeRowsTSV(ctx, pw, cols, rows)
		// Always close the pipe writer so the driver's read loop
		// terminates.
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		encErr <- err
	}()

	stmt := buildLoadDataStmt(w.schema, table.Name, cols, name)
	_, execErr := w.db.ExecContext(ctx, stmt)

	// Wait for the encoder so we don't leak a goroutine.
	serErr := <-encErr

	// Encoder error is the most informative; surface it first.
	if serErr != nil && !errors.Is(serErr, io.ErrClosedPipe) {
		return fmt.Errorf("mysql: LOAD DATA: serialize rows for %q: %w", table.Name, serErr)
	}
	if execErr != nil {
		return fmt.Errorf("mysql: LOAD DATA into %q: %w", table.Name, execErr)
	}
	return nil
}

// checkLocalInfile reports whether the server's `local_infile` system
// variable is enabled. MySQL 8.0+ ships with it OFF by default, in
// which case any LOAD DATA LOCAL INFILE statement fails server-side.
// Pre-flighting here lets the writer fall back to BatchedInsert with
// one WARN line instead of crashing mid-stream.
func (w *RowWriter) checkLocalInfile(ctx context.Context) (bool, error) {
	var v string
	if err := w.db.QueryRowContext(ctx, "SELECT @@local_infile").Scan(&v); err != nil {
		return false, err
	}
	v = strings.TrimSpace(v)
	return v == "1" || strings.EqualFold(v, "ON"), nil
}

// buildLoadDataStmt returns the LOAD DATA LOCAL INFILE statement that
// pulls bytes from the named registered reader.
//
// Two MySQL-isms shape the statement form:
//
//   - CHARACTER SET binary disables the per-stream utf8mb4 validation
//     pass; raw bytes in BLOB/VARBINARY columns (the destination of
//     `[]byte` IR values) would otherwise trip Error 1300 ("Invalid
//     utf8mb4 character string") on the first non-ASCII byte.
//   - With CHARACTER SET binary, JSON columns reject input as
//     "Cannot create a JSON value from a string with CHARACTER SET
//     'binary'" because the JSON validator demands a Unicode charset.
//     The fix is to load every field into a user variable and assign
//     real columns via a SET clause — JSON columns get a CONVERT()
//     wrapper that re-tags the bytes as utf8mb4 (the serializer
//     already emits valid UTF-8 for JSON values per prepareValue).
//
// Generated columns are skipped upstream so the column list and the
// per-row TSV stream stay in lockstep.
func buildLoadDataStmt(schema, tableName string, cols []*ir.Column, readerName string) string {
	target := quoteIdent(tableName)
	if schema != "" {
		target = quoteIdent(schema) + "." + quoteIdent(tableName)
	}

	// Per-column user variables: `@c0, @c1, ...`. Avoids identifier
	// collisions with the columns themselves (a column literally
	// named after the original would otherwise alias the variable).
	vars := make([]string, len(cols))
	setParts := make([]string, len(cols))
	for i, c := range cols {
		vars[i] = fmt.Sprintf("@c%d", i)
		setParts[i] = quoteIdent(c.Name) + " = " + columnSetExpr(c, vars[i])
	}

	return fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s "+
			"CHARACTER SET binary "+
			"FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' "+
			"LINES TERMINATED BY '\\n' "+
			"(%s) SET %s",
		readerName, target,
		strings.Join(vars, ", "),
		strings.Join(setParts, ", "),
	)
}

// columnSetExpr returns the SET-clause RHS for col, given the user
// variable that holds its raw input. JSON columns get a CONVERT()
// wrapper so MySQL's JSON parser sees utf8mb4-tagged bytes; every
// other column type takes the variable verbatim — MySQL's implicit
// conversion handles VARCHAR/TEXT (binary→utf8mb4 reinterpretation),
// numerics (string parse), and binary types (passthrough).
func columnSetExpr(col *ir.Column, varName string) string {
	// ir.JSON and ir.Array both emit a MySQL `JSON` column
	// (emitColumnType maps ir.Array → JSON; the IR keeps the source
	// type ir.Array). Under `CHARACTER SET binary` MySQL's JSON
	// validator rejects un-retagged bytes with Error 3144 ("Cannot
	// create a JSON value from a string with CHARACTER SET 'binary'"),
	// so both need the utf8mb4 CONVERT wrapper — the LOAD DATA half of
	// the value-side ir.Array→JSON fix in prepareValue (Bug 18). The
	// serializer already emits valid UTF-8 JSON text for an array
	// value; this is only a charset re-tag.
	switch col.Type.(type) {
	case ir.JSON, ir.Array:
		return "CONVERT(" + varName + " USING utf8mb4)"
	}
	// Text-like columns: re-tag the binary stream as utf8mb4 so
	// CHECK constraints and stored procedures see the column's
	// declared charset rather than `binary`. The bytes themselves
	// are unchanged.
	switch col.Type.(type) {
	case ir.Varchar, ir.Text, ir.Set:
		return "CONVERT(" + varName + " USING utf8mb4)"
	}
	// Bug 48 fix: PG extensions with cross-engine default translators
	// (hstore → MySQL JSON; citext → MySQL VARCHAR-with-collation) keep
	// their `ir.ExtensionType` shape in the IR until the writer emits;
	// the value-side `prepareHstoreToJSON` translator runs at INSERT
	// time but doesn't touch the LOAD DATA path. Without the CONVERT
	// wrapper, MySQL parses the hstore→JSON bytes as charset=binary and
	// rejects with "Cannot create a JSON value from a string with
	// CHARACTER SET 'binary'" (Error 3144 / SQLSTATE 22032). Apply the
	// same utf8mb4 reinterpretation that ir.JSON / ir.Varchar / ir.Text
	// get — the bytes already are UTF-8; this is just a charset tag.
	// vector / pg_trgm / postgis don't reach this path (they refuse at
	// the cross-engine preflight); hstore and citext are the only
	// ExtensionType arms with cross-engine default translators today.
	if _, isExt := col.Type.(ir.ExtensionType); isExt {
		return "CONVERT(" + varName + " USING utf8mb4)"
	}
	return varName
}

// mintReaderName generates a unique registry slot name for one
// WriteRows invocation. Random hex suffix keeps concurrent writers
// from colliding on the package-level handler registry.
func mintReaderName() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return loadDataReaderPrefix + hex.EncodeToString(buf[:]), nil
}

// encodeRowsTSV consumes rows from the channel, runs each value
// through prepareValue + tsvEncode, and writes tab-separated lines to
// w until the channel closes or ctx is cancelled. Returns the first
// error it encounters.
func encodeRowsTSV(ctx context.Context, w io.Writer, cols []*ir.Column, rows <-chan ir.Row) error {
	// One reusable byte buffer so the per-row hot path doesn't
	// allocate per-field. The serializer writes to buf, then flushes
	// the row at end-of-line.
	buf := make([]byte, 0, 4096)

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				return nil
			}
			buf = buf[:0]
			for i, c := range cols {
				if i > 0 {
					buf = append(buf, '\t')
				}
				v := prepareValue(row[c.Name], c)
				var err error
				buf, err = tsvEncode(buf, v)
				if err != nil {
					return err
				}
			}
			buf = append(buf, '\n')
			if _, err := w.Write(buf); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tsvEncode appends one MySQL-LOAD-DATA-escaped field to dst and
// returns the new slice. NULL values emit the literal `\N` (the
// default ESCAPED BY sequence MySQL recognises). Strings and byte
// slices are escaped per MySQL's rules: tab/newline/CR/backslash/NUL
// are backslash-escaped.
func tsvEncode(dst []byte, v any) ([]byte, error) {
	if v == nil {
		return append(dst, '\\', 'N'), nil
	}
	switch x := v.(type) {
	case string:
		return appendEscapedString(dst, x), nil
	case []byte:
		return appendEscapedBytes(dst, x), nil
	case bool:
		if x {
			return append(dst, '1'), nil
		}
		return append(dst, '0'), nil
	case int:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case uint:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(dst, x, 10), nil
	case float32:
		return strconv.AppendFloat(dst, float64(x), 'g', -1, 32), nil
	case float64:
		return strconv.AppendFloat(dst, x, 'g', -1, 64), nil
	case time.Time:
		// MySQL's canonical DATETIME literal format. UTC is forced
		// because the engine connects with `loc=UTC` (see
		// connect.go); preserving the original location here would
		// silently re-interpret times under the session's timezone.
		return x.UTC().AppendFormat(dst, "2006-01-02 15:04:05.999999"), nil
	default:
		return nil, fmt.Errorf("mysql: LOAD DATA: unsupported value type %T", v)
	}
}

// appendEscapedString writes s to dst with MySQL's LOAD DATA escape
// rules applied. The empty string is emitted as zero bytes between
// field separators — MySQL reads that as the column's default if NOT
// NULL (typically empty string), matching the BatchedInsert path's
// behaviour for empty-string values.
func appendEscapedString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		dst = appendEscapedByte(dst, s[i])
	}
	return dst
}

// appendEscapedBytes is the []byte twin of appendEscapedString. Kept
// as a separate function so the hot path on string values doesn't pay
// the conversion cost.
func appendEscapedBytes(dst, b []byte) []byte {
	for i := 0; i < len(b); i++ {
		dst = appendEscapedByte(dst, b[i])
	}
	return dst
}

// appendEscapedByte applies the four MySQL LOAD DATA escapes that
// matter under the default (TAB-separated, backslash-escape) format:
//
//   - 0x00 → "\0"
//   - 0x09 → "\t"
//   - 0x0a → "\n"
//   - 0x0d → "\r"
//   - 0x5c → "\\"
//
// Other bytes pass through unchanged. This matches the inverse rules
// the MySQL server applies on input.
func appendEscapedByte(dst []byte, c byte) []byte {
	switch c {
	case 0x00:
		return append(dst, '\\', '0')
	case '\t':
		return append(dst, '\\', 't')
	case '\n':
		return append(dst, '\\', 'n')
	case '\r':
		return append(dst, '\\', 'r')
	case '\\':
		return append(dst, '\\', '\\')
	default:
		return append(dst, c)
	}
}

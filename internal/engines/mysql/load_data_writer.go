// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
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
			slog.WarnContext(
				ctx, "mysql: LOAD DATA: falling back to batched INSERT (table has geometry column)",
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
		slog.WarnContext(
			ctx, "mysql: LOAD DATA: server has local_infile=OFF; falling back to batched INSERT",
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

	// Pin a single connection for the LOAD DATA + post-load warning
	// check (Bugs 102/103/106 closure, v0.92.2). @@warning_count and
	// SHOW WARNINGS are session-scoped; without connection affinity
	// the pool can hand us a different conn for the warning probe and
	// the refusal silently misses.
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: LOAD DATA: pin connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	stmt := buildLoadDataStmt(w.schema, table.Name, cols, name)
	_, execErr := conn.ExecContext(ctx, stmt)

	// Wait for the encoder so we don't leak a goroutine.
	serErr := <-encErr

	// Encoder error is the most informative; surface it first.
	if serErr != nil && !errors.Is(serErr, io.ErrClosedPipe) {
		return fmt.Errorf("mysql: LOAD DATA: serialize rows for %q: %w", table.Name, serErr)
	}
	if execErr != nil {
		return fmt.Errorf("mysql: LOAD DATA into %q: %w", table.Name, execErr)
	}

	// CRITICAL (Bugs 102/103 v0.92.2 root cause): LOAD DATA LOCAL
	// INFILE silently bypasses strict sql_mode for per-row type-
	// conversion errors. The session's @@sql_mode is strict (we
	// inject it in parseDSN), and a direct INSERT of an 80-digit
	// NUMERIC into DECIMAL(65,30) correctly errors 1264 — but the
	// same value through LOAD DATA with `(@var) SET col=@var`
	// indirection silently clamps to MAX and bumps @@warning_count.
	//
	// Empirically (probed against MySQL 8.0 with strict sql_mode):
	//   - Direct INSERT 80-digit → Error 1264, statement aborts
	//   - LOAD DATA same value → row inserted with MAX clamp;
	//     @@warning_count = 1; SHOW WARNINGS exposes the truncation
	//   - Same for TIMESTAMP out-of-range / zero-date
	//   - Explicit CAST(@var AS DECIMAL(65,30)) in SET still clamps
	//
	// The only reliable closure is the post-load warning probe.
	// Surface any warnings: a loud refusal under strict sql_mode, a loud
	// one-time WARN under `--mysql-sql-mode=''` (Vector B — the relaxed
	// path no longer skips silently; a silent clamp/truncation is still
	// reported, just not refused, since the operator opted into coercion).
	return w.reportBulkWriteWarnings(ctx, conn, table.Name)
}

// reportBulkWriteWarnings inspects the diagnostic-area warnings produced
// by the just-completed bulk write (LOAD DATA, or a batched INSERT) on the
// pinned conn, and either refuses loudly (strict sql_mode) or WARNs loudly
// once per table (relaxed sql_mode, --mysql-sql-mode=”) — closing the
// Vector B silent-clamp gap. With zero warnings it's a no-op.
//
// SHOW WARNINGS is read FIRST (before any other statement on the conn):
// reading `@@warning_count` first empties the diagnostic list, so the
// prior code's subsequent SHOW WARNINGS returned nothing and the refusal
// rendered an empty `Examples: []`. Reading SHOW WARNINGS directly gives
// both the count (row count) AND the detail.
//
// Strict sql_mode: LOAD DATA / non-strict-INSERT downgrade per-row
// type-conversion errors to warnings; a non-empty list is silent
// corruption sluice must refuse (Bugs 102/103 / v0.92.2).
//
// Relaxed sql_mode (`--mysql-sql-mode=”`): the operator opted into
// server-side coercion (e.g. to accept legacy zero-dates, now better
// handled read-side by --zero-date). That opt-in does NOT make a silent
// numeric clamp / string truncation acceptable — MySQL still flags it
// (verified: under sql_mode=” an out-of-range value clamps AND bumps the
// warning list). So instead of the pre-Vector-B silent skip, emit a loud
// one-time-per-table WARN naming the coercions + the data-preserving
// remedy. Not a refusal: the operator chose relaxed mode deliberately.
// readShowWarnings reads SHOW WARNINGS on conn (must be read before any
// other statement on the session, which would clear the diagnostic list),
// returning up to 8 formatted detail lines and the total warning count.
func readShowWarnings(ctx context.Context, conn *sql.Conn, table string) (details []string, count int, err error) {
	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return nil, 0, fmt.Errorf("mysql: bulk write into %q: SHOW WARNINGS failed: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var level, code, msg string
		if err := rows.Scan(&level, &code, &msg); err != nil {
			return nil, 0, fmt.Errorf("mysql: bulk write into %q: scan warning: %w", table, err)
		}
		count++
		// Cap at a few warnings — gigantic loads can emit thousands and
		// we just need enough for the operator to diagnose.
		if len(details) < 8 {
			details = append(details, fmt.Sprintf("%s %s: %s", level, code, msg))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("mysql: bulk write into %q: iterate warnings: %w", table, err)
	}
	return details, count, nil
}

func (w *RowWriter) reportBulkWriteWarnings(ctx context.Context, conn *sql.Conn, table string) error {
	details, count, err := readShowWarnings(ctx, conn, table)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	more := ""
	if count > 8 {
		more = fmt.Sprintf(" (… and %d more)", count-8)
	}

	if sessionSQLMode == "" {
		// Relaxed: WARN once per table, don't refuse.
		if _, seen := w.warnedClamp.LoadOrStore(table, struct{}{}); !seen {
			slog.WarnContext(
				ctx,
				"mysql: target SILENTLY coerced value(s) under --mysql-sql-mode='' — out-of-range/over-long "+
					"values were clamped or truncated on write, not refused (Vector B). The migration proceeds "+
					"with the coerced values per your relaxed sql_mode opt-in.",
				slog.String("table", table),
				slog.Int("warnings", count),
				slog.String("examples", strings.Join(details, "; ")+more),
				slog.String("hint", "to PRESERVE such values, map the column to a fitting type via "+
					"--type-override (e.g. =decimal(P,S) for a numeric overflow, =text/=varchar for an over-long "+
					"string, =datetime for an out-of-range timestamp) or fix the source; to REFUSE instead of "+
					"coerce, drop --mysql-sql-mode='' so strict mode rejects the value loudly"),
			)
		}
		return nil
	}

	return fmt.Errorf("mysql: bulk write into %q produced %d warning(s) under strict sql_mode — "+
		"LOAD DATA's per-row type-conversion errors are silently downgraded to warnings (a MySQL "+
		"behaviour quirk this refusal closes; Bugs 102/103 / v0.92.2). Examples: [%s]%s. "+
		"Recovery (data-preserving): map the column to a target type that FITS the value via "+
		"--type-override — e.g. `=datetime` for an out-of-range timestamp (MySQL DATETIME covers "+
		"1000–9999 vs TIMESTAMP's 1970–2038), `=decimal(P,S)` for a numeric overflow, "+
		"or `=text`/`=varchar` to keep the raw value — or fix the source data. "+
		"Do NOT pass --mysql-sql-mode='' to silence a range/overflow/truncation warning: under "+
		"non-strict sql_mode MySQL SILENTLY clamps or truncates the offending values (out-of-range "+
		"dates → 0000-00-00, over-long strings cut, numbers clamped) and the migration exits 0 with "+
		"corrupted data. --mysql-sql-mode='' is appropriate ONLY for accepting genuinely legacy "+
		"zero-date data as-is (see docs/operator/migrating-legacy-mysql.md)",
		table, count, strings.Join(details, "; "), more)
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
	// catalog Bug 75: ir.Bit is streamed as the canonical '0'/'1'
	// bit-string (see encodeRowsTSV). MySQL's BIT(N) column needs the
	// numeric value, so parse the base-2 digits. NULLIF keeps a NULL
	// field (\N → SQL NULL) NULL rather than CONV('')→0. CAST to
	// UNSIGNED so the assignment to BIT(N) is the integer value, not a
	// re-stringified one.
	if _, isBit := col.Type.(ir.Bit); isBit {
		return "CAST(CONV(NULLIF(" + varName + ", ''), 2, 10) AS UNSIGNED)"
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
				raw := row[c.Name]
				// catalog Bug 75: ir.Bit. The INSERT path wants ceil(N/8)
				// big-endian bytes (what prepareValue produces), but LOAD
				// DATA reinterprets a binary field as a string and
				// corrupts it. Instead emit the IR-canonical '0'/'1'
				// bit-string verbatim and let columnSetExpr's CONV(...,2,10)
				// SET expression parse it. NULL stays NULL.
				if _, isBit := c.Type.(ir.Bit); isBit && raw != nil {
					if s, ok := raw.(string); ok {
						buf = appendEscapedString(buf, s)
						continue
					}
				}
				v := prepareValue(raw, c)
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

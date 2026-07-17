// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PG→PG raw-copy passthrough (ADR-0078, roadmap item 3b(b)).
//
// The IR-first bulk-copy path decodes every source row into an ir.Row
// and re-encodes it via pgx.CopyFrom on the target — the price of
// engine-neutral generality. For a same-engine, no-transform PG→PG copy
// that price buys nothing: the bytes the source emits are exactly the
// bytes the target wants. This file implements the optional
// [ir.RawCopyExporter] / [ir.RawCopyImporter] / [ir.RawCopyVersionProber]
// surfaces that let the orchestrator byte-pipe
// `COPY (SELECT …) TO STDOUT` → `COPY tbl (…) FROM STDIN` directly,
// escaping database/sql via the SAME Conn.Raw → *pgx.Conn → *pgconn.PgConn
// path writeViaCopy already uses.
//
// The CRUX correctness invariant (see ADR-0078): the exporter projects
// EXACTLY the source-readable columns (generated columns excluded, the
// same projection [buildSelect] uses) via an explicit SELECT, NEVER a
// bare `COPY tbl TO STDOUT` (which would include generated columns and
// desync the importer's column list). The importer's column list comes
// from the SAME [nonGeneratedColumns] helper, so the two line up by
// construction. The orchestrator's value-fidelity gate guarantees there
// is no redaction / type-override / shard-injection to skip — this path
// is byte-for-byte faithful precisely because nothing is supposed to
// change.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"sluicesync.dev/sluice/internal/ir"
)

// ServerMajorVersion implements [ir.RawCopyVersionProber] on the
// reader. Returns the source server's major version (e.g. 16 for PG
// 16.x), derived from server_version_num (MMmmpp). Used by the
// orchestrator to gate binary raw-copy: binary is engaged only when the
// source and target majors match.
func (r *RowReader) ServerMajorVersion(ctx context.Context) (int, error) {
	// r.q is *sql.DB (migrate path) or *sql.Conn (snapshot path); both
	// expose QueryRowContext but the narrow `querier` interface doesn't,
	// so resolve through the row-querier shape.
	q, ok := r.q.(rawCopyRowQuerier)
	if !ok {
		return 0, fmt.Errorf("postgres: ServerMajorVersion: query source %T cannot query", r.q)
	}
	return rawCopyServerMajor(ctx, q)
}

// ServerMajorVersion implements [ir.RawCopyVersionProber] on the
// writer. Returns the target server's major version. See the reader's
// method for the gate semantics.
func (w *RowWriter) ServerMajorVersion(ctx context.Context) (int, error) {
	return rawCopyServerMajor(ctx, w.db)
}

// rawCopyRowQuerier is the single-row query shape both *sql.DB and
// *sql.Conn satisfy. The reader's narrow `querier` interface omits it.
type rawCopyRowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// rawCopyServerMajor reads server_version_num via q and returns the
// integer major component. server_version_num packs major/minor as
// MMmmpp (170002 = 17.2, 160006 = 16.6), so the major is value/10000.
func rawCopyServerMajor(ctx context.Context, q rawCopyRowQuerier) (int, error) {
	var n int
	if err := q.QueryRowContext(ctx, "SHOW server_version_num").Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres: raw-copy server version: %w", err)
	}
	return n / 10000, nil
}

// ExportRawCopy implements [ir.RawCopyExporter]. It runs
// `COPY (SELECT <readable cols> FROM <schema.table> [WHERE pk range])
// TO STDOUT [WITH (FORMAT …)]` and streams the server's native COPY
// bytes into w via *pgconn.PgConn.CopyTo — no per-value decode.
//
// The SELECT projection is [sourceReadableColumns] (generated columns
// excluded), identical to [buildSelect]; the optional chunk adds the
// (pk > $lo AND pk <= $hi) predicate the chunked IR path uses. On the
// snapshot-pinned reader (closer == nil) the pinned *sql.Conn runs the
// COPY inside the same exported-snapshot transaction.
func (r *RowReader) ExportRawCopy(ctx context.Context, table *ir.Table, chunk *ir.RawCopyChunk, format ir.RawCopyFormat, w io.Writer) error {
	if table == nil {
		return errors.New("postgres: ExportRawCopy: table is nil")
	}
	if w == nil {
		return errors.New("postgres: ExportRawCopy: writer is nil")
	}
	sqlStmt, err := buildRawCopyToStmt(r.schema, table, chunk, format)
	if err != nil {
		return err
	}

	exec := func(driverConn any) error {
		conn, perr := pgConnFromDriver(driverConn)
		if perr != nil {
			return perr
		}
		// Pin the wire encoding to UTF8 so the byte-pipe is self-consistent
		// regardless of either DSN's client_encoding (see rawCopyForceUTF8).
		if eerr := rawCopyForceUTF8(ctx, conn); eerr != nil {
			return fmt.Errorf("postgres: ExportRawCopy %q: %w", table.Name, eerr)
		}
		// Pin float text rendering to shortest-exact on every non-binary
		// format (see rawCopyPinFloatDigits — Bug 194); binary COPY carries
		// raw IEEE-754 send bytes, which the GUC never touches.
		if format != ir.RawCopyBinary {
			if eerr := rawCopyPinFloatDigits(ctx, conn); eerr != nil {
				return fmt.Errorf("postgres: ExportRawCopy %q: %w", table.Name, eerr)
			}
		}
		if _, cerr := conn.CopyTo(ctx, w, sqlStmt); cerr != nil {
			return fmt.Errorf("postgres: ExportRawCopy %q: COPY TO STDOUT: %w", table.Name, cerr)
		}
		return nil
	}
	return r.rawConn(ctx, exec)
}

// ImportRawCopy implements [ir.RawCopyImporter]. It runs
// `COPY <schema.table> (<non-generated cols>) FROM STDIN
// [WITH (FORMAT …)]` and feeds the byte stream r straight into the
// server via *pgconn.PgConn.CopyFrom, returning the server-reported row
// count.
//
// The column list is [nonGeneratedColumns] — the SAME helper the
// exporter's SELECT projection derives from — so the FROM-STDIN column
// list and the TO-STDOUT projection line up by construction (the CRUX
// invariant; see file header / ADR-0078).
func (w *RowWriter) ImportRawCopy(ctx context.Context, table *ir.Table, format ir.RawCopyFormat, r io.Reader) (int64, error) {
	if table == nil {
		return 0, errors.New("postgres: ImportRawCopy: table is nil")
	}
	if r == nil {
		return 0, errors.New("postgres: ImportRawCopy: reader is nil")
	}
	sqlStmt := buildRawCopyFromStmt(w.schema, table, format)

	sqlConn, err := w.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: ImportRawCopy: acquire conn: %w", err)
	}
	defer func() { _ = sqlConn.Close() }() // returns conn to pool

	var copied int64
	rawErr := sqlConn.Raw(func(driverConn any) error {
		conn, perr := pgConnFromDriver(driverConn)
		if perr != nil {
			return perr
		}
		// Match the exporter's pinned UTF8 wire encoding so the byte stream
		// the importer receives is decoded under the same encoding it was
		// emitted (see rawCopyForceUTF8).
		if eerr := rawCopyForceUTF8(ctx, conn); eerr != nil {
			return fmt.Errorf("postgres: ImportRawCopy %q: %w", table.Name, eerr)
		}
		tag, cerr := conn.CopyFrom(ctx, r, sqlStmt)
		if cerr != nil {
			return fmt.Errorf("postgres: ImportRawCopy %q: COPY FROM STDIN: %w", table.Name, cerr)
		}
		copied = tag.RowsAffected()
		return nil
	})
	if rawErr != nil {
		return 0, rawErr
	}
	return copied, nil
}

// rawConn escapes the reader's database/sql source to the underlying
// *pgconn.PgConn and invokes exec on it. The reader's query source is a
// *sql.DB in the simple/migrate path (closer == db) and a *sql.Conn in
// the snapshot path; both expose Raw, but *sql.DB does so only via a
// freshly-acquired *sql.Conn, so the DB path checks one out first.
func (r *RowReader) rawConn(ctx context.Context, exec func(driverConn any) error) error {
	switch q := r.q.(type) {
	case *sql.Conn:
		// Snapshot-pinned conn: run the COPY on the pinned connection so
		// it reads within the exported-snapshot transaction.
		return q.Raw(exec)
	case *sql.DB:
		conn, err := q.Conn(ctx)
		if err != nil {
			return fmt.Errorf("postgres: ExportRawCopy: acquire conn: %w", err)
		}
		defer func() { _ = conn.Close() }() // returns conn to pool
		return conn.Raw(exec)
	default:
		return fmt.Errorf("postgres: ExportRawCopy: query source %T does not support raw COPY", r.q)
	}
}

// rawCopyForceUTF8 pins the session's client_encoding to UTF8 on the raw
// connection before a COPY. The raw lane byte-pipes the source's
// COPY-TO-STDOUT stream straight into the target's COPY-FROM-STDIN: the
// bytes are encoded under the SOURCE session's client_encoding and decoded
// under the TARGET session's. If an operator sets client_encoding=LATIN1
// in one DSN and not the other, that asymmetry would silently corrupt
// non-ASCII text — the byte-pipe sees no Row, so the typed IR path's
// per-value re-encode (which would normalize this) never runs. Forcing
// both sessions to UTF8 makes the stream self-consistent by construction,
// regardless of either DSN — matching what the IR/pgx COPY path already
// gets from pgx's default UTF8 session. (ADR-0078 known-limitation note.)
func rawCopyForceUTF8(ctx context.Context, conn *pgconn.PgConn) error {
	if _, err := conn.Exec(ctx, "SET client_encoding TO 'UTF8'").ReadAll(); err != nil {
		return fmt.Errorf("pin client_encoding=UTF8: %w", err)
	}
	return nil
}

// rawCopyPinFloatDigits pins extra_float_digits=3 on the raw-copy EXPORT
// session before a text-format COPY TO (Bug 194 — CRITICAL silent loss).
// PG ≥ 12 renders float4/float8 shortest-exact ONLY when the session's
// extra_float_digits ≥ 1 (the modern compiled-in default); a
// server/database/role default below that reverts float8out/float4out to
// the legacy %.15g/%.6g renderings, which silently round any float
// needing more digits — and Supabase ships extra_float_digits=0
// SERVER-WIDE, so every text raw-copy off a default Supabase source
// corrupted in transit (π …2d18 → …2d11, float4 2^24 → 2^24-16; only
// DBL_MAX failed loudly, overflowing COPY FROM with 22003). 3 is the
// maximum and forces round-trip-exact digits on every supported server
// version, pre-12 included (where ≥1 alone is not enough).
//
// This MUST be a statement-level SET, never a DSN/startup parameter:
// poolers strip extra_float_digits from startup packets (Supavisor's
// ignore_startup_parameters includes it explicitly) but pass
// statement-level SETs through — verified live against Supabase's
// session pooler. The import side needs no pin (float8in/float4in parse
// any digit count exactly), and the binary format needs none on either
// side (float4send/float8send bytes; extra_float_digits is text-only).
// The typed IR lanes are also immune: pgx's extended protocol decodes
// float OIDs in binary format.
func rawCopyPinFloatDigits(ctx context.Context, conn *pgconn.PgConn) error {
	if _, err := conn.Exec(ctx, "SET extra_float_digits = 3").ReadAll(); err != nil {
		return fmt.Errorf("pin extra_float_digits=3: %w", err)
	}
	return nil
}

// pgConnFromDriver unwraps a database/sql driver connection to the
// underlying *pgconn.PgConn, the byte-pipe COPY primitive. Mirrors the
// stdlib.Conn → *pgx.Conn escape in [RowWriter.writeViaCopy].
func pgConnFromDriver(driverConn any) (*pgconn.PgConn, error) {
	stdlibConn, ok := driverConn.(*stdlib.Conn)
	if !ok {
		return nil, fmt.Errorf("postgres: raw COPY: expected *stdlib.Conn, got %T", driverConn)
	}
	conn := stdlibConn.Conn()
	return conn.PgConn(), nil
}

// buildRawCopyToStmt assembles the source-side COPY TO STDOUT statement
// for a table or PK-bounded chunk. The inner SELECT is the
// [sourceReadableColumns] projection (generated columns excluded),
// identical to [buildSelect]; a non-nil chunk appends the
// (pk > lower AND pk <= upper) predicate. Bounds are inlined as literals
// (not parameters) because COPY TO STDOUT does not accept bind
// parameters; v1 restricts chunk PKs to a single integer column
// (validated by the orchestrator gate), so the inlined literal is a
// bare integer and cannot carry an injection.
func buildRawCopyToStmt(schema string, table *ir.Table, chunk *ir.RawCopyChunk, format ir.RawCopyFormat) (string, error) {
	src := sourceReadableColumns(table.Columns)
	if len(src) == 0 {
		return "", fmt.Errorf("postgres: ExportRawCopy: table %q has no readable columns", table.Name)
	}
	cols := make([]string, len(src))
	for i, c := range src {
		cols[i] = quoteIdent(c.Name)
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)

	var sb strings.Builder
	sb.WriteString("COPY (SELECT ")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(tableRef)
	if chunk != nil {
		pred, err := rawCopyChunkPredicate(chunk)
		if err != nil {
			return "", err
		}
		if pred != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(pred)
		}
	}
	sb.WriteString(") TO STDOUT")
	sb.WriteString(rawCopyFormatClause(format))
	return sb.String(), nil
}

// buildRawCopyFromStmt assembles the target-side COPY FROM STDIN
// statement. The column list is [nonGeneratedColumns] — the SAME helper
// the exporter's SELECT projection derives from — so the two column
// lists line up (CRUX invariant). The table reference uses
// pgx.Identifier.Sanitize, matching [RowWriter.writeViaCopy].
func buildRawCopyFromStmt(schema string, table *ir.Table, format ir.RawCopyFormat) string {
	cols := nonGeneratedColumns(table.Columns)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteIdent(c.Name)
	}
	return fmt.Sprintf(
		"COPY %s (%s) FROM STDIN%s",
		pgx.Identifier{schema, table.Name}.Sanitize(),
		strings.Join(names, ", "),
		rawCopyFormatClause(format),
	)
}

// rawCopyChunkPredicate renders the (pk > lower AND pk <= upper) WHERE
// body for a chunk. nil bounds are open (chunk 0 has no lower; the last
// chunk has no upper). v1 supports a single integer PK only — the
// orchestrator gate guarantees that shape, so the bounds inline as bare
// integer literals.
func rawCopyChunkPredicate(chunk *ir.RawCopyChunk) (string, error) {
	if chunk.PKColumn == "" {
		return "", errors.New("postgres: ExportRawCopy: chunk has empty PK column")
	}
	col := quoteIdent(chunk.PKColumn)
	var parts []string
	if chunk.LowerPK != nil {
		lit, err := rawCopyIntLiteral(chunk.LowerPK)
		if err != nil {
			return "", err
		}
		parts = append(parts, col+" > "+lit)
	}
	if chunk.UpperPK != nil {
		lit, err := rawCopyIntLiteral(chunk.UpperPK)
		if err != nil {
			return "", err
		}
		parts = append(parts, col+" <= "+lit)
	}
	return strings.Join(parts, " AND "), nil
}

// rawCopyIntLiteral renders a chunk bound as a SQL integer literal. The
// orchestrator only ever produces integer bounds for the raw lane (the
// gate routes non-integer PKs to the IR path / single stream), so any
// other dynamic type here is a programming error, refused loudly rather
// than risking a malformed predicate.
func rawCopyIntLiteral(v any) (string, error) {
	switch n := v.(type) {
	case int:
		return fmt.Sprintf("%d", n), nil
	case int32:
		return fmt.Sprintf("%d", n), nil
	case int64:
		return fmt.Sprintf("%d", n), nil
	default:
		return "", fmt.Errorf("postgres: ExportRawCopy: chunk bound %v (%T) is not an integer", v, v)
	}
}

// rawCopyFormatClause renders the COPY WITH-format suffix. Text is the
// default; only binary needs an explicit clause, but both are written
// explicitly so the exporter and importer statements are unambiguous and
// symmetric.
func rawCopyFormatClause(format ir.RawCopyFormat) string {
	return " WITH (FORMAT " + format.String() + ")"
}

// Static interface assertions: the PG reader/writer satisfy the
// raw-copy passthrough surfaces.
var (
	_ ir.RawCopyExporter = (*RowReader)(nil)
	_ ir.RawCopyImporter = (*RowWriter)(nil)
)

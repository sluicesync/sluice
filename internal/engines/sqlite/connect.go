// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
)

// queryOnlyPragma is appended to every driver DSN so modernc.org/sqlite
// applies `PRAGMA query_only=ON` on each new connection. SQLite is a
// migrate SOURCE, so we open it read-only at the SQL level: query_only
// rejects any write to the database while still permitting the catalog
// reads (sqlite_master, PRAGMA *) and SELECTs the readers need. It works
// for plain-path DSNs too (unlike the `file:...?mode=ro` URI form, which
// is fiddly with Windows paths), so it is the portable read-only lever.
const queryOnlyPragma = "_pragma=query_only(1)"

// dsnDateEncodingParam is the sluice-internal source-DSN query key that
// overrides the process-global --sqlite-date-encoding PER SOURCE (ADR-0129),
// mirroring the MySQL engine's `zero_date` param (ADR-0127). It is NOT a
// modernc driver option, so [parseDSN] strips it from the driver DSN before
// it ever reaches the driver (an unknown query key would otherwise error or
// be silently ignored) — the SQLite analogue of the mysql engine's
// nativeSluiceParams strip (Bug 126).
const dsnDateEncodingParam = "sqlite_date_encoding"

// parseDSN normalises one of sluice's accepted SQLite DSN forms into the
// driver DSN modernc.org/sqlite expects, the bare filesystem path for
// display / error messages, and the per-source date encoding resolved from
// the `sqlite_date_encoding` query param (dateEncodingInherit when absent, so
// the reader defers to the process-global default). Accepted inputs:
//
//   - a bare path: "./app.db", "/data/app.db", `C:\data\app.db`
//   - a file URI:  "file:app.db", "file:/data/app.db?cache=shared"
//   - a sqlite URL: "sqlite:///data/app.db", "sqlite://./app.db"
//
// The query_only pragma is appended so the connection opens read-only.
// A "file:" input is passed through verbatim (modernc understands the
// SQLite URI form natively); the other forms are reduced to a plain
// path, which modernc opens directly.
func parseDSN(dsn string) (driverDSN, path string, enc dateEncoding, err error) {
	if strings.TrimSpace(dsn) == "" {
		return "", "", dateEncodingInherit, errors.New("sqlite: DSN is empty")
	}

	// Pull sluice's own sqlite_date_encoding param out of the query string
	// (ADR-0129) BEFORE any driver-DSN assembly, so it never reaches modernc.
	// Absent → dateEncodingInherit (defer to the process-global default);
	// present-but-invalid → loud refusal here, before a connection is opened.
	clean, encRaw, present := stripDateEncodingParam(dsn)
	enc = dateEncodingInherit
	if present {
		enc, err = parseDateEncoding(encRaw)
		if err != nil {
			return "", "", dateEncodingInherit,
				fmt.Errorf("sqlite: invalid %s DSN param %q (%w)", dsnDateEncodingParam, encRaw, err)
		}
	}

	var base string
	switch {
	case strings.HasPrefix(clean, "file:"):
		// Native SQLite URI form — keep it. The displayed path is the
		// portion after the scheme with any query string trimmed.
		base = clean
		path = trimQuery(strings.TrimPrefix(clean, "file:"))
	case strings.HasPrefix(clean, "sqlite://"):
		// sqlite://<path> — the scheme is sluice/convention sugar, not a
		// driver form. Strip it down to the bare path. "sqlite:///abs"
		// (three slashes) collapses to "/abs"; "sqlite://rel" to "rel".
		p := strings.TrimPrefix(clean, "sqlite://")
		base = trimQuery(p)
		path = base
	default:
		base = trimQuery(clean)
		path = base
	}

	if path == "" {
		return "", "", dateEncodingInherit, fmt.Errorf("sqlite: DSN %q has no file path", dsn)
	}

	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return base + sep + queryOnlyPragma, path, enc, nil
}

// stripDateEncodingParam removes sluice's [dsnDateEncodingParam] from a DSN's
// `?k=v&…` query string, returning the DSN without it, the raw encoding value
// (empty if the key is absent or valueless), and whether the key was present
// (so the caller can distinguish absent → inherit from present → parse). Any
// other query params are left untouched so the `file:` URI form's driver
// options (e.g. cache=shared) still reach modernc. The first '?' splits the
// path from the query, the same convention [trimQuery] uses.
func stripDateEncodingParam(dsn string) (clean, value string, present bool) {
	q := strings.IndexByte(dsn, '?')
	if q < 0 {
		return dsn, "", false
	}
	head, query := dsn[:q], dsn[q+1:]
	parts := strings.Split(query, "&")
	kept := parts[:0] // filter in place — kept index never outruns the read index
	for _, p := range parts {
		k, v, _ := strings.Cut(p, "=")
		if k == dsnDateEncodingParam {
			value, present = v, true
			continue
		}
		kept = append(kept, p)
	}
	if len(kept) == 0 {
		return head, value, present
	}
	return head + "?" + strings.Join(kept, "&"), value, present
}

// trimQuery returns s without any "?query" suffix.
func trimQuery(s string) string {
	if i := strings.IndexByte(s, '?'); i >= 0 {
		return s[:i]
	}
	return s
}

// openReadOnly opens a read-only *sql.DB against the SQLite source named by
// dsn and verifies it is reachable (PingContext). Returns the pool, the bare
// file path (for error messages — always the operator's original source, even
// when a dump was materialized), the per-source date encoding resolved from the
// DSN, and tempPath: the materialized temp DB the caller must os.Remove on
// Close (empty when the source was a real binary `.db`, so a `.db` source
// removes nothing). The caller owns the pool's lifecycle via Close.
//
// The source is sniffed by its SQLite magic header (ADR-0130): a binary `.db`
// opens exactly as before; anything else is treated as a SQL text dump and
// materialized in-process into a temp SQLite DB, which is then opened read-only
// via the same path. On any error after materialize the temp file is removed
// before returning, so a failed open never leaks one.
func openReadOnly(ctx context.Context, dsn string) (db *sql.DB, path string, enc dateEncoding, tempPath string, err error) {
	driverDSN, path, enc, err := parseDSN(dsn)
	if err != nil {
		return nil, "", dateEncodingInherit, "", err
	}

	isBinary, err := sniffSQLiteBinary(path)
	if err != nil {
		return nil, "", dateEncodingInherit, "", fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	if !isBinary {
		// A SQL text dump (e.g. `wrangler d1 export`): materialize it into a
		// temp DB and read THAT, keeping `path` pointed at the original dump for
		// error messages. The query_only pragma still applies on the temp pool.
		tempPath, err = materializeDump(ctx, path)
		if err != nil {
			return nil, "", dateEncodingInherit, "", err // already names the dump
		}
		driverDSN = tempPath + "?" + queryOnlyPragma
	}

	db, err = sql.Open("sqlite", driverDSN)
	if err != nil {
		cleanupTemp(tempPath)
		return nil, "", dateEncodingInherit, "", fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		cleanupTemp(tempPath)
		return nil, "", dateEncodingInherit, "", fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	return db, path, enc, tempPath, nil
}

// cleanupTemp removes a materialized temp DB, ignoring the empty (no-temp,
// real-`.db`) case. Errors are ignored: it runs on a failure path where the
// open error is the signal that matters.
func cleanupTemp(tempPath string) {
	if tempPath != "" {
		_ = os.Remove(tempPath)
	}
}

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

// parseDSN normalises one of sluice's accepted SQLite DSN forms into the
// driver DSN modernc.org/sqlite expects, plus the bare filesystem path
// for display / error messages. Accepted inputs:
//
//   - a bare path: "./app.db", "/data/app.db", `C:\data\app.db`
//   - a file URI:  "file:app.db", "file:/data/app.db?cache=shared"
//   - a sqlite URL: "sqlite:///data/app.db", "sqlite://./app.db"
//
// The query_only pragma is appended so the connection opens read-only.
// A "file:" input is passed through verbatim (modernc understands the
// SQLite URI form natively); the other forms are reduced to a plain
// path, which modernc opens directly.
func parseDSN(dsn string) (driverDSN, path string, err error) {
	if strings.TrimSpace(dsn) == "" {
		return "", "", errors.New("sqlite: DSN is empty")
	}

	var base string
	switch {
	case strings.HasPrefix(dsn, "file:"):
		// Native SQLite URI form — keep it. The displayed path is the
		// portion after the scheme with any query string trimmed.
		base = dsn
		path = trimQuery(strings.TrimPrefix(dsn, "file:"))
	case strings.HasPrefix(dsn, "sqlite://"):
		// sqlite://<path> — the scheme is sluice/convention sugar, not a
		// driver form. Strip it down to the bare path. "sqlite:///abs"
		// (three slashes) collapses to "/abs"; "sqlite://rel" to "rel".
		p := strings.TrimPrefix(dsn, "sqlite://")
		base = trimQuery(p)
		path = base
	default:
		base = trimQuery(dsn)
		path = base
	}

	if path == "" {
		return "", "", fmt.Errorf("sqlite: DSN %q has no file path", dsn)
	}

	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return base + sep + queryOnlyPragma, path, nil
}

// trimQuery returns s without any "?query" suffix.
func trimQuery(s string) string {
	if i := strings.IndexByte(s, '?'); i >= 0 {
		return s[:i]
	}
	return s
}

// openReadOnly opens a read-only *sql.DB against the SQLite file named
// by dsn and verifies it is reachable (PingContext). Returns the pool
// and the bare file path (for error messages). The caller owns the
// pool's lifecycle via Close.
func openReadOnly(ctx context.Context, dsn string) (*sql.DB, string, error) {
	driverDSN, path, err := parseDSN(dsn)
	if err != nil {
		return nil, "", err
	}
	db, err := sql.Open("sqlite", driverDSN)
	if err != nil {
		return nil, "", fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, "", fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	return db, path, nil
}

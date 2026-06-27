// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// sqliteMagic is the 16-byte header every binary SQLite database file begins
// with (https://www.sqlite.org/fileformat2.html §1.3). A resolved --source
// file that does NOT start with it is treated as a SQL TEXT dump (what
// `wrangler d1 export` and `sqlite3 .dump` emit) and materialized in-process
// (ADR-0130). Magic-header sniffing is reliable and independent of file
// extension: a binary `.db` named `.sql` still opens directly, and a `.sql`
// dump materializes.
const sqliteMagic = "SQLite format 3\x00"

// sniffSQLiteBinary reports whether the file at path begins with the SQLite
// binary magic header. A file shorter than the 16-byte header (e.g. a tiny
// dump) is not binary and is NOT an error. A genuine read error (missing or
// unreadable file) is returned so the caller can fail loudly before opening
// anything.
func sniffSQLiteBinary(path string) (bool, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied source path
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()

	hdr := make([]byte, len(sqliteMagic))
	n, err := io.ReadFull(f, hdr)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// Shorter than the magic → cannot be a binary DB; treat as a dump.
			return false, nil
		}
		return false, err
	}
	return n == len(sqliteMagic) && string(hdr) == sqliteMagic, nil
}

// materializeDump loads the SQL text dump at dumpPath into a FRESH temp SQLite
// database under os.TempDir() and returns the temp file's path. The caller owns
// the temp file's lifecycle (the reader removes it on Close).
//
// modernc.org/sqlite executes a multi-statement script in a single Exec (proven
// by the real-Cloudflare-D1 validation, ADR-0130), so the common path is one
// Exec of the whole dump. If that fails, it falls back to splitting the dump on
// statement boundaries and executing sequentially; if the split ALSO fails the
// ORIGINAL single-Exec error is surfaced (it is the truest description of a
// malformed dump). Any failure removes the temp file and returns a loud error
// naming the dump — no data moves on a malformed dump (the loud-failure
// posture, ADR-0130 §5).
func materializeDump(ctx context.Context, dumpPath string) (tempPath string, err error) {
	dump, err := os.ReadFile(dumpPath) //nolint:gosec // operator-supplied source path
	if err != nil {
		return "", fmt.Errorf("sqlite: read dump %q: %w", dumpPath, err)
	}

	f, err := os.CreateTemp("", "sluice-sqlite-*.db")
	if err != nil {
		return "", fmt.Errorf("sqlite: create temp db for dump %q: %w", dumpPath, err)
	}
	created := f.Name()
	tempPath = created
	_ = f.Close() // only the path is needed; modernc opens the file itself

	// From here any failure must remove the temp file and report no path, so a
	// caller that gets an error never has to clean up after us. `created` (not
	// the named return) is used because the error returns set tempPath="" before
	// this defer runs. The db.Close defer is registered AFTER this one so it
	// runs FIRST (LIFO) — the file handle is released before os.Remove, which
	// matters on Windows.
	defer func() {
		if err != nil {
			_ = os.Remove(created)
		}
	}()

	db, err := sql.Open("sqlite", tempPath)
	if err != nil {
		return "", fmt.Errorf("sqlite: open temp db for dump %q: %w", dumpPath, err)
	}
	defer func() { _ = db.Close() }()

	if err = execDump(ctx, db, string(dump)); err != nil {
		return "", fmt.Errorf("sqlite: materialize dump %q: %w", dumpPath, err)
	}
	return tempPath, nil
}

// execDump runs the dump as one multi-statement Exec, falling back to a
// statement-split sequential exec only if that fails. On a fallback that also
// fails, the original single-Exec error is returned.
func execDump(ctx context.Context, db *sql.DB, dump string) error {
	_, err := db.ExecContext(ctx, dump)
	if err == nil {
		return nil
	}
	// Single-Exec failed; some inputs (or a future modernc) may need
	// statement-by-statement execution. Try the split; if THAT also fails,
	// surface the original single-Exec error.
	if splitErr := execSplit(ctx, db, dump); splitErr != nil {
		return err
	}
	return nil
}

// execSplit splits dump into individual statements and executes them in order,
// stopping (and returning) on the first error.
func execSplit(ctx context.Context, db *sql.DB, dump string) error {
	for _, stmt := range splitSQLStatements(dump) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// splitSQLStatements breaks a SQL script into individual statements on
// top-level ';' boundaries, respecting single-quoted strings (with ” escapes),
// double-quoted identifiers, and `--` line / `/* */` block comments so a ';'
// inside any of them does not split a statement. Empty/whitespace-only
// fragments are dropped. Bytewise iteration is safe because every delimiter is
// ASCII and UTF-8 continuation bytes (>= 0x80) never collide with one. This is
// the best-effort fallback for the rare dump modernc won't run in one Exec.
func splitSQLStatements(script string) []string {
	var stmts []string
	start, n := 0, len(script)
	flush := func(end int) {
		if s := strings.TrimSpace(script[start:end]); s != "" {
			stmts = append(stmts, s)
		}
		start = end + 1
	}
	for i := 0; i < n; i++ {
		switch script[i] {
		case '\'':
			i++
			for i < n {
				if script[i] == '\'' {
					if i+1 < n && script[i+1] == '\'' {
						i++ // '' is an escaped quote, not a terminator
						i++
						continue
					}
					break
				}
				i++
			}
		case '"':
			i++
			for i < n && script[i] != '"' {
				i++
			}
		case '-':
			if i+1 < n && script[i+1] == '-' {
				for i < n && script[i] != '\n' {
					i++
				}
			}
		case '/':
			if i+1 < n && script[i+1] == '*' {
				i += 2
				for i < n {
					if script[i] == '*' && i+1 < n && script[i+1] == '/' {
						break
					}
					i++
				}
				i++ // skip the '*'; the loop's i++ skips the '/'
			}
		case ';':
			flush(i)
		}
	}
	flush(n)
	return stmts
}

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
// database and returns the temp file's path. The caller owns the temp file's
// lifecycle (the reader removes it on Close). stageDir is where the temp
// database is created (--stage-dir / SLUICE_STAGE_DIR — the materialized copy
// is roughly the database's size, which overwhelms a tmpfs /tmp on large
// dumps, the ADR-0145 hazard class); empty keeps the os.TempDir default, and
// a missing directory refuses loudly naming the flag (mirroring the flatfile
// staging path) — never a silent fallback to the system temp dir.
//
// The dump is STREAMED, never read whole into memory (a `sqlite3 .dump` of a
// large database is bigger than the database itself, so reading it into RAM
// would be catastrophic — tens of GB). [streamMaterializeDump] reads it in
// bounded blocks, splits complete statements off, and executes them in
// multi-statement batches on ONE pinned connection (so a `BEGIN TRANSACTION …
// COMMIT`-wrapped dump commits correctly). Any failure removes the temp file and
// returns a loud error naming the dump — no data moves on a malformed dump (the
// loud-failure posture, ADR-0130 §5).
func materializeDump(ctx context.Context, dumpPath, stageDir string) (tempPath string, err error) {
	src, err := os.Open(dumpPath) //nolint:gosec // operator-supplied source path
	if err != nil {
		return "", fmt.Errorf("sqlite: read dump %q: %w", dumpPath, err)
	}
	defer func() { _ = src.Close() }()

	f, err := os.CreateTemp(stageDir, "sluice-sqlite-*.db")
	if err != nil {
		if stageDir != "" {
			return "", fmt.Errorf("sqlite: create temp db for dump %q under --stage-dir %q: %w", dumpPath, stageDir, err)
		}
		return "", fmt.Errorf("sqlite: create temp db for dump %q: %w", dumpPath, err)
	}
	created := f.Name()
	tempPath = created
	_ = f.Close() // only the path is needed; modernc opens the file itself

	// From here any failure must remove the temp file and report no path, so a
	// caller that gets an error never has to clean up after us. `created` (not
	// the named return) is used because the error returns set tempPath="" before
	// this defer runs. The conn/db Close defers are registered AFTER this one so
	// they run FIRST (LIFO) — the file handle is released before os.Remove, which
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

	// Pin ONE underlying connection for the whole load. This is load-bearing:
	// a `sqlite3 .dump` wraps the script in a single `BEGIN TRANSACTION … COMMIT`,
	// so the statements must all run on the same connection for the transaction
	// to persist and commit (a pooled multi-connection exec — or, as some tools
	// do, a fresh process per chunk — would roll back the uncommitted prefix and
	// then fail with "no such table"). It also bounds memory: the dump is
	// STREAMED (never read whole into RAM).
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("sqlite: open temp db for dump %q: %w", dumpPath, err)
	}
	defer func() { _ = conn.Close() }()

	if err = streamMaterializeDump(ctx, conn, src, dumpReadBlockBytes); err != nil {
		return "", fmt.Errorf("sqlite: materialize dump %q: %w", dumpPath, err)
	}
	return tempPath, nil
}

const (
	// dumpReadBlockBytes is how much of the dump is read per streamed block.
	dumpReadBlockBytes = 1 << 20 // 1 MiB
	// dumpExecBatchBytes is the accumulated-statement size at which a batch is
	// executed (one ExecContext of a multi-statement script — what modernc runs
	// natively). Bounds memory while amortising the per-Exec round-trip.
	dumpExecBatchBytes = 8 << 20 // 8 MiB
)

// streamMaterializeDump loads a SQL dump from r into the SQLite connection conn,
// statement by statement, never holding more than ~one block + one partial
// statement + one batch in memory. It reads fixed-size blocks, splits them into
// complete top-level statements ([splitDumpChunk], which carries an unterminated
// trailing fragment to the next block so a statement, string, or comment may
// span block boundaries), accumulates them into a batch, and executes each batch
// on conn. All statements share conn, so a `BEGIN TRANSACTION … COMMIT`-wrapped
// dump commits correctly. blockSize is parameterised for tests; <= 0 uses the
// default.
func streamMaterializeDump(ctx context.Context, conn *sql.Conn, r io.Reader, blockSize int) error {
	if blockSize <= 0 {
		blockSize = dumpReadBlockBytes
	}
	block := make([]byte, blockSize)
	var (
		carry string
		batch strings.Builder
	)
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		if _, err := conn.ExecContext(ctx, batch.String()); err != nil {
			return err
		}
		batch.Reset()
		return nil
	}
	for {
		nr, rerr := r.Read(block)
		if nr > 0 {
			stmts, rest := splitDumpChunk(carry + string(block[:nr]))
			carry = rest
			for _, s := range stmts {
				batch.WriteString(s)
				batch.WriteString(";\n")
				if batch.Len() >= dumpExecBatchBytes {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return rerr
		}
	}
	// The final fragment after the last top-level ';' (e.g. a trailing statement
	// with no terminator). An unterminated statement here is malformed and fails
	// loudly at Exec — exactly the pre-existing posture.
	if rest := strings.TrimSpace(carry); rest != "" {
		batch.WriteString(rest)
		batch.WriteString(";\n")
	}
	return flush()
}

// splitDumpChunk breaks a SQL script into complete top-level statements and a
// trailing CARRY — the fragment after the last top-level ';' (which may be an
// unterminated statement, string, or comment that the next block completes). It
// respects single-quoted strings (with ” escapes), double-quoted identifiers,
// and `--` line / `/* */` block comments so a ';' inside any of them does not
// split. The carry is re-prepended and re-scanned with the next block, so a
// token straddling a block boundary is always resolved correctly with NO
// persistent scan state: a two-char delimiter (`”`, `--`, `/*`, `*/`) whose
// first byte is the chunk's last byte is simply left in the carry — nothing can
// follow it in the chunk, so no statement is ever wrongly emitted. Bytewise
// iteration is safe: every delimiter is ASCII and UTF-8 continuation bytes
// (>= 0x80) never collide with one.
func splitDumpChunk(script string) (stmts []string, carry string) {
	start, n := 0, len(script)
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
			if s := strings.TrimSpace(script[start:i]); s != "" {
				stmts = append(stmts, s)
			}
			start = i + 1
		}
	}
	return stmts, script[start:]
}

// splitSQLStatements splits a COMPLETE SQL script into individual statements
// (the whole-script convenience over [splitDumpChunk]: the trailing fragment is
// the final statement). Empty/whitespace-only fragments are dropped.
func splitSQLStatements(script string) []string {
	stmts, carry := splitDumpChunk(script)
	if s := strings.TrimSpace(carry); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

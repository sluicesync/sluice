// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package dumpsig detects well-known file-format signatures on operator-
// supplied flat-file sources and builds the loud, recipe-bearing refusals
// for the formats sluice deliberately does not parse (roadmap item 55
// Phase 3, ADR-0163).
//
// The IR-first tenet forbids grammar over full-dialect SQL dumps, and
// pg_dump's custom format is explicitly private — so a plain mysqldump /
// pg_dump `.sql` file or a `PGDMP` archive handed to any file-reading
// source driver must be refused AT OPEN, naming the scratch-server-replay
// recipe, never half-parsed into a confusing mid-stream error. The same
// sniff also powers the cross-driver misuse refusals (a mydumper directory
// handed to the csv driver, a CSV handed to the mydumper driver, …), which
// name the RIGHT driver instead of a generic failure.
//
// Detection is by content signature (magic bytes / first-line markers),
// with filename extensions used ONLY as a secondary hint for the
// wrong-driver messages — never to decide how bytes are parsed.
package dumpsig

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Kind classifies a file head by content signature.
type Kind int

// Recognised signatures. KindUnknown means "none of the below" — the file
// may still be anything (including a perfectly good CSV).
const (
	KindUnknown Kind = iota

	// KindMySQLDumpSQL is a plain mysqldump SQL dump: the first line is
	// `-- MySQL dump <version>...`.
	KindMySQLDumpSQL

	// KindPGDumpSQL is a plain pg_dump SQL dump: the leading comment block
	// contains `-- PostgreSQL database dump`.
	KindPGDumpSQL

	// KindPGDumpCustom is a pg_dump custom-format archive (`pg_dump -Fc`),
	// which begins with the `PGDMP` magic.
	KindPGDumpCustom

	// KindSQLiteDB is a binary SQLite database file (`SQLite format 3\x00`).
	KindSQLiteDB

	// KindGzip / KindZstd are compressed containers (magic 1F 8B / 28 B5 2F FD).
	KindGzip
	KindZstd

	// KindUTF16 is a UTF-16/UTF-32 byte-order mark — a text file sluice's
	// UTF-8-only readers must not byte-interpret.
	KindUTF16
)

// headBytes is how much of the file Detect reads. Every signature above
// lands well inside it: the magics are at offset 0 and the pg_dump text
// marker sits in the first few comment lines.
const headBytes = 512

// Detect reads the head of the file at path and classifies it. A file
// shorter than every signature classifies as KindUnknown. Read errors are
// returned so the caller can fail loudly on an unreadable source.
func Detect(path string) (Kind, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied source path
	if err != nil {
		return KindUnknown, err
	}
	defer func() { _ = f.Close() }()

	head := make([]byte, headBytes)
	n, err := io.ReadFull(f, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return KindUnknown, err
	}
	return DetectHead(head[:n]), nil
}

// DetectHead classifies an already-read file head (Detect's pure core).
func DetectHead(head []byte) Kind {
	switch {
	case bytes.HasPrefix(head, []byte("PGDMP")):
		return KindPGDumpCustom
	case bytes.HasPrefix(head, []byte("SQLite format 3\x00")):
		return KindSQLiteDB
	case bytes.HasPrefix(head, []byte{0x1f, 0x8b}):
		return KindGzip
	case bytes.HasPrefix(head, []byte{0x28, 0xb5, 0x2f, 0xfd}):
		return KindZstd
	case bytes.HasPrefix(head, []byte{0xff, 0xfe}), bytes.HasPrefix(head, []byte{0xfe, 0xff}),
		bytes.HasPrefix(head, []byte{0x00, 0x00, 0xfe, 0xff}):
		// FF FE also covers UTF-32LE (FF FE 00 00). Refusing on the BOM is
		// enough — the remedy (transcode to UTF-8) is identical.
		return KindUTF16
	case bytes.HasPrefix(head, []byte("-- MySQL dump")):
		return KindMySQLDumpSQL
	case pgDumpTextMarker(head):
		return KindPGDumpSQL
	}
	return KindUnknown
}

// pgDumpTextMarker reports whether the head's leading comment block carries
// pg_dump's plain-text banner. The banner is `--\n-- PostgreSQL database
// dump\n--`, so the marker is matched at a LINE START within the head (an
// arbitrary later occurrence in, say, a CSV field would not start a line
// this early, and a refusal here is loud, not silent).
func pgDumpTextMarker(head []byte) bool {
	const marker = "-- PostgreSQL database dump"
	if bytes.HasPrefix(head, []byte(marker)) {
		return true
	}
	return bytes.Contains(head, []byte("\n"+marker))
}

// RefuseForeignDump returns the recipe-bearing coded refusal for a foreign
// dump kind, or nil when kind is not a foreign-dump signature. driver is
// the refusing engine's name (message prefix).
func RefuseForeignDump(driver, path string, kind Kind) error {
	var recipe string
	switch kind {
	case KindMySQLDumpSQL:
		recipe = fmt.Sprintf("%q is a plain mysqldump SQL dump — sluice deliberately does not parse "+
			"full-dialect SQL dumps (the IR-first tenet; docs/research/flat-file-sources.md). "+
			"Restore it to a scratch MySQL server and migrate live:\n"+
			"  docker run -d --name sluice-scratch -e MYSQL_ROOT_PASSWORD=scratch -p 33306:3306 mysql:8\n"+
			"  mysql -h127.0.0.1 -P33306 -uroot -pscratch <db> < %s\n"+
			"  sluice migrate --source-driver mysql --source 'root:scratch@tcp(127.0.0.1:33306)/<db>' ...\n"+
			"(a mydumper/pscale-dump DIRECTORY needs no scratch server: --source-driver mydumper)",
			path, filepath.Base(path))
	case KindPGDumpSQL:
		recipe = fmt.Sprintf("%q is a plain pg_dump SQL dump — sluice deliberately does not parse "+
			"full-dialect SQL dumps (the IR-first tenet; docs/research/flat-file-sources.md). "+
			"Restore it to a scratch PostgreSQL server and migrate live:\n"+
			"  docker run -d --name sluice-scratch -e POSTGRES_PASSWORD=scratch -p 55432:5432 postgres:16\n"+
			"  psql 'postgres://postgres:scratch@127.0.0.1:55432/postgres' -f %s\n"+
			"  sluice migrate --source-driver postgres --source 'postgres://postgres:scratch@127.0.0.1:55432/postgres' ...",
			path, filepath.Base(path))
	case KindPGDumpCustom:
		recipe = fmt.Sprintf("%q is a pg_dump custom-format archive (PGDMP) — an explicitly private "+
			"format sluice deliberately does not parse (docs/research/flat-file-sources.md). "+
			"Restore it to a scratch PostgreSQL server with pg_restore and migrate live:\n"+
			"  docker run -d --name sluice-scratch -e POSTGRES_PASSWORD=scratch -p 55432:5432 postgres:16\n"+
			"  pg_restore -d 'postgres://postgres:scratch@127.0.0.1:55432/postgres' %s\n"+
			"  sluice migrate --source-driver postgres --source 'postgres://postgres:scratch@127.0.0.1:55432/postgres' ...",
			path, filepath.Base(path))
	default:
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeSourceForeignDump,
		"restore the dump to a scratch server with its native tool, then migrate live",
		fmt.Errorf("%s: %s", driver, recipe))
}

// RefuseWrongDriver returns the coded cross-driver misuse refusal: the
// input was RECOGNISED, but this driver does not read it; remedy names the
// right driver (or preparation step).
func RefuseWrongDriver(driver, hint string, err error) error {
	return sluicecode.Wrap(sluicecode.CodeSourceWrongDriver, hint,
		fmt.Errorf("%s: %s", driver, err.Error()))
}

// RefuseRecognised maps a detected non-foreign kind to its wrong-driver
// refusal for a driver that reads plain UTF-8 flat files (csv/tsv/ndjson,
// and the sqlite dump-ingest path via sqliteIsSelf). Returns nil for
// KindUnknown (nothing recognised — the caller proceeds to parse).
func RefuseRecognised(driver, path string, kind Kind, sqliteIsSelf bool) error {
	if fd := RefuseForeignDump(driver, path, kind); fd != nil {
		return fd
	}
	switch kind {
	case KindSQLiteDB:
		if sqliteIsSelf {
			return nil
		}
		return RefuseWrongDriver(driver, "use --source-driver sqlite",
			fmt.Errorf("%q is a binary SQLite database — use --source-driver sqlite", path))
	case KindGzip, KindZstd:
		comp := "gzip"
		if kind == KindZstd {
			comp = "zstd"
		}
		return RefuseWrongDriver(driver, "decompress the file first",
			fmt.Errorf("%q is %s-compressed — decompress it first (this driver reads plain files only)", path, comp))
	case KindUTF16:
		return RefuseWrongDriver(driver, "transcode the file to UTF-8 first",
			fmt.Errorf("%q carries a UTF-16/UTF-32 byte-order mark — sluice reads UTF-8 only; transcode it first "+
				"(e.g. `iconv -f UTF-16 -t UTF-8`, or PowerShell `Get-Content ... | Set-Content -Encoding utf8`)", path))
	}
	return nil
}

// LooksLikeMydumperDir reports whether dir has the mydumper output shape —
// a `metadata` file plus at least one `*-schema.sql[.gz|.zst]` — used to
// point a misdirected csv/sqlite open at --source-driver mydumper. A read
// error reports false (the caller's own directory refusal stands).
func LooksLikeMydumperDir(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	var haveMetadata, haveSchema bool
	for _, e := range entries {
		name := e.Name()
		if name == "metadata" {
			haveMetadata = true
		}
		base := strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".zst")
		if strings.HasSuffix(base, "-schema.sql") {
			haveSchema = true
		}
	}
	return haveMetadata && haveSchema
}

// FlatFileExtDriver maps a filename extension to the flat-file source
// driver that reads it, for wrong-driver hints. ok is false for
// unrecognised extensions.
func FlatFileExtDriver(path string) (driver string, ok bool) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		return "csv", true
	case ".tsv":
		return "tsv", true
	case ".ndjson", ".jsonl":
		return "ndjson", true
	}
	return "", false
}

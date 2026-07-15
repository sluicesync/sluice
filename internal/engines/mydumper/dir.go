// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// dumpDir is the validated view of a mydumper output directory: the single
// dumped database, each table's schema file and ordered data chunks, and
// the metadata file's source position. Built once at Open* by
// [openDumpDir]; readers only ever consume this — they never re-scan the
// directory.
type dumpDir struct {
	path     string
	database string

	// tables maps table name → its files. tableOrder is the sorted name
	// list so iteration is deterministic.
	tables     map[string]*tableFiles
	tableOrder []string

	// binlogFile / binlogPos / gtidSet are the source position recorded in
	// the metadata file, when present (either the traditional
	// `SHOW MASTER STATUS` block or the ini `[master]`/`[source]` section).
	// Surfaced at INFO by [dumpDir.logSourcePosition]; the future dump→CDC
	// handoff hook (ADR-0161 §8 — recorded, not built).
	binlogFile string
	binlogPos  string
	gtidSet    string
}

// tableFiles is one table's slice of the dump: its schema file and its
// data chunks in chunk-number order. Either list entry may carry a
// .gz/.zst compression suffix; [openDumpFile] decompresses transparently.
type tableFiles struct {
	name       string
	schemaFile string   // absolute path; always present
	chunks     []string // absolute paths, sorted by chunk number; may be empty
}

// compression suffixes mydumper appends to every output file when
// --compress is in effect. Stripped for classification; honoured by
// [openDumpFile].
var compressionSuffixes = []string{".gz", ".zst"}

// stripCompressionSuffix removes a trailing .gz/.zst, reporting whether one
// was present.
func stripCompressionSuffix(name string) (string, bool) {
	for _, suf := range compressionSuffixes {
		if strings.HasSuffix(name, suf) {
			return strings.TrimSuffix(name, suf), true
		}
	}
	return name, false
}

// auxiliarySchemaSuffixes are the schema-only companion files mydumper can
// emit alongside the per-table schema files. They define views, triggers,
// routines/events, and sequences — schema objects with no row data — so
// they are SKIPPED with a WARN naming each (the operator can carry them by
// hand); silently ignoring them would hide that the target lacks those
// objects, and refusing would block every dump taken with --routines et al.
var auxiliarySchemaSuffixes = []string{
	"-schema-create.sql",
	"-schema-post.sql",
	"-schema-triggers.sql",
	"-schema-view.sql",
	"-schema-sequence.sql",
}

// openDumpDir scans path and validates the mydumper layout: a `metadata`
// file plus at least one `<db>.<table>-schema.sql`. Every file in the
// directory must be attributable — a recognised metadata/schema/chunk/
// auxiliary file — or the whole open is refused loudly naming the stranger
// (this reader never guesses at unknown files). Exactly one database
// prefix is supported; a multi-database dump is refused naming the
// databases (Phase 1 scope, ADR-0161 §2).
func openDumpDir(path string) (*dumpDir, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("mydumper: source path is empty (expected a mydumper output directory)")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("mydumper: open source %q: %w", path, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("mydumper: source %q is not a directory (the mydumper engine reads a "+
			"dump DIRECTORY; for a single-file SQL dump see docs/research/flat-file-sources.md)", path)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("mydumper: read source directory %q: %w", path, err)
	}

	d := &dumpDir{path: path, tables: map[string]*tableFiles{}}
	var (
		haveMetadata bool
		strangers    []string
		databases    = map[string]bool{}
	)
	table := func(db, name string) *tableFiles {
		databases[db] = true
		t := d.tables[name]
		if t == nil {
			t = &tableFiles{name: name}
			d.tables[name] = t
		}
		return t
	}

	type numberedChunk struct {
		table string
		path  string
		num   string
	}
	var chunks []numberedChunk

	for _, e := range entries {
		name := e.Name()
		full := filepath.Join(path, name)
		if e.IsDir() {
			return nil, fmt.Errorf("mydumper: unexpected subdirectory %q in dump directory %q", name, path)
		}

		// The dump-wide metadata file. mydumper writes `metadata` when the
		// dump completes and `metadata.partial`(.N) while in flight; a
		// partial marker means the dump may be incomplete, which is a
		// refusal — migrating a torn dump would silently miss rows.
		if name == "metadata" {
			haveMetadata = true
			continue
		}
		if strings.HasPrefix(name, "metadata.partial") {
			return nil, fmt.Errorf("mydumper: %q contains %q — the dump did not complete (mydumper renames "+
				"it to `metadata` on success); refusing to read a possibly-torn dump", path, name)
		}

		base, _ := stripCompressionSuffix(name)

		// Schema-only auxiliary files: skip with a WARN naming each.
		if suf, ok := matchAuxiliarySuffix(base); ok {
			slog.Warn("mydumper: skipping schema-only auxiliary file (views/triggers/routines are not "+
				"carried by the flat-file reader; apply them by hand if needed)",
				slog.String("file", name), slog.String("kind", strings.TrimSuffix(strings.TrimPrefix(suf, "-"), ".sql")))
			continue
		}

		// Per-table checksum/row-count companions (`<db>.<table>-metadata`,
		// `<db>.<table>-checksum`): informational only.
		if db, tbl, ok := splitDumpName(base, "-metadata"); ok {
			_, _ = db, tbl
			continue
		}
		if db, tbl, ok := splitDumpName(base, "-checksum"); ok {
			_, _ = db, tbl
			continue
		}

		// Per-table schema file: `<db>.<table>-schema.sql`.
		if db, tbl, ok := splitDumpName(base, "-schema.sql"); ok {
			t := table(db, tbl)
			if t.schemaFile != "" {
				return nil, fmt.Errorf("mydumper: table %q has two schema files (%q and %q)",
					tbl, filepath.Base(t.schemaFile), name)
			}
			t.schemaFile = full
			continue
		}

		// Data chunk: `<db>.<table>.<NNNNN>.sql`.
		if db, tbl, num, ok := splitChunkName(base); ok {
			databases[db] = true
			chunks = append(chunks, numberedChunk{table: tbl, path: full, num: num})
			continue
		}

		strangers = append(strangers, name)
	}

	if !haveMetadata {
		return nil, fmt.Errorf("mydumper: %q has no `metadata` file — not a mydumper/pscale-dump "+
			"output directory", path)
	}
	if len(d.tables) == 0 {
		return nil, fmt.Errorf("mydumper: %q has no `<db>.<table>-schema.sql` files — not a "+
			"mydumper/pscale-dump output directory", path)
	}
	if len(strangers) > 0 {
		sort.Strings(strangers)
		return nil, fmt.Errorf("mydumper: %q contains files this reader does not recognise: %s "+
			"(refusing rather than guessing; remove them or dump into a clean directory)",
			path, strings.Join(strangers, ", "))
	}
	if len(databases) > 1 {
		names := make([]string, 0, len(databases))
		for db := range databases {
			names = append(names, db)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("mydumper: %q contains dumps of %d databases (%s); the mydumper engine "+
			"reads exactly one — dump a single database per directory (mydumper -B <db>)",
			path, len(names), strings.Join(names, ", "))
	}
	for db := range databases {
		d.database = db
	}

	// Attach chunks to their tables, refusing a chunk whose table has no
	// schema file (rows with no column types would decode by guesswork).
	for _, c := range chunks {
		t := d.tables[c.table]
		if t == nil || t.schemaFile == "" {
			return nil, fmt.Errorf("mydumper: data chunk %q has no matching %s.%s-schema.sql",
				filepath.Base(c.path), d.database, c.table)
		}
		t.chunks = append(t.chunks, c.path)
	}
	for _, t := range d.tables {
		sortChunks(t.chunks)
		d.tableOrder = append(d.tableOrder, t.name)
	}
	sort.Strings(d.tableOrder)

	if err := d.parseMetadata(); err != nil {
		return nil, err
	}
	return d, nil
}

// matchAuxiliarySuffix reports whether base names a schema-only auxiliary
// file, returning the matched suffix.
func matchAuxiliarySuffix(base string) (string, bool) {
	for _, suf := range auxiliarySchemaSuffixes {
		if strings.HasSuffix(base, suf) {
			return suf, true
		}
	}
	return "", false
}

// splitDumpName splits `<db>.<table><suffix>` into its db and table parts.
// The FIRST dot separates database from table (a table name containing a
// dot is ambiguous in mydumper's own filename scheme; documented in
// ADR-0161 §2).
func splitDumpName(base, suffix string) (db, tbl string, ok bool) {
	if !strings.HasSuffix(base, suffix) {
		return "", "", false
	}
	stem := strings.TrimSuffix(base, suffix)
	db, tbl, found := strings.Cut(stem, ".")
	if !found || db == "" || tbl == "" {
		return "", "", false
	}
	return db, tbl, true
}

// splitChunkName splits a data-chunk filename `<db>.<table>.<NNNNN>.sql`
// into db, table, and the chunk-number text. The number is the LAST
// dot-segment before `.sql` and must be all digits.
func splitChunkName(base string) (db, tbl, num string, ok bool) {
	stem, found := strings.CutSuffix(base, ".sql")
	if !found {
		return "", "", "", false
	}
	lastDot := strings.LastIndexByte(stem, '.')
	if lastDot < 0 {
		return "", "", "", false
	}
	num = stem[lastDot+1:]
	if num == "" || !allDigits(num) {
		return "", "", "", false
	}
	db, tbl, found = strings.Cut(stem[:lastDot], ".")
	if !found || db == "" || tbl == "" {
		return "", "", "", false
	}
	return db, tbl, num, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// sortChunks orders a table's chunk paths by their numeric chunk id. The
// zero-padded `<NNNNN>` segment makes a plain string sort correct for
// same-width ids, but mydumper widens the field past 99999 chunks, so the
// sort compares by numeric value (length-then-lexicographic on the digit
// string — exact for arbitrary magnitudes without integer overflow).
func sortChunks(chunks []string) {
	key := func(p string) string {
		base, _ := stripCompressionSuffix(filepath.Base(p))
		_, _, num, ok := splitChunkName(base)
		if !ok {
			return base // unreachable post-validation; stable fallback
		}
		return num
	}
	sort.Slice(chunks, func(i, j int) bool {
		a, b := key(chunks[i]), key(chunks[j])
		a, b = strings.TrimLeft(a, "0"), strings.TrimLeft(b, "0")
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return a < b
	})
}

// openDumpFile opens a dump file for streaming, transparently decompressing
// by suffix. The returned ReadCloser owns the underlying file.
func openDumpFile(path string) (io.ReadCloser, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied dump directory contents
	if err != nil {
		return nil, fmt.Errorf("mydumper: open %q: %w", path, err)
	}
	switch {
	case strings.HasSuffix(path, ".gz"):
		zr, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("mydumper: open gzip %q: %w", path, err)
		}
		return &wrappedReadCloser{Reader: zr, closers: []io.Closer{zr, f}}, nil
	case strings.HasSuffix(path, ".zst"):
		zr, err := zstd.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("mydumper: open zstd %q: %w", path, err)
		}
		return &wrappedReadCloser{Reader: zr.IOReadCloser(), closers: []io.Closer{zr.IOReadCloser(), f}}, nil
	default:
		return f, nil
	}
}

// wrappedReadCloser closes a decompressor and its underlying file in order.
type wrappedReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (w *wrappedReadCloser) Close() error {
	var first error
	for _, c := range w.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// metadataMaxBytes bounds the metadata read: the file is a few hundred
// bytes of bookkeeping; a multi-megabyte "metadata" is not a mydumper dump.
const metadataMaxBytes = 1 << 20

// parseMetadata extracts the source binlog position / GTID set from the
// dump-wide metadata file. Both historical shapes are recognised:
//
//   - traditional (mydumper ≤0.11, pscale dump):
//     `SHOW MASTER STATUS:` followed by indented `Log:` / `Pos:` / `GTID:`
//   - ini (mydumper ≥0.12): a `[master]` / `[source]` section with
//     `File = …` / `Position = …` / `Executed_Gtid_Set = …`
//
// Parsing is deliberately LENIENT — the position is informational (logged
// at INFO; the ADR-0161 §8 handoff hook), so a metadata file with neither
// shape parses to empty fields rather than failing the open.
func (d *dumpDir) parseMetadata() error {
	path := filepath.Join(d.path, "metadata")
	f, err := os.Open(path) //nolint:gosec // operator-supplied dump directory contents
	if err != nil {
		return fmt.Errorf("mydumper: open metadata %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, metadataMaxBytes))
	if err != nil {
		return fmt.Errorf("mydumper: read metadata %q: %w", path, err)
	}

	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			// mydumper ≥1.0 comments the [source] coordinates out when it
			// judges them non-authoritative (`# SOURCE_LOG_FILE = …` under
			// --trx-tables). A commented position is deliberately NOT
			// surfaced as one.
			continue
		}
		key, val, found := strings.Cut(line, ":")
		if found {
			switch strings.TrimSpace(key) {
			case "Log":
				d.binlogFile = metadataValue(val)
				continue
			case "Pos":
				d.binlogPos = metadataValue(val)
				continue
			case "GTID":
				d.gtidSet = metadataValue(val)
				continue
			}
		}
		key, val, found = strings.Cut(line, "=")
		if found {
			switch strings.TrimSpace(key) {
			case "File", "SOURCE_LOG_FILE":
				d.binlogFile = metadataValue(val)
			case "Position", "SOURCE_LOG_POS":
				d.binlogPos = metadataValue(val)
			case "Executed_Gtid_Set", "Executed_GTID_Set":
				d.gtidSet = metadataValue(val)
			}
		}
	}
	return nil
}

// metadataValue trims whitespace and the optional quoting mydumper's ini
// shapes wrap values in.
func metadataValue(v string) string {
	return strings.Trim(strings.TrimSpace(v), `"'`)
}

// logSourcePosition surfaces the dump's recorded binlog position / GTID at
// INFO, once per open, so an operator can line the migrated data up with a
// later CDC start by hand. This is the recorded-not-built dump→CDC handoff
// hook (ADR-0161 §8).
func (d *dumpDir) logSourcePosition() {
	if d.binlogFile == "" && d.binlogPos == "" && d.gtidSet == "" {
		slog.Info("mydumper: metadata file records no binlog position", slog.String("dir", d.path))
		return
	}
	slog.Info(
		"mydumper: dump source position (usable to start replication/CDC after the copy)",
		slog.String("dir", d.path),
		slog.String("binlog_file", d.binlogFile),
		slog.String("binlog_pos", d.binlogPos),
		slog.String("gtid_set", d.gtidSet),
	)
}

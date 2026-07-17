// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	// klauspost's gzip is a drop-in stdlib replacement measured ~1.5×
	// faster decompressing dump-shaped chunks (BenchmarkChunkGzipDecompress;
	// audit 2026-07-15 M3.3), and the module already depends on
	// klauspost/compress for zstd.
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"sluicesync.dev/sluice/internal/engines/internal/dumpsig"
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

	// metadataRows is the dump's OWN recorded row count for this table,
	// when it recorded one: the ini `rows =` entry in the dump-wide
	// metadata file (mydumper ≥0.12; ground-truthed exact against
	// v1.0.3) or, failing that, a bare-integer per-table `-metadata`
	// companion (older mydumper; pscale-dump writes the companion but
	// leaves it EMPTY, so absence is normal). Consumed by
	// [dumpDir.warnIfRowCountMismatch] as a post-stream tripwire.
	metadataRows    int64
	hasMetadataRows bool
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
		// Cross-driver misuse refusals (roadmap item 55 Phase 3, ADR-0163):
		// classify the file before failing generically, so a mysqldump/pg_dump
		// dump gets the scratch-server recipe and a CSV/SQLite file gets the
		// right driver named.
		if kind, derr := dumpsig.Detect(path); derr == nil {
			// A single .gz/.zst file is almost always ONE chunk of a mydumper
			// dump — the likelier remedy is pointing at the directory (this
			// reader decompresses per-table chunks itself), not decompressing.
			if kind == dumpsig.KindGzip || kind == dumpsig.KindZstd {
				return nil, dumpsig.RefuseWrongDriver("mydumper",
					"point --source at the dump DIRECTORY",
					fmt.Errorf("%q is a single compressed file — the mydumper engine reads the dump DIRECTORY "+
						"(metadata + *-schema.sql + data chunks; .gz/.zst chunks are decompressed automatically); "+
						"pass the directory that contains this file", path))
			}
			if rerr := dumpsig.RefuseRecognised("mydumper", path, kind, false); rerr != nil {
				return nil, rerr
			}
		}
		if drv, ok := dumpsig.FlatFileExtDriver(path); ok {
			return nil, dumpsig.RefuseWrongDriver("mydumper", "use --source-driver "+drv,
				fmt.Errorf("%q looks like a %s flat file — use --source-driver %s "+
					"(the mydumper engine reads a dump DIRECTORY)", path, drv, drv))
		}
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
	companionRows := map[string]int64{}

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

		// Per-table row-count companion (`<db>.<table>-metadata`): when it
		// carries a bare integer (older mydumper), that count feeds the
		// post-stream row-count tripwire. pscale-dump writes the file but
		// leaves it EMPTY (ground-truthed, Bug 188 probe), and mydumper
		// ≥0.12 records counts in the dump-wide metadata instead — so an
		// unparseable companion is simply informational, never a refusal.
		if _, tbl, ok := splitDumpName(base, "-metadata"); ok {
			if n, ok := readCompanionRowCount(full); ok {
				companionRows[tbl] = n
			}
			continue
		}
		// Per-table checksum companions (`<db>.<table>-checksum`):
		// informational only (mydumper's own checksum algorithm).
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
		warnIfChunkNumberGaps(t)
		if n, ok := companionRows[t.name]; ok {
			t.metadataRows, t.hasMetadataRows = n, true
		}
		d.tableOrder = append(d.tableOrder, t.name)
	}
	sort.Strings(d.tableOrder)

	// parseMetadata runs after the companion attach so the dump-wide ini
	// `rows =` counts (the modern, ground-truthed-exact shape) win when
	// both are present.
	if err := d.parseMetadata(); err != nil {
		return nil, err
	}
	// The zero-chunk loss net runs LAST so it sees the merged row counts
	// (companion + dump-wide metadata) — the count decides which of the
	// three shapes a chunk-less table is.
	for _, name := range d.tableOrder {
		d.warnIfTableHasNoChunks(d.tables[name])
	}
	return d, nil
}

// warnedNoChunkTables dedups the zero-chunk loss-net WARNs per (dump dir,
// table) per process. A single migrate re-opens the dump directory once per
// reader open (schema reader, row reader, verifier, …, ~5× per run), and each
// open re-runs the loss net — without the dedup the same WARN printed once
// per open (v0.99.263 cycle observation). sync.Map is the warn-on-first-write
// shape the postgres engine's warnNoFailoverSupport uses.
var warnedNoChunkTables sync.Map

// resetZeroChunkWarnsForTest clears the warned set. Used only from unit
// tests in this package; not part of the engine's surface.
func resetZeroChunkWarnsForTest() {
	warnedNoChunkTables.Range(func(key, _ any) bool {
		warnedNoChunkTables.Delete(key)
		return true
	})
}

// warnIfTableHasNoChunks is the zero-chunk loss net (audit 2026-07-16
// M1.4): a table whose dump carries a schema file but NO data chunks
// streams as EMPTY, and for a count-less dump nothing else would ever
// say so. This is a WARN, not a refusal, because the shape is
// legitimate: ground-truthed against real mydumper v1.0.3, an EMPTY
// table dumps as schema-file-only — no data chunk is written unless
// --build-empty-files — so refusing would block every dump of a schema
// with an empty table. Three shapes:
//
//   - the dump's own metadata records rows = 0 → corroborated empty,
//     silent (modern mydumper always records counts, so its dumps stay
//     noise-free);
//   - the metadata records rows > 0 → the dump CONTRADICTS itself: its
//     chunk files are missing (lost/deleted) or its count is stale —
//     WARN naming the recorded count (kept a WARN, not a refusal, per
//     the MED-D0-2 owner decision: dump-metadata counts are a tripwire,
//     not an oracle);
//   - no recorded count at all → the count-less blind spot. pscale-dump
//     NEVER records row counts (its per-table -metadata companions are
//     empty — Bug 188 probe), so a wholesale chunk-file loss is
//     indistinguishable from a genuinely empty table; WARN says exactly
//     that and points at the live source. Documented in
//     docs/operator/flat-file-sources.md.
//
// Both WARN shapes fire once per (dump dir, table) per process (the
// [warnedNoChunkTables] dedup) — the finding doesn't change between the
// several dump-opens of one run, so repeating it is pure noise.
func (d *dumpDir) warnIfTableHasNoChunks(t *tableFiles) {
	if len(t.chunks) > 0 {
		return
	}
	if t.hasMetadataRows && t.metadataRows == 0 {
		return // the dump's own count corroborates "empty"
	}
	if _, loaded := warnedNoChunkTables.LoadOrStore(d.path+"\x00"+t.name, struct{}{}); loaded {
		return // already warned for this dump dir + table in this process
	}
	if t.hasMetadataRows {
		slog.Warn(
			"mydumper: table has NO data chunk files but the dump's own metadata records rows for it — "+
				"the chunk files are missing (deleted or lost?) or the recorded count is stale; the table "+
				"would stream as EMPTY, so verify the dump (or re-dump) before trusting this table",
			slog.String("table", t.name),
			slog.Int64("metadata_rows", t.metadataRows),
		)
		return
	}
	slog.Warn(
		"mydumper: table has a schema file but NO data chunk files — it will stream as EMPTY. A truly "+
			"empty table dumps this way (mydumper writes no data file without --build-empty-files), but a "+
			"dump whose chunk files were all lost looks IDENTICAL, and this dump records no row count for "+
			"the table (pscale-dump never does), so sluice cannot tell the difference — cross-check the "+
			"live source if this table should have rows",
		slog.String("table", t.name),
	)
}

// warnIfChunkNumberGaps surfaces a non-contiguous chunk-number sequence,
// once per table. This is deliberately a WARN, not the torn-dump refusal
// the metadata.partial marker gets: real mydumper derives chunk numbers
// from PK ranges, so a sparse primary key LEGITIMATELY skips numbers
// (ground-truthed against v1.0.3: a table with PKs 1..500 and
// 90000000..90000500 dumped as chunks 00001-00003 + 450001-450003, and
// `-r` dumps start at 00001 while unsplit tables start at 00000) — but a
// deleted or lost middle chunk produces exactly the same shape, and
// before this WARN it streamed silently short (audit-2026-07-15
// MED-D0-2). The row-count tripwire ([dumpDir.warnIfRowCountMismatch])
// is the decisive cross-check when the dump recorded counts.
func warnIfChunkNumberGaps(t *tableFiles) {
	prev := int64(-1)
	for _, chunk := range t.chunks {
		base, _ := stripCompressionSuffix(filepath.Base(chunk))
		_, _, numText, ok := splitChunkName(base)
		if !ok {
			return // unreachable post-validation; never guess about gaps
		}
		num, err := strconv.ParseInt(numText, 10, 64)
		if err != nil {
			return // a >19-digit chunk id; no continuity claim possible
		}
		if prev >= 0 && num != prev+1 {
			slog.Warn(
				"mydumper: table's data-chunk numbers are not contiguous — mydumper numbers chunks by PK "+
					"range, so a sparse primary key legitimately skips numbers, but a DELETED OR LOST middle "+
					"chunk looks identical and its rows would be silently missing; cross-check the row count "+
					"(the dump-metadata row-count tripwire fires automatically when the dump recorded one, "+
					"or compare against the live source)",
				slog.String("table", t.name),
				slog.Int64("gap_after_chunk", prev),
				slog.Int64("next_chunk", num),
				slog.Int("chunks", len(t.chunks)),
			)
			return
		}
		prev = num
	}
}

// companionMaxBytes bounds a per-table `-metadata` companion read: the
// file is a row count (or empty, pscale-dump); anything big is not one.
const companionMaxBytes = 4096

// readCompanionRowCount reads a per-table `-metadata` companion and
// parses its bare-integer row count. Lenient by design — empty
// (pscale-dump) or otherwise-shaped content is informational only, so
// any read/parse miss reports no count rather than failing the open.
func readCompanionRowCount(path string) (int64, bool) {
	f, err := openDumpFile(path)
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, companionMaxBytes))
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// warnIfRowCountMismatch is the post-stream row-count tripwire: after a
// table's chunks are fully read (bulk copy or verify count), the rows
// actually seen are compared against the dump's own recorded count when
// one exists. A mismatch WARNs naming both counts — deliberately not a
// refusal (owner decision: real-dump metadata fidelity across producers
// is unverified, so the count is a tripwire, not an oracle); the named
// counts let an operator escalate. Catches the missing-middle-chunk
// class the chunk-gap WARN can only suspect (audit-2026-07-15 MED-D0-2).
func (d *dumpDir) warnIfRowCountMismatch(tableName string, seen int64) {
	tf := d.tables[tableName]
	if tf == nil || !tf.hasMetadataRows || tf.metadataRows == seen {
		return
	}
	slog.Warn(
		"mydumper: the dump's own metadata records a different row count than its data chunks hold — "+
			"the dump may be missing a chunk (or its metadata count is stale); verify against the live "+
			"source before trusting this table",
		slog.String("table", tableName),
		slog.Int64("metadata_rows", tf.metadataRows),
		slog.Int64("chunk_rows", seen),
	)
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
// The ini shape also records per-table `rows = N` counts under
// [`db`.`table`] sections (ground-truthed exact against v1.0.3); those
// feed the post-stream row-count tripwire
// ([dumpDir.warnIfRowCountMismatch]), overriding any per-table
// `-metadata` companion count.
//
// Parsing is deliberately LENIENT — the position and counts are
// informational (position logged at INFO, the ADR-0161 §8 handoff hook;
// counts a WARN-only tripwire), so a metadata file with neither shape
// parses to empty fields rather than failing the open.
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

	var sectionTable *tableFiles // the [`db`.`table`] section we are inside, if any
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			// mydumper ≥1.0 comments the [source] coordinates out when it
			// judges them non-authoritative (`# SOURCE_LOG_FILE = …` under
			// --trx-tables). A commented position is deliberately NOT
			// surfaced as one.
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			sectionTable = d.metadataSectionTable(line[1 : len(line)-1])
			continue
		}
		if sectionTable != nil {
			if key, val, found := strings.Cut(line, "="); found && strings.TrimSpace(key) == "rows" {
				if n, err := strconv.ParseInt(metadataValue(val), 10, 64); err == nil && n >= 0 {
					sectionTable.metadataRows, sectionTable.hasMetadataRows = n, true
				}
			}
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

// metadataSectionTable resolves an ini section name to the dump table it
// describes, or nil for the bookkeeping sections ([config], [source],
// [master], [myloader_session_variables], the db-only [`db`] section, a
// table of some other database, …). mydumper writes table sections
// backtick-quoted ([`db`.`table`], ground-truthed v1.0.3); an unquoted
// db.table falls back to the FIRST-dot split, the same convention as
// [splitDumpName].
func (d *dumpDir) metadataSectionTable(section string) *tableFiles {
	var db, tbl string
	if strings.HasPrefix(section, "`") && strings.HasSuffix(section, "`") {
		inner := section[1 : len(section)-1]
		var found bool
		db, tbl, found = strings.Cut(inner, "`.`")
		if !found {
			return nil // [`db`] — the database-checksum section
		}
	} else {
		var found bool
		db, tbl, found = strings.Cut(section, ".")
		if !found {
			return nil
		}
	}
	if db != d.database {
		return nil
	}
	return d.tables[tbl]
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

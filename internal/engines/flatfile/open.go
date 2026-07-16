// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/engines/internal/dumpsig"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// stage validates the flat file at dsn (signature refusals first, explicit-
// flag refusals second) and materializes it into a fresh temp SQLite
// database, returning the temp path. The caller hands the temp path to the
// sqlite staged readers, which own its removal; on ANY error here the temp
// file is already removed and no path is returned.
func (e Engine) stage(ctx context.Context, dsn string) (string, error) {
	path := strings.TrimSpace(dsn)
	if path == "" {
		return "", fmt.Errorf("%s: source path is empty (expected a %s file)", e.Name(), e.Name())
	}
	if err := e.validateSource(path); err != nil {
		return "", err
	}

	f, err := os.Open(path) //nolint:gosec // operator-supplied source path
	if err != nil {
		return "", fmt.Errorf("%s: open %q: %w", e.Name(), path, err)
	}
	defer func() { _ = f.Close() }()

	// The control-table roster at the flat-file door: a file named e.g.
	// sluice_cdc_state.csv derives a sluice-reserved control-table name.
	// The live readers SKIP roster tables from enumeration, but this
	// source has exactly one table — skipping would be a silent empty
	// migration — so the loud move is a refusal naming the remedy
	// (audit-2026-07-15 MED-D0-6, roadmap item 65b).
	tableName := deriveTableName(path)
	if appliershared.IsControlTable(tableName) {
		return "", fmt.Errorf("%s: %q derives table name %q — a sluice-reserved control-table name "+
			"(sluice bookkeeping, never user data); rename the file to load its rows as a user table",
			e.Name(), path, tableName)
	}

	// opts.StageDir (--stage-dir / SLUICE_STAGE_DIR) overrides where the
	// staged copy lives; "" is the os.TempDir default. The staged copy is
	// roughly the source file's size — the override exists for hosts whose
	// /tmp is a small tmpfs (the ADR-0145 hazard class).
	tmp, err := os.CreateTemp(e.opts.StageDir, "sluice-flatfile-*.db")
	if err != nil {
		if e.opts.StageDir != "" {
			return "", fmt.Errorf("%s: create staged db for %q under --stage-dir %q: %w", e.Name(), path, e.opts.StageDir, err)
		}
		return "", fmt.Errorf("%s: create temp db for %q: %w", e.Name(), path, err)
	}
	staged := tmp.Name()
	_ = tmp.Close() // only the path is needed; modernc opens the file itself

	st, err := newStager(ctx, staged, tableName)
	if err != nil {
		_ = os.Remove(staged)
		return "", fmt.Errorf("%s: stage %q: %w", e.Name(), path, err)
	}

	r := bufio.NewReaderSize(f, 1<<20)
	switch e.format {
	case formatNDJSON:
		err = e.stageNDJSON(ctx, r, path, st)
	default:
		err = e.stageCSV(ctx, r, path, st)
	}
	if err == nil {
		err = st.finish(ctx)
	}
	if cerr := st.close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(staged)
		return "", err
	}
	return staged, nil
}

// validateSource runs the open-time refusals: directory misuse, foreign-
// dump / wrong-driver signatures, extension cross-checks, and the explicit-
// declaration requirements (header presence for csv/tsv). Everything here
// fires BEFORE any staging work, so no data moves on a refused source.
func (e Engine) validateSource(path string) error {
	driver := e.Name()

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s: open source %q: %w", driver, path, err)
	}
	if info.IsDir() {
		if dumpsig.LooksLikeMydumperDir(path) {
			return dumpsig.RefuseWrongDriver(driver, "use --source-driver mydumper",
				fmt.Errorf("%q is a mydumper/pscale-dump output directory — use --source-driver mydumper", path))
		}
		return fmt.Errorf("%s: source %q is a directory (the %s driver reads a single file)", driver, path, driver)
	}
	if info.Size() == 0 {
		return fmt.Errorf("%s: source %q is empty (no columns can be derived from an empty file)", driver, path)
	}

	// Content signatures: foreign dumps get the scratch-server recipe;
	// recognised-but-wrong inputs (SQLite binary, gzip/zstd, UTF-16) name the
	// right driver or preparation step.
	kind, err := dumpsig.Detect(path)
	if err != nil {
		return fmt.Errorf("%s: open %q: %w", driver, path, err)
	}
	if rerr := dumpsig.RefuseRecognised(driver, path, kind, false); rerr != nil {
		return rerr
	}

	// Extension cross-checks among the flat-file family. Extensions never
	// decide how bytes are parsed — they only catch the silent-wrong-dialect
	// traps (a .tsv fed to the comma lexer stages one wide column). An
	// EXPLICIT --csv-delimiter overrides the csv/tsv mismatch (the operator
	// declared intent); the ndjson-vs-delimited mismatch has no override.
	if err := e.checkExtension(path); err != nil {
		return err
	}

	switch e.format {
	case formatNDJSON:
		// A single-array JSON document is not NDJSON (it does not stream) —
		// point at the standard conversion instead of failing line 1.
		head, herr := readHeadTrimmed(path)
		if herr != nil {
			return fmt.Errorf("%s: open %q: %w", driver, path, herr)
		}
		if strings.HasPrefix(head, "[") {
			return dumpsig.RefuseWrongDriver(driver, "convert to NDJSON first (jq -c '.[]')",
				fmt.Errorf("%q is a single JSON array document, not NDJSON — convert it first "+
					"(e.g. `jq -c '.[]' %s > out.ndjson`) and re-run", path, filepath.Base(path)))
		}
	default:
		// Header presence is NEVER sniffed: a wrong guess silently eats a data
		// row (header assumed over a headerless file) or turns data into column
		// names. Refuse until the operator declares it.
		if !e.opts.HeaderDeclared {
			return sluicecode.Wrap(sluicecode.CodeCSVHeaderUndeclared,
				"pass --csv-header or --csv-no-header",
				fmt.Errorf("%s: %q: header presence is never sniffed — pass --csv-header "+
					"(first record carries the column names) or --csv-no-header (columns are named col1..colN)",
					driver, path))
		}
	}
	return nil
}

// checkExtension enforces the flat-file-family extension cross-checks
// described in validateSource.
func (e Engine) checkExtension(path string) error {
	driver := e.Name()
	extDriver, known := dumpsig.FlatFileExtDriver(path)
	if !known || extDriver == driver {
		return nil
	}
	delimited := func(d string) bool { return d == "csv" || d == "tsv" }
	switch {
	case delimited(driver) && delimited(extDriver):
		if e.opts.Delimiter != "" {
			return nil // explicit delimiter = declared intent; extension yields
		}
		return dumpsig.RefuseWrongDriver(driver, "use --source-driver "+extDriver,
			fmt.Errorf("%q has a .%s extension but the %s driver was chosen — use --source-driver %s; "+
				"if the file really is %s-delimited, declare that explicitly with --source-driver csv and --csv-delimiter",
				path, extDriver, driver, extDriver, driver))
	default:
		return dumpsig.RefuseWrongDriver(driver, "use --source-driver "+extDriver,
			fmt.Errorf("%q looks like a %s file — use --source-driver %s (or rename the file if the extension is wrong)",
				path, extDriver, extDriver))
	}
}

// readHeadTrimmed returns the first non-whitespace-leading portion of the
// file head (small read; used for the NDJSON single-array check).
func readHeadTrimmed(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied source path
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, 64)
	n, err := f.Read(head)
	if n == 0 && err != nil {
		return "", err
	}
	s := strings.TrimPrefix(string(head[:n]), utf8BOM)
	return strings.TrimLeft(s, " \t\r\n"), nil
}

// parseDelimiter resolves the --csv-delimiter flag text to a byte. Accepted:
// a single ASCII character, or the spellings `\t` / `tab` for TAB. The
// delimiter must not collide with the quoting/record grammar.
func parseDelimiter(s string) (byte, error) {
	switch s {
	case `\t`, "tab":
		return '\t', nil
	}
	if len(s) != 1 || s[0] > unicode.MaxASCII {
		return 0, fmt.Errorf("csv: invalid --csv-delimiter %q (a single ASCII character, or `\\t`/`tab`)", s)
	}
	switch s[0] {
	case '"', '\r', '\n', 0x00:
		return 0, fmt.Errorf("csv: invalid --csv-delimiter %q (the quote, CR, LF, and NUL characters cannot delimit)", s)
	}
	return s[0], nil
}

// delimiter resolves the engine's effective delimiter: the explicit flag
// when passed, else the per-driver default (csv=',', tsv=TAB). The flag
// text was validated at WithFlatFileOptions, so the parse cannot fail here.
func (e Engine) delimiter() byte {
	if e.opts.Delimiter != "" {
		d, err := parseDelimiter(e.opts.Delimiter)
		if err == nil {
			return d
		}
	}
	if e.format == formatTSV {
		return '\t'
	}
	return ','
}

// deriveTableName maps the source filename to the staged (and therefore
// target) table name: the basename minus its final extension, with every
// character outside [A-Za-z0-9_] replaced by '_', prefixed with 't_' when
// it would start with a digit. `users-2024.csv` → `users_2024`. Deterministic
// and visible in --dry-run; rename the file (or the target table after the
// migrate) if a different name is wanted.
func deriveTableName(path string) string {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	var b strings.Builder
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	name := b.String()
	if name == "" || strings.Trim(name, "_") == "" {
		return "flatfile"
	}
	if name[0] >= '0' && name[0] <= '9' {
		name = "t_" + name
	}
	return name
}

// errNULByte is the shared refusal detail for a NUL byte in the input —
// almost always the UTF-16-without-BOM tell (ASCII text interleaved with
// 0x00 is byte-wise "valid UTF-8", so utf8.Valid alone would not catch it).
var errNULByte = errors.New("contains a NUL (0x00) byte — likely UTF-16 without a BOM; " +
	"sluice reads UTF-8 only, transcode the file first (e.g. `iconv -f UTF-16 -t UTF-8`)")

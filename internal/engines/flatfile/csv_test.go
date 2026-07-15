// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ---- helpers ----------------------------------------------------------

// writeSource writes a flat file with the given name into a temp dir and
// returns its path.
func writeSource(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// csvEngine builds a csv Engine with declared options.
func csvEngine(t *testing.T, o Options) Engine {
	t.Helper()
	e, err := Engine{format: formatCSV}.WithFlatFileOptions(o)
	if err != nil {
		t.Fatalf("WithFlatFileOptions: %v", err)
	}
	return e.(Engine)
}

func strp(s string) *string { return &s }

// closeIf closes a reader that implements io.Closer (the ir reader
// interfaces don't declare Close; the staged sqlite readers implement it).
func closeIf(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// readStaged opens the engine's readers over path and drains every row of
// the (single) staged table, returning the schema table and the rows in
// file order.
func readStaged(t *testing.T, e Engine, path string) (*ir.Table, []ir.Row) {
	t.Helper()
	ctx := context.Background()

	sr, err := e.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Tables) != 1 {
		t.Fatalf("staged %d tables; want 1", len(schema.Tables))
	}
	table := schema.Tables[0]

	rr, err := e.OpenRowReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var rows []ir.Row
	for row := range ch {
		rows = append(rows, row)
	}
	type errer interface{ Err() error }
	if e, ok := rr.(errer); ok && e.Err() != nil {
		t.Fatalf("row stream error: %v", e.Err())
	}
	return table, rows
}

// wantText asserts a cell holds exactly the given string.
func wantText(t *testing.T, row ir.Row, col, want string) {
	t.Helper()
	got, ok := row[col]
	if !ok {
		t.Fatalf("column %q missing from row %v", col, row)
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("column %q = %T(%v); want string", col, got, got)
	}
	if s != want {
		t.Errorf("column %q = %q; want %q", col, s, want)
	}
}

func wantNull(t *testing.T, row ir.Row, col string) {
	t.Helper()
	got, ok := row[col]
	if !ok {
		t.Fatalf("column %q missing from row %v", col, row)
	}
	if got != nil {
		t.Errorf("column %q = %#v; want NULL", col, got)
	}
}

// wantCode asserts err's chain carries the given sluicecode.
func wantCode(t *testing.T, err error, code sluicecode.Code) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("error carries no sluicecode: %v", err)
	}
	if ce.Code != code {
		t.Fatalf("error code = %s; want %s (err: %v)", ce.Code, code, err)
	}
}

// ---- the quoting/escape/value-shape pin matrix ------------------------
//
// Everything stages as TEXT, so the "families" of this schema-less path
// are the QUOTING × ESCAPE × NULL-posture shapes (the Bug-74 discipline
// re-derived for a format with no types): quoted/unquoted, embedded
// delimiter/quote/newline/CRLF, empty vs quoted-empty vs the declared
// NULL repr, big integers > 2^53 as text, decimals as text (leading and
// trailing zeros preserved), unicode/emoji, and CRLF vs LF record ends.
// The same corpus is ground-truthed against real Postgres and MySQL
// targets in flatfile_integration_test.go.

func TestCSVValueMatrix(t *testing.T) {
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp(`\N`)})

	content := "plain,quoted,delim,quote,newline,empty_q,null_rep,null_quoted,bigint,neg,decimal,sci,uni,ws\n" +
		`hello,"world","a,b","she said ""hi""","line1` + "\nline2\"" + `,"",\N,"\N",9007199254740993,-9223372036854775808,007.1500,1.5e300,héllo 🚀,"  padded  "` + "\r\n" +
		`x,"y",",,",""""""` + `,"crlf` + "\r\n" + `line","empty",\N,"NULL",18446744073709551615,-1,0.000000000000000001,-2E-7,Ω≈ç√,plain`

	path := writeSource(t, "matrix.csv", content)
	table, rows := readStaged(t, e, path)

	// Every staged column is TEXT.
	for _, c := range table.Columns {
		if _, ok := c.Type.(ir.Text); !ok {
			t.Errorf("column %q staged as %T; want ir.Text", c.Name, c.Type)
		}
	}
	if len(rows) != 2 {
		t.Fatalf("staged %d rows; want 2", len(rows))
	}

	r1, r2 := rows[0], rows[1]
	wantText(t, r1, "plain", "hello")
	wantText(t, r1, "quoted", "world")
	wantText(t, r1, "delim", "a,b")
	wantText(t, r1, "quote", `she said "hi"`)
	wantText(t, r1, "newline", "line1\nline2")
	wantText(t, r1, "empty_q", "")                // quoted empty = empty string
	wantNull(t, r1, "null_rep")                   // unquoted \N = NULL
	wantText(t, r1, "null_quoted", `\N`)          // quoted "\N" = data
	wantText(t, r1, "bigint", "9007199254740993") // 2^53+1, exact as text
	wantText(t, r1, "neg", "-9223372036854775808")
	wantText(t, r1, "decimal", "007.1500") // leading/trailing zeros preserved
	wantText(t, r1, "sci", "1.5e300")
	wantText(t, r1, "uni", "héllo 🚀")
	wantText(t, r1, "ws", "  padded  ")

	wantText(t, r2, "quote", `""`)             // "" "" "" → two literal quotes
	wantText(t, r2, "delim", ",,")             // delimiters inside quotes
	wantText(t, r2, "newline", "crlf\r\nline") // CRLF INSIDE quotes is data
	wantText(t, r2, "empty_q", "empty")
	wantNull(t, r2, "null_rep")
	wantText(t, r2, "null_quoted", "NULL") // quoted "NULL" stays the string
	wantText(t, r2, "bigint", "18446744073709551615")
	wantText(t, r2, "decimal", "0.000000000000000001")
	wantText(t, r2, "sci", "-2E-7")
	wantText(t, r2, "uni", "Ω≈ç√")
}

// TestCSVNullPostureMatrix pins the whole NULL contract: flag unset /
// declared '\N' / declared NULL / declared ” — × unquoted-empty,
// quoted-empty, unquoted-repr, quoted-repr.
func TestCSVNullPostureMatrix(t *testing.T) {
	t.Run("undeclared: unquoted empty refuses with CSV-NULL-AMBIGUOUS", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true})
		path := writeSource(t, "a.csv", "a,b\n1,\n")
		_, err := e.OpenSchemaReader(context.Background(), path)
		wantCode(t, err, sluicecode.CodeCSVNullAmbiguous)
	})
	t.Run("undeclared: quoted empty is the empty string", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true})
		path := writeSource(t, "a.csv", "a,b\n1,\"\"\n")
		_, rows := readStaged(t, e, path)
		wantText(t, rows[0], "b", "")
	})
	t.Run("declared empty: unquoted empty is NULL, quoted empty is the empty string", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp("")})
		path := writeSource(t, "a.csv", "a,b,c\n1,,\"\"\n")
		_, rows := readStaged(t, e, path)
		wantNull(t, rows[0], "b")
		wantText(t, rows[0], "c", "")
	})
	t.Run("declared NULL: the bare word is NULL, empty is the empty string", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp("NULL")})
		path := writeSource(t, "a.csv", "a,b,c,d\n1,NULL,,\"NULL\"\n")
		_, rows := readStaged(t, e, path)
		wantNull(t, rows[0], "b")
		wantText(t, rows[0], "c", "") // a declared repr resolves the ambiguity
		wantText(t, rows[0], "d", "NULL")
	})
}

// TestCSVStrictLexerRefusals pins the loud-failure grammar: bare quote,
// garbage after a closing quote, unterminated quote, lone CR, ragged
// arity, NUL bytes, invalid UTF-8, header defects — each names its record.
func TestCSVStrictLexerRefusals(t *testing.T) {
	base := Options{HeaderDeclared: true, Header: true, NullRepr: strp("")}
	cases := []struct {
		name, content, wantSub string
	}{
		{"bare quote in unquoted field", "a,b\nx\"y,2\n", "bare quote"},
		{"garbage after closing quote", "a,b\n\"x\"y,2\n", "after a closing quote"},
		{"unterminated quoted field", "a,b\n\"x,2\n", "unterminated quoted field"},
		{"lone CR", "a,b\n1\r2,3\n", "lone CR"},
		{"ragged: too few fields", "a,b\n1\n", "want 2"},
		{"ragged: too many fields", "a,b\n1,2,3\n", "want 2"},
		{"NUL byte", "a,b\n1,x\x00y\n", "NUL"},
		{"invalid UTF-8", "a,b\n1,x\xffy\n", "not valid UTF-8"},
		{"empty header name", "a,\n1,2\n", "header column 2 is empty"},
		{"duplicate header name", "a,a\n1,2\n", "duplicate header column"},
		{"empty file", "", "is empty"},
		{"header only, blank body ok", "a,b\n\n", ""}, // header + blank line: zero rows, no error
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := csvEngine(t, base)
			path := writeSource(t, "r.csv", tc.content)
			sr, err := e.OpenSchemaReader(context.Background(), path)
			if tc.wantSub == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				closeIf(sr)
				return
			}
			if err == nil {
				closeIf(sr)
				t.Fatal("expected a refusal, got nil error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// TestCSVHeaderModes pins --csv-no-header naming (col1..colN) and the
// undeclared-header refusal.
func TestCSVHeaderModes(t *testing.T) {
	t.Run("no-header names col1..colN and keeps record 1 as data", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: false, NullRepr: strp("")})
		path := writeSource(t, "nh.csv", "1,2,3\n4,5,6\n")
		table, rows := readStaged(t, e, path)
		if got := len(table.Columns); got != 3 {
			t.Fatalf("%d columns; want 3", got)
		}
		for i, want := range []string{"col1", "col2", "col3"} {
			if table.Columns[i].Name != want {
				t.Errorf("column %d = %q; want %q", i, table.Columns[i].Name, want)
			}
		}
		if len(rows) != 2 {
			t.Fatalf("%d rows; want 2 (record 1 must stay data)", len(rows))
		}
		wantText(t, rows[0], "col1", "1")
	})
	t.Run("undeclared header refuses with CSV-HEADER-UNDECLARED", func(t *testing.T) {
		e := csvEngine(t, Options{})
		path := writeSource(t, "u.csv", "a,b\n1,2\n")
		_, err := e.OpenSchemaReader(context.Background(), path)
		wantCode(t, err, sluicecode.CodeCSVHeaderUndeclared)
	})
}

// TestCSVBOMAndLineEndings pins the UTF-8 BOM strip (lossless, warned) and
// LF-only vs CRLF-only files.
func TestCSVBOMAndLineEndings(t *testing.T) {
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp("")})
	t.Run("UTF-8 BOM stripped", func(t *testing.T) {
		path := writeSource(t, "bom.csv", "\xef\xbb\xbfa,b\n1,2\n")
		table, rows := readStaged(t, e, path)
		if table.Columns[0].Name != "a" {
			t.Errorf("first column = %q; want %q (BOM must not leak into the name)", table.Columns[0].Name, "a")
		}
		wantText(t, rows[0], "a", "1")
	})
	t.Run("CRLF file", func(t *testing.T) {
		path := writeSource(t, "crlf.csv", "a,b\r\n1,2\r\n3,4\r\n")
		_, rows := readStaged(t, e, path)
		if len(rows) != 2 {
			t.Fatalf("%d rows; want 2", len(rows))
		}
		wantText(t, rows[1], "b", "4")
	})
	t.Run("no trailing newline", func(t *testing.T) {
		path := writeSource(t, "notrail.csv", "a,b\n1,2")
		_, rows := readStaged(t, e, path)
		if len(rows) != 1 {
			t.Fatalf("%d rows; want 1", len(rows))
		}
		wantText(t, rows[0], "b", "2")
	})
	t.Run("blank lines skipped", func(t *testing.T) {
		path := writeSource(t, "blank.csv", "a,b\n1,2\n\n3,4\n\n")
		_, rows := readStaged(t, e, path)
		if len(rows) != 2 {
			t.Fatalf("%d rows; want 2", len(rows))
		}
	})
}

// TestTSVAndDelimiter pins the tsv driver (fixed TAB) and the csv driver's
// --csv-delimiter override, including quoting around tabs.
func TestTSVAndDelimiter(t *testing.T) {
	t.Run("tsv driver", func(t *testing.T) {
		te, err := Engine{format: formatTSV}.WithFlatFileOptions(Options{HeaderDeclared: true, Header: true, NullRepr: strp(`\N`)})
		if err != nil {
			t.Fatalf("WithFlatFileOptions: %v", err)
		}
		path := writeSource(t, "d.tsv", "a\tb\tc\n1\t\"x\ty\"\t\\N\n")
		_, rows := readStaged(t, te.(Engine), path)
		wantText(t, rows[0], "b", "x\ty") // tab inside quotes is data
		wantNull(t, rows[0], "c")
	})
	t.Run("csv driver with explicit semicolon", func(t *testing.T) {
		e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp(""), Delimiter: ";"})
		path := writeSource(t, "semi.csv", "a;b\n1,5;2\n")
		_, rows := readStaged(t, e, path)
		wantText(t, rows[0], "a", "1,5") // comma is data under a ; delimiter
	})
	t.Run("tsv driver refuses a non-tab delimiter", func(t *testing.T) {
		_, err := Engine{format: formatTSV}.WithFlatFileOptions(Options{HeaderDeclared: true, Header: true, Delimiter: ";"})
		if err == nil || !strings.Contains(err.Error(), "fixed to a TAB") {
			t.Fatalf("want the fixed-tab refusal; got %v", err)
		}
	})
	t.Run("tsv driver accepts an explicit tab spelling", func(t *testing.T) {
		if _, err := (Engine{format: formatTSV}).WithFlatFileOptions(Options{HeaderDeclared: true, Header: true, Delimiter: "tab"}); err != nil {
			t.Fatalf("explicit tab on tsv should be accepted: %v", err)
		}
	})
	t.Run("invalid delimiters refuse", func(t *testing.T) {
		for _, d := range []string{`"`, "\n", "\r", "ab", "→"} {
			if _, err := (Engine{format: formatCSV}).WithFlatFileOptions(Options{HeaderDeclared: true, Header: true, Delimiter: d}); err == nil {
				t.Errorf("delimiter %q should refuse", d)
			}
		}
	})
}

// TestDeriveTableName pins the filename → table-name mapping.
func TestDeriveTableName(t *testing.T) {
	cases := map[string]string{
		"users.csv":                         "users",
		"users-2024.csv":                    "users_2024",
		"data.export.tsv":                   "data_export",
		"2024report.csv":                    "t_2024report",
		"übung.csv":                         "_bung",
		filepath.Join("x", "orders.ndjson"): "orders",
	}
	for in, want := range cases {
		if got := deriveTableName(in); got != want {
			t.Errorf("deriveTableName(%q) = %q; want %q", in, got, want)
		}
	}
}

// TestStagedReaderOwnsTempFile pins the temp-db lifecycle: Close removes it.
func TestStagedReaderOwnsTempFile(t *testing.T) {
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp("")})
	path := writeSource(t, "own.csv", "a\n1\n")
	staged, err := e.stage(context.Background(), path)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	sr, err := sqlite.OpenStagedSchemaReader(context.Background(), staged, path)
	if err != nil {
		t.Fatalf("open staged: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(staged); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staged temp db %q still exists after Close (err=%v)", staged, err)
	}
}

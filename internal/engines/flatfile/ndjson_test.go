// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func ndjsonEngine(t *testing.T) Engine {
	t.Helper()
	e, err := Engine{format: formatNDJSON}.WithFlatFileOptions(Options{})
	if err != nil {
		t.Fatalf("WithFlatFileOptions: %v", err)
	}
	return e.(Engine)
}

// ndjsonMatrixLine1 carries every JSON value kind; the accented char in
// "esc" is deliberately spelled as the \u ESCAPE (not a literal) so the
// JSON string decode is exercised alongside \t and \".
const ndjsonMatrixLine1 = `{"s":"plain","esc":"a\tb\"c` + "\\u00e9" + `d",` +
	`"big":9007199254740993,"huge":123456789012345678901234567890,` +
	`"dec":0.10000000000000000001,"exp":-1.5E+300,"neg":-9223372036854775808,` +
	`"b_t":true,"b_f":false,"n":null,"obj":{"k": [1, 2]},"arr":[1,"two",null]}`

const ndjsonMatrixLine2 = `{"s":"röw2 🚀","esc":"","big":1,"huge":0,"dec":0.5,` +
	`"exp":2e-7,"neg":-0,"b_t":false,"b_f":true,"n":null,"obj":{},"arr":[]}`

// TestNDJSONValueMatrix pins the raw-text value contract per JSON kind:
// string (incl. \u escapes and unicode), number (int, > 2^53, beyond
// int64, decimal, exponent — all RAW TEXT, never a float64 round-trip),
// bool, null, nested object/array (verbatim raw JSON).
func TestNDJSONValueMatrix(t *testing.T) {
	e := ndjsonEngine(t)
	content := ndjsonMatrixLine1 + "\n" + ndjsonMatrixLine2 + "\n"

	path := writeSource(t, "matrix.ndjson", content)
	table, rows := readStaged(t, e, path)

	for _, c := range table.Columns {
		if _, ok := c.Type.(ir.Text); !ok {
			t.Errorf("column %q staged as %T; want ir.Text", c.Name, c.Type)
		}
	}
	if len(rows) != 2 {
		t.Fatalf("staged %d rows; want 2", len(rows))
	}
	r1, r2 := rows[0], rows[1]

	wantText(t, r1, "s", "plain")
	wantText(t, r1, "esc", "a\tb\"céd")        // \t, \", and \u escapes JSON-decoded
	wantText(t, r1, "big", "9007199254740993") // 2^53+1 exact
	wantText(t, r1, "huge", "123456789012345678901234567890")
	wantText(t, r1, "dec", "0.10000000000000000001")
	wantText(t, r1, "exp", "-1.5E+300") // raw token text, case preserved
	wantText(t, r1, "neg", "-9223372036854775808")
	wantText(t, r1, "b_t", "true")
	wantText(t, r1, "b_f", "false")
	wantNull(t, r1, "n")
	wantText(t, r1, "obj", `{"k": [1, 2]}`) // nested doc: raw text, verbatim
	wantText(t, r1, "arr", `[1,"two",null]`)

	wantText(t, r2, "s", "röw2 🚀")
	wantText(t, r2, "esc", "")
	wantText(t, r2, "neg", "-0") // raw text: -0 preserved, not normalized to 0
	wantText(t, r2, "obj", "{}")
	wantText(t, r2, "arr", "[]")
}

// TestNDJSONColumnEvolution pins absent-key-vs-null and mid-file column
// growth: a key first seen on line 3 backfills earlier rows as NULL, and
// column order is first-seen order.
func TestNDJSONColumnEvolution(t *testing.T) {
	e := ndjsonEngine(t)
	content := `{"a":"1"}` + "\n" +
		`{"a":"2","b":null}` + "\n" +
		"\n" + // blank line skipped
		`{"c":"new","a":"3"}` + "\n" +
		`{}` + "\n" // empty object AFTER columns exist = all-NULL row
	path := writeSource(t, "grow.ndjson", content)
	table, rows := readStaged(t, e, path)

	names := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		names[i] = c.Name
	}
	if got, want := strings.Join(names, ","), "a,b,c"; got != want {
		t.Fatalf("columns = %s; want %s (first-seen order)", got, want)
	}
	if len(rows) != 4 {
		t.Fatalf("staged %d rows; want 4", len(rows))
	}
	wantNull(t, rows[0], "b") // absent key = NULL
	wantNull(t, rows[0], "c") // pre-growth row backfilled NULL
	wantNull(t, rows[1], "b") // explicit null = NULL (same collapse, documented)
	wantText(t, rows[2], "c", "new")
	wantNull(t, rows[3], "a")
}

// TestNDJSONRefusals pins the loud-failure shapes: non-object lines,
// duplicate keys, trailing garbage, invalid JSON, empty-object-first,
// single-array documents, and the csv-flag misuse refusals.
func TestNDJSONRefusals(t *testing.T) {
	cases := []struct {
		name, content, wantSub string
	}{
		// A LEADING '[' is caught earlier as a single-array document (its own
		// recipe, pinned below); a mid-file array line hits the per-line check.
		{"top-level array line", `{"a":1}` + "\n" + `[1,2]` + "\n", "requires one JSON OBJECT per line"},
		{"top-level scalar line", `42` + "\n", "requires one JSON OBJECT per line"},
		{"duplicate key", `{"a":1,"a":2}` + "\n", "duplicate key"},
		{"trailing garbage", `{"a":1} {"b":2}` + "\n", "trailing content"},
		{"invalid JSON", `{"a":` + "\n", "invalid JSON"},
		{"empty object before any column", `{}` + "\n" + `{"a":1}` + "\n", "empty object before any column"},
		{"NUL byte", "{\"a\":\"x\x00y\"}\n", "NUL"},
		{"NUL escape in a string", "{\"a\":\"x\\u0000y\"}\n", "decodes to a NUL"},
		{"invalid UTF-8", "{\"a\":\"x\xffy\"}\n", "not valid UTF-8"},
		// F3: key-name refusals — at the offending LINE, with named messages,
		// instead of a late zero-length-identifier / duplicate-column error
		// from the staging database.
		{"empty key", `{"":"x"}` + "\n", "empty object key"},
		{"case-colliding keys in one object", `{"a":1,"A":2}` + "\n", "case-insensitively"},
		{"case-colliding keys across lines", `{"a":1}` + "\n" + `{"A":2}` + "\n", "case-insensitively"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := ndjsonEngine(t)
			path := writeSource(t, "r.ndjson", tc.content)
			_, err := e.OpenSchemaReader(context.Background(), path)
			if err == nil {
				t.Fatal("expected a refusal, got nil error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}

	// The \u0000 escape bypasses the raw-byte NUL scan (audit L-D0-14):
	// the refusal must fire at the flatfile layer, naming file, LINE, and
	// key — not surface later as a coordinate-free PG COPY error.
	t.Run("NUL escape names the line and key", func(t *testing.T) {
		e := ndjsonEngine(t)
		path := writeSource(t, "nul.ndjson",
			"{\"a\":\"clean\"}\n{\"a\":\"ok\",\"b\":\"x\\u0000y\"}\n")
		_, err := e.OpenSchemaReader(context.Background(), path)
		if err == nil {
			t.Fatal("expected the NUL-escape refusal, got nil error")
		}
		for _, want := range []string{"line 2", `"b"`, "decodes to a NUL"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err.Error(), want)
			}
		}
	})

	// The refusal covers only the DECODED-string case: a NUL escape inside
	// a nested object/array stays as its 6-character escape text — TEXT-
	// representable on every target — and must keep staging verbatim.
	t.Run("NUL escape inside a nested document stages verbatim", func(t *testing.T) {
		e := ndjsonEngine(t)
		path := writeSource(t, "nested-nul.ndjson", "{\"doc\":{\"k\":\"x\\u0000y\"}}\n")
		_, rows := readStaged(t, e, path)
		if len(rows) != 1 {
			t.Fatalf("staged %d rows; want 1", len(rows))
		}
		wantText(t, rows[0], "doc", "{\"k\":\"x\\u0000y\"}")
	})

	t.Run("single-array JSON document names the jq recipe", func(t *testing.T) {
		e := ndjsonEngine(t)
		path := writeSource(t, "arr.json", `[{"a":1},{"a":2}]`)
		_, err := e.OpenSchemaReader(context.Background(), path)
		if err == nil || !strings.Contains(err.Error(), "jq -c") {
			t.Fatalf("want the single-array jq recipe; got %v", err)
		}
	})

	t.Run("csv flags refuse on the ndjson driver", func(t *testing.T) {
		for name, o := range map[string]Options{
			"--csv-null":      {NullRepr: strp("NULL")},
			"--csv-header":    {HeaderDeclared: true, Header: true},
			"--csv-delimiter": {Delimiter: ";"},
		} {
			if _, err := (Engine{format: formatNDJSON}).WithFlatFileOptions(o); err == nil {
				t.Errorf("%s should refuse on ndjson", name)
			}
		}
	})
}

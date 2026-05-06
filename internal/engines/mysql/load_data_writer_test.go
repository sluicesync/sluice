package mysql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestTSVEncode_Primitives(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 34, 56, 123456000, time.UTC)

	cases := []struct {
		name string
		in   any
		want string
	}{
		{"nil → \\N", nil, `\N`},
		{"bool true → 1", true, "1"},
		{"bool false → 0", false, "0"},
		{"int64", int64(-42), "-42"},
		{"uint64", uint64(1 << 63), "9223372036854775808"},
		{"int", 7, "7"},
		{"float64", 3.14, "3.14"},
		{"string plain", "hello", "hello"},
		{"empty string", "", ""},
		{"string with tab", "a\tb", `a\tb`},
		{"string with newline", "a\nb", `a\nb`},
		{"string with CR", "a\rb", `a\rb`},
		{"string with backslash", `a\b`, `a\\b`},
		{"string with NUL", "a\x00b", `a\0b`},
		{"string mixed", "a\\\tb\nc\rd\x00e", `a\\\tb\nc\rd\0e`},
		{"bytes", []byte{0xde, 0xad, 0xbe, 0xef}, "\xde\xad\xbe\xef"},
		{"bytes with tab", []byte{'a', '\t', 'b'}, `a\tb`},
		{"bytes with NUL", []byte{0x00}, `\0`},
		{"empty bytes", []byte{}, ""},
		{"time UTC", now, "2026-05-01 12:34:56.123456"},
		{"time second precision", time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), "2026-05-01 00:00:00"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := tsvEncode(nil, c.in)
			if err != nil {
				t.Fatalf("tsvEncode(%#v) error: %v", c.in, err)
			}
			if string(got) != c.want {
				t.Errorf("tsvEncode(%#v) = %q; want %q", c.in, string(got), c.want)
			}
		})
	}
}

func TestTSVEncode_TimeNonUTCNormalised(t *testing.T) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Skipf("LA tz unavailable: %v", err)
	}
	// 12:00 LA == 19:00 UTC in May (PDT, UTC-7)
	la := time.Date(2026, 5, 1, 12, 0, 0, 0, loc)
	got, err := tsvEncode(nil, la)
	if err != nil {
		t.Fatalf("tsvEncode: %v", err)
	}
	if string(got) != "2026-05-01 19:00:00" {
		t.Errorf("tsvEncode normalised LA → UTC: got %q; want %q", string(got), "2026-05-01 19:00:00")
	}
}

func TestTSVEncode_UnsupportedType(t *testing.T) {
	type weird struct{ X int }
	_, err := tsvEncode(nil, weird{X: 1})
	if err == nil {
		t.Fatal("tsvEncode on unsupported type: want error, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported value type") {
		t.Errorf("error message: %v", err)
	}
}

func TestEncodeRowsTSV_HappyPath(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "name", Type: ir.Varchar{Length: 64}},
		{Name: "active", Type: ir.Boolean{}},
	}

	rows := make(chan ir.Row, 3)
	rows <- ir.Row{"id": int64(1), "name": "Alice", "active": true}
	rows <- ir.Row{"id": int64(2), "name": "Bob\twith tab", "active": false}
	rows <- ir.Row{"id": int64(3), "name": nil, "active": true}
	close(rows)

	var buf bytes.Buffer
	if err := encodeRowsTSV(context.Background(), &buf, cols, rows); err != nil {
		t.Fatalf("encodeRowsTSV: %v", err)
	}

	// The middle line carries Bob's real tab as the escape sequence
	// `\t` (two chars), with real tabs separating fields.
	want := "1\tAlice\t1\n" +
		"2\tBob\\twith tab\t0\n" +
		"3\t\\N\t1\n"

	if buf.String() != want {
		t.Errorf("encodeRowsTSV:\n got  %q\n want %q", buf.String(), want)
	}
}

func TestEncodeRowsTSV_GeneratedColumnsExcludedByCaller(t *testing.T) {
	// encodeRowsTSV doesn't filter generated columns itself — its
	// caller passes the already-filtered list. This test pins the
	// contract: the function emits exactly the columns it's given,
	// in order.
	cols := []*ir.Column{
		{Name: "a", Type: ir.Integer{Width: 32}},
		{Name: "c", Type: ir.Integer{Width: 32}},
	}
	rows := make(chan ir.Row, 1)
	rows <- ir.Row{"a": int64(1), "b": int64(2), "c": int64(3)}
	close(rows)

	var buf bytes.Buffer
	if err := encodeRowsTSV(context.Background(), &buf, cols, rows); err != nil {
		t.Fatalf("encodeRowsTSV: %v", err)
	}
	if buf.String() != "1\t3\n" {
		t.Errorf("encodeRowsTSV with filtered cols: got %q; want %q", buf.String(), "1\t3\n")
	}
}

func TestEncodeRowsTSV_ContextCancelled(t *testing.T) {
	cols := []*ir.Column{{Name: "n", Type: ir.Integer{Width: 32}}}
	rows := make(chan ir.Row) // unbuffered, never sent

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	err := encodeRowsTSV(ctx, &buf, cols, rows)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("encodeRowsTSV with cancelled ctx: got %v; want context.Canceled", err)
	}
}

func TestEncodeRowsTSV_PrepareValueAppliedToSet(t *testing.T) {
	// prepareValue collapses []string → comma-joined string for ir.Set
	// columns. The serializer feeds values through prepareValue, so
	// the wire form matches the BatchedInsert path.
	cols := []*ir.Column{
		{Name: "tags", Type: ir.Set{Values: []string{"a", "b", "c"}}},
	}
	rows := make(chan ir.Row, 1)
	rows <- ir.Row{"tags": []string{"a", "c"}}
	close(rows)

	var buf bytes.Buffer
	if err := encodeRowsTSV(context.Background(), &buf, cols, rows); err != nil {
		t.Fatalf("encodeRowsTSV: %v", err)
	}
	if buf.String() != "a,c\n" {
		t.Errorf("set-column TSV: got %q; want %q", buf.String(), "a,c\n")
	}
}

func TestEncodeRowsTSV_PrepareValueAppliedToJSON(t *testing.T) {
	// JSON []byte → string conversion is a prepareValue
	// responsibility (PlanetScale-_binary fix). The serializer
	// observes the converted value and writes it as a TSV string.
	cols := []*ir.Column{
		{Name: "doc", Type: ir.JSON{Binary: true}},
	}
	rows := make(chan ir.Row, 1)
	rows <- ir.Row{"doc": []byte(`{"k":"v"}`)}
	close(rows)

	var buf bytes.Buffer
	if err := encodeRowsTSV(context.Background(), &buf, cols, rows); err != nil {
		t.Fatalf("encodeRowsTSV: %v", err)
	}
	if buf.String() != `{"k":"v"}`+"\n" {
		t.Errorf("json-column TSV: got %q; want %q", buf.String(), `{"k":"v"}`+"\n")
	}
}

func TestBuildLoadDataStmt(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "weird`col", Type: ir.Varchar{Length: 32}},
		{Name: "doc", Type: ir.JSON{Binary: true}},
		{Name: "data", Type: ir.Blob{Size: ir.BlobRegular}},
	}
	got := buildLoadDataStmt("mydb", "users", cols, "sluice_loaddata_abc123")
	want := "LOAD DATA LOCAL INFILE 'Reader::sluice_loaddata_abc123' INTO TABLE `mydb`.`users` " +
		"CHARACTER SET binary " +
		"FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' " +
		"LINES TERMINATED BY '\\n' " +
		"(@c0, @c1, @c2, @c3) SET " +
		"`id` = @c0, " +
		"`weird``col` = CONVERT(@c1 USING utf8mb4), " +
		"`doc` = CONVERT(@c2 USING utf8mb4), " +
		"`data` = @c3"
	if got != want {
		t.Errorf("buildLoadDataStmt:\n got  %q\n want %q", got, want)
	}
}

func TestBuildLoadDataStmt_NoSchema(t *testing.T) {
	cols := []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}
	got := buildLoadDataStmt("", "t", cols, "sluice_loaddata_x")
	if !strings.Contains(got, "INTO TABLE `t`") {
		t.Errorf("no-schema form should target unqualified table; got %q", got)
	}
	if strings.Contains(got, "INTO TABLE ``") {
		t.Errorf("no-schema form should not emit empty backticks; got %q", got)
	}
}

func TestColumnSetExpr(t *testing.T) {
	cases := []struct {
		name string
		col  *ir.Column
		want string
	}{
		{"int → passthrough", &ir.Column{Name: "n", Type: ir.Integer{Width: 32}}, "@c0"},
		{"blob → passthrough", &ir.Column{Name: "b", Type: ir.Blob{Size: ir.BlobRegular}}, "@c0"},
		{"timestamp → passthrough", &ir.Column{Name: "t", Type: ir.Timestamp{}}, "@c0"},
		{"varchar → utf8mb4 convert", &ir.Column{Name: "s", Type: ir.Varchar{Length: 32}}, "CONVERT(@c0 USING utf8mb4)"},
		{"text → utf8mb4 convert", &ir.Column{Name: "t", Type: ir.Text{Size: ir.TextLong}}, "CONVERT(@c0 USING utf8mb4)"},
		{"set → utf8mb4 convert", &ir.Column{Name: "x", Type: ir.Set{Values: []string{"a"}}}, "CONVERT(@c0 USING utf8mb4)"},
		{"json → utf8mb4 convert", &ir.Column{Name: "j", Type: ir.JSON{Binary: true}}, "CONVERT(@c0 USING utf8mb4)"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := columnSetExpr(c.col, "@c0"); got != c.want {
				t.Errorf("columnSetExpr: got %q; want %q", got, c.want)
			}
		})
	}
}

func TestMintReaderName_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 64; i++ {
		n, err := mintReaderName()
		if err != nil {
			t.Fatalf("mintReaderName: %v", err)
		}
		if !strings.HasPrefix(n, loadDataReaderPrefix) {
			t.Errorf("name %q missing expected prefix", n)
		}
		if seen[n] {
			t.Errorf("name collision after %d draws: %q", i, n)
		}
		seen[n] = true
	}
}

func TestEncodeRowsTSV_LargeBatch(t *testing.T) {
	// Catches buffer-reuse bugs where the per-row buf isn't reset
	// between rows.
	cols := []*ir.Column{
		{Name: "n", Type: ir.Integer{Width: 32}},
		{Name: "s", Type: ir.Varchar{Length: 32}},
	}
	const total = 1000
	rows := make(chan ir.Row, 16)
	go func() {
		defer close(rows)
		for i := 0; i < total; i++ {
			rows <- ir.Row{"n": int64(i), "s": "row"}
		}
	}()

	var buf bytes.Buffer
	if err := encodeRowsTSV(context.Background(), &buf, cols, rows); err != nil {
		t.Fatalf("encodeRowsTSV: %v", err)
	}

	got := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(got) != total {
		t.Fatalf("got %d lines; want %d", len(got), total)
	}
	if got[0] != "0\trow" {
		t.Errorf("first line = %q; want %q", got[0], "0\trow")
	}
	if got[total-1] != "999\trow" {
		t.Errorf("last line = %q; want %q", got[total-1], "999\trow")
	}
}

// io.Pipe is goroutine-safe by construction; the test exercises the
// producer/consumer handshake the real writer relies on.
func TestEncodeRowsTSV_StreamsToReader(t *testing.T) {
	cols := []*ir.Column{{Name: "n", Type: ir.Integer{Width: 32}}}
	rows := make(chan ir.Row, 4)
	for i := 0; i < 4; i++ {
		rows <- ir.Row{"n": int64(i)}
	}
	close(rows)

	pr, pw := io.Pipe()
	encErr := make(chan error, 1)
	go func() {
		err := encodeRowsTSV(context.Background(), pw, cols, rows)
		_ = pw.Close()
		encErr <- err
	}()

	got, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if err := <-encErr; err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := "0\n1\n2\n3\n"
	if string(got) != want {
		t.Errorf("piped output:\n got  %q\n want %q", string(got), want)
	}
}

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The shape half of the MySQL array-leaf policy, and the three claims
// [arrayLeafForJSON]'s doc comment makes.
//
// The family half — every element family the Postgres reader can produce
// × its declared MySQL verdict — is the roster gate next door
// (TestEveryDecodableArrayElementHasAMySQLLeafVerdict). The real-server
// half is TestMigrate_PGToMySQL_JSONAndByteaArrays in internal/pipeline.
// This file covers what neither does: the SHAPES a value can take around
// a leaf, and the loud refusals.

package mysql

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// jsonArrayCol / byteaArrayCol / textArrayCol are the three columns the
// shape matrix runs, chosen so the two []byte-leaf families are covered
// alongside a control whose leaf policy is untouched. Without the
// control a matrix that stopped exercising leaves at all would still be
// green on the two families it was written for.
var (
	jsonArrayCol  = &ir.Column{Name: "j", Type: ir.Array{Element: ir.JSON{}}}
	byteaArrayCol = &ir.Column{Name: "y", Type: ir.Array{Element: ir.Blob{}}}
	textArrayCol  = &ir.Column{Name: "t", Type: ir.Array{Element: ir.Text{}}}
)

// TestConvertArrayLikeToJSON_ShapeMatrix runs every shape an ir.Array
// value can take — 1-D, multi-dim, NULL element, empty, and the mixed
// 2-D that carries a NULL beside a present leaf — across the two
// []byte-leaf families and the string control.
func TestConvertArrayLikeToJSON_ShapeMatrix(t *testing.T) {
	for _, tc := range []struct {
		name string
		col  *ir.Column
		in   []any
		want string
	}{
		// ---- json / jsonb: the document's text, carried as a string. ----
		{
			"json 1-D mixed documents",
			jsonArrayCol,
			[]any{[]byte(`{"a":1}`), []byte(`[1,2]`), []byte(`"str"`), []byte(`42`)},
			`["{\"a\":1}","[1,2]","\"str\"","42"]`,
		},
		{
			"json 2-D",
			jsonArrayCol,
			[]any{[]any{[]byte(`{"a":1}`), []byte(`{"b":2}`)}, []any{[]byte(`{"c":3}`), []byte(`{"d":4}`)}},
			`[["{\"a\":1}","{\"b\":2}"],["{\"c\":3}","{\"d\":4}"]]`,
		},
		{
			"json NULL element",
			jsonArrayCol,
			[]any{[]byte(`{"a":1}`), nil, []byte(`{"b":2}`)},
			`["{\"a\":1}",null,"{\"b\":2}"]`,
		},
		{
			// The distinction the whole design turns on. Nesting the
			// documents would render BOTH elements as `null`.
			"json null DOCUMENT beside a SQL NULL element",
			jsonArrayCol,
			[]any{[]byte(`null`), nil, []byte(`true`)},
			`["null",null,"true"]`,
		},
		{
			"json 3-D",
			jsonArrayCol,
			[]any{[]any{[]any{[]byte(`1`)}}},
			`[[["1"]]]`,
		},
		{"json empty", jsonArrayCol, []any{}, `[]`},

		// ---- bytea: declared base64. ----
		{
			"bytea 1-D",
			byteaArrayCol,
			[]any{[]byte{0x00}, []byte{0xff}, []byte{0xde, 0xad, 0xbe, 0xef}},
			`["AA==","/w==","3q2+7w=="]`,
		},
		{
			"bytea 2-D",
			byteaArrayCol,
			[]any{[]any{[]byte{0x01}, []byte{0x02}}, []any{[]byte{0x03}, []byte{0x04}}},
			`[["AQ==","Ag=="],["Aw==","BA=="]]`,
		},
		{
			// An EMPTY bytea is a present value and must not become null —
			// the distinction byteaArrayLeaf's nil-slice wart protects on
			// the Postgres side, checked here on this one.
			"bytea empty element beside a NULL element",
			byteaArrayCol,
			[]any{[]byte{}, nil, []byte("A")},
			`["",null,"QQ=="]`,
		},
		{
			// The item-135 trap from the other side: content that SPELLS
			// the \x-hex rendering must be carried as its own bytes.
			"bytea whose content spells the hex prefix",
			byteaArrayCol,
			[]any{[]byte(`\xAB`)},
			`["XHhBQg=="]`,
		},
		{"bytea empty array", byteaArrayCol, []any{}, `[]`},

		// ---- the string control. ----
		{"text 1-D", textArrayCol, []any{"a", "b"}, `["a","b"]`},
		{"text 2-D", textArrayCol, []any{[]any{"a", "b"}, []any{"c", "d"}}, `[["a","b"],["c","d"]]`},
		{"text NULL element", textArrayCol, []any{"a", nil, "b"}, `["a",null,"b"]`},
		{
			"text spelling the four letters null",
			textArrayCol,
			[]any{"null", nil, "true"},
			`["null",null,"true"]`,
		},
		{"text empty", textArrayCol, []any{}, `[]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := prepareValue(tc.in, tc.col)
			if err != nil {
				t.Fatalf("prepareValue: %v", err)
			}
			s, ok := got.(string)
			if !ok {
				t.Fatalf("prepareValue returned %T, not the JSON string a MySQL JSON column needs", got)
			}
			if s != tc.want {
				t.Errorf("got  %s\nwant %s", s, tc.want)
			}
			// Whatever we emit has to be a document MySQL's JSON parser
			// would accept; a hand-built string is easy to get subtly wrong.
			if !json.Valid([]byte(s)) {
				t.Errorf("emitted %s, which is not valid JSON", s)
			}
		})
	}
}

// TestConvertArrayLikeToJSON_WholeColumnNULL pins the fifth shape, which
// never reaches convertArrayLikeToJSON at all: prepareValue short-
// circuits a nil value before any array handling. Stated as a test
// because "the NULL case is handled upstream" is exactly the kind of
// claim that stops being true quietly.
func TestConvertArrayLikeToJSON_WholeColumnNULL(t *testing.T) {
	for _, col := range []*ir.Column{jsonArrayCol, byteaArrayCol, textArrayCol} {
		got, err := prepareValue(nil, col)
		if err != nil {
			t.Fatalf("column %s: %v", col.Name, err)
		}
		if got != nil {
			t.Errorf("column %s: a whole-column NULL became %#v", col.Name, got)
		}
	}
}

// TestArrayLeafForJSON_TriggerDoorByteaAgreesWithTheSQLDoor is the
// cross-door check. The pgtrigger reader delivers a bytea leaf as PG's
// `\x`-hex TEXT while the pgoutput / scan lanes deliver raw []byte; a
// leaf policy that only knew about []byte would store the ASCII of the
// hex on one door and base64 on the other — the same value, two
// encodings, decided by which door the row came through.
func TestArrayLeafForJSON_TriggerDoorByteaAgreesWithTheSQLDoor(t *testing.T) {
	fromSQL, err := prepareValue([]any{[]byte{0xde, 0xad, 0xbe, 0xef}}, byteaArrayCol)
	if err != nil {
		t.Fatalf("SQL door: %v", err)
	}
	fromTrigger, err := prepareValue([]any{`\xdeadbeef`}, byteaArrayCol)
	if err != nil {
		t.Fatalf("trigger door: %v", err)
	}
	if fromSQL != fromTrigger {
		t.Errorf("the two reader doors disagree on the same bytea value:\n  SQL door     = %v\n"+
			"  trigger door = %v", fromSQL, fromTrigger)
	}
	if fromSQL != `["3q2+7w=="]` {
		t.Errorf("bytea array = %v; want [\"3q2+7w==\"]", fromSQL)
	}
}

// TestArrayLeafForJSON_CDCApplierLaneRefusesRatherThanGuesses pins the
// one lane that cannot resolve the element family at all, and the
// reason the pipeline grew a preflight for it.
//
// The ChangeApplier resolves its column descriptors from the TARGET's
// information_schema, where MySQL renders BOTH `json[]` and `bytea[]` as
// the same `JSON` column — so the element type is simply absent and the
// two families arrive as the same `[]byte`. The writer refuses rather
// than guessing: guessing wrong means the cold copy and every later CDC
// apply encode the same column differently, with no error on either
// side. pipeline.preflightArrayBytesLeafOnCDC turns this late refusal
// into an early one; this is the backstop that makes the blind halves of
// that preflight (warm resume, the multi-database fan-out) loud rather
// than silent.
func TestArrayLeafForJSON_CDCApplierLaneRefusesRatherThanGuesses(t *testing.T) {
	// Exactly the shape colTypesFor builds: the TARGET's own column.
	colTypes := map[string]*ir.Column{"c": {Name: "c", Type: ir.JSON{Binary: true}}}
	for _, leaf := range []any{[]byte(`{"a":1}`), []byte{0xde, 0xad}} {
		got, err := prepareApplierValue([]any{leaf}, colTypes, "c")
		if err == nil {
			t.Errorf("the applier lane accepted a %T leaf and produced %#v; it cannot know whether those "+
				"bytes are a json document or a bytea, and either guess diverges from the cold copy",
				leaf, got)
			continue
		}
		if !strings.Contains(err.Error(), `column "c"`) {
			t.Errorf("refusal does not name the column: %v", err)
		}
	}
	// A string-leaf family still applies cleanly on this lane — the
	// refusal is scoped to the byte-shaped leaves, not to arrays.
	got, err := prepareApplierValue([]any{"a", nil, "b"}, colTypes, "c")
	if err != nil {
		t.Fatalf("the applier lane refused a text[] value: %v", err)
	}
	if got != `["a",null,"b"]` {
		t.Errorf("text[] on the applier lane = %v; want [\"a\",null,\"b\"]", got)
	}
}

// TestArrayLeafForJSON_LoudRefusals covers every case the leaf policy
// refuses rather than guesses at. Each one used to be a silent base64
// substitution or a silently-stored rendering.
func TestArrayLeafForJSON_LoudRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		col     *ir.Column
		in      any
		wantSub string
	}{
		{
			"a byte leaf on a family that has no byte-shaped leaf",
			textArrayCol,
			[]any{[]byte("abc")},
			"only json/jsonb and bytea elements",
		},
		{
			"a byte leaf on a column with no resolvable element type",
			&ir.Column{Name: "j", Type: ir.JSON{}},
			[]any{[]byte("abc")},
			"not declared as an array",
		},
		{
			"an empty json document",
			jsonArrayCol,
			[]any{[]byte{}},
			"not a valid JSON document",
		},
		{
			"a json leaf of an unexpected Go type",
			jsonArrayCol,
			[]any{42},
			"IR-canonical leaf is the document's bytes",
		},
		{
			"a bytea leaf whose string is not the \\x-hex form",
			byteaArrayCol,
			[]any{`\xabc`},
			"even-hex",
		},
		{
			"a bytea leaf of an unexpected Go type",
			byteaArrayCol,
			[]any{42},
			"IR-canonical leaf is []byte",
		},
		{
			// Reachable, and previously invisible: the top-level
			// refuseUnrepresentableFloat type-asserts the WHOLE value, so a
			// NaN sitting inside an array slipped past it, the marshal
			// error was swallowed, and the raw []any surfaced downstream as
			// "unsupported value type []interface {}".
			"a non-finite float element",
			&ir.Column{Name: "f", Type: ir.Array{Element: ir.Float{}}},
			[]any{1.0, math.NaN()},
			"cannot represent",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := prepareValue(tc.in, tc.col)
			if err == nil {
				t.Fatalf("no refusal; prepareValue returned %#v", got)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("refusal %q does not contain %q", err.Error(), tc.wantSub)
			}
			// Naming the row is the point of a loud refusal — a message the
			// operator cannot act on is barely better than silence.
			if !strings.Contains(err.Error(), `"`+tc.col.Name+`"`) {
				t.Errorf("refusal %q does not name the column %q", err.Error(), tc.col.Name)
			}
			var coded *sluicecode.CodedError
			if !errors.As(err, &coded) || coded.Code != sluicecode.CodeValueUnrepresentable {
				t.Errorf("refusal is not a %s error: %v", sluicecode.CodeValueUnrepresentable, err)
			}
		})
	}
}

// TestConvertArrayLikeToJSON_MapArmHasNoByteLeaves holds the claim the
// map[string]any arm makes about not needing the leaf normalisation the
// []any arm runs.
//
// The arm's producer is the pgtrigger reader's decodeJSONBRow, a
// UseNumber json.Unmarshal into `any`. The load-bearing half of that
// argument is a property of encoding/json — that such a decode NEVER
// yields a []byte, for any input, including base64-looking strings —
// and that is what this pins. The other half, "decodeJSONBRow is the
// only producer", is a property of sluice's own call graph in a
// different package; it is stated at the arm and is an UNVERIFIED
// PREMISE here.
func TestConvertArrayLikeToJSON_MapArmHasNoByteLeaves(t *testing.T) {
	corpus := []string{
		`{"a":1,"b":"eyJhIjoxfQ==","c":null,"d":[1,"x",{"e":true}],"f":{"g":" "}}`,
		`{"deep":{"deeper":{"deepest":["\\xdeadbeef","3q2+7w=="]}}}`,
		`{"big":9223372036854775807,"neg":-0.0,"exp":1e308}`,
	}
	var walk func(t *testing.T, path string, v any)
	walk = func(t *testing.T, path string, v any) {
		t.Helper()
		switch shaped := v.(type) {
		case map[string]any:
			for k, e := range shaped {
				walk(t, path+"."+k, e)
			}
		case []any:
			for i, e := range shaped {
				walk(t, path+"["+strconv.Itoa(i)+"]", e)
			}
		case []byte:
			t.Errorf("a UseNumber json.Unmarshal produced a []byte at %s — the map arm's "+
				"no-normalisation claim no longer holds", path)
		}
	}
	for _, doc := range corpus {
		var out any
		dec := json.NewDecoder(strings.NewReader(doc))
		dec.UseNumber()
		if err := dec.Decode(&out); err != nil {
			t.Fatalf("decode %s: %v", doc, err)
		}
		walk(t, "$", out)
	}
}

// TestArrayLeafForJSON_NoMySQLReaderReconstructsAnArray holds the WART
// note's claim that the base64 (and the json document text) are one-way:
// no MySQL-sourced column is ever read back as an ir.Array, so nothing
// in sluice undoes either encoding.
//
// Derived rather than listed: the corpus is every `case` label of
// translateType's own data_type switch, read out of the source. A new
// MySQL type that mapped to ir.Array would fail this immediately.
func TestArrayLeafForJSON_NoMySQLReaderReconstructsAnArray(t *testing.T) {
	dataTypes := mysqlTranslateTypeCaseLabels(t)
	if len(dataTypes) < 25 {
		t.Fatalf("derived only %d data_type case labels from translateType; the derivation is broken and "+
			"this check would pass on a shrunken corpus", len(dataTypes))
	}
	for _, dt := range dataTypes {
		// ColumnType is fed the same token so the ENUM/SET/BIT arms have
		// something to parse; an arm that errors is fine here — an error
		// is not an ir.Array.
		got, err := translateType(columnMeta{DataType: dt, ColumnType: dt})
		if err != nil {
			continue
		}
		if _, isArray := got.(ir.Array); isArray {
			t.Errorf("mysql data_type %q now translates to ir.Array. The MySQL target's array encodings "+
				"(base64 for bytea leaves, document text for json leaves) are documented as ONE-WAY on "+
				"the strength of this never happening; a reader that produces arrays needs the reverse "+
				"policy written down before it ships", dt)
		}
	}
	// And the unknown-type arm, which is the other way a reader could
	// start producing arrays.
	if got, err := translateType(columnMeta{DataType: "no_such_type", ColumnType: "no_such_type"}); err == nil {
		if _, isArray := got.(ir.Array); isArray {
			t.Error("translateType's fallback arm produces ir.Array")
		}
	}
}

// mysqlTranslateTypeCaseLabels reads the `case` labels of the
// `switch c.DataType` inside translateType out of types.go.
func mysqlTranslateTypeCaseLabels(t *testing.T) []string {
	t.Helper()
	const src = "types.go"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, src, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", src, err)
	}
	var out []string
	for _, decl := range file.Decls {
		fn, isFunc := decl.(*ast.FuncDecl)
		if !isFunc || fn.Name.Name != "translateType" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			clause, isClause := n.(*ast.CaseClause)
			if !isClause {
				return true
			}
			for _, expr := range clause.List {
				lit, isLit := expr.(*ast.BasicLit)
				if !isLit || lit.Kind != token.STRING {
					continue
				}
				label, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", lit.Value, err)
				}
				out = append(out, label)
			}
			return true
		})
	}
	sort.Strings(out)
	return out
}

// prepareValueCallSiteRoster is the fail-by-default enumeration of every
// function in the MySQL engine that routes a row value through
// [prepareValue]. It exists because this package has THREE write cores
// and the project's most expensive recurring bug shape is a value rule
// wired into two of them (Bug 226's third write core, the grow gate, the
// bind-parameter clamp).
//
// # What it reaches, and the half it cannot
//
// It sees every function that CALLS prepareValue, so a core that stops
// calling it — or a new one that is wired up correctly — fails until
// someone updates this list and says why. It CANNOT see a brand-new
// write core that never calls prepareValue at all; nothing short of
// "every value binding goes through one door" as a type-level property
// could, and that is not how this package is built. Stated here rather
// than implied, because a gate whose coverage is narrower than its name
// stops the next reader from looking.
var prepareValueCallSiteRoster = map[string]string{
	"flattenArgs":         "batched INSERT (row_writer.go) — the default bulk-copy core",
	"encodeRowsTSV":       "LOAD DATA LOCAL INFILE (load_data_writer.go) — the fast bulk-copy core",
	"prepareApplierValue": "CDC apply (change_applier.go) — the continuous-sync core",
	"prepareValue":        "self-recursion, the ir.Domain unwrap",
}

// TestPrepareValueCallSiteRoster enumerates the MySQL write cores.
func TestPrepareValueCallSiteRoster(t *testing.T) {
	found := map[string]bool{}
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				if ident, isIdent := call.Fun.(*ast.Ident); isIdent && ident.Name == "prepareValue" {
					found[fn.Name.Name] = true
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// Anti-vacuity: a walk that stopped finding anything would agree with
	// an empty roster and pass.
	if len(found) < 3 {
		t.Fatalf("found only %d prepareValue call sites (%v); the AST walk is broken and this roster "+
			"would pass on nothing", len(found), sortedNames(found))
	}
	for name := range found {
		if _, declared := prepareValueCallSiteRoster[name]; !declared {
			t.Errorf("%s calls prepareValue but is not in prepareValueCallSiteRoster. Add it with the "+
				"write core it belongs to — this list is how a value rule that reached two cores and "+
				"missed the third becomes visible", name)
		}
	}
	for name, why := range prepareValueCallSiteRoster {
		if !found[name] {
			t.Errorf("prepareValueCallSiteRoster lists %q (%s) but nothing by that name calls prepareValue "+
				"any more — either the core was renamed (update the entry) or it stopped routing values "+
				"through the shared door (that is the bug this gate exists for)", name, why)
		}
		if strings.TrimSpace(why) == "" {
			t.Errorf("call site %q has no stated core", name)
		}
	}
}

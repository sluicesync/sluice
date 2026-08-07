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

// TestConvertArrayLikeToJSON_LiteralArmsTakeTheSameLeafPolicy is the
// ARRIVAL-SHAPE axis, which the first cut of the leaf policy did not
// have. The family half above asks "what does this element family
// become?"; this asks "does the answer depend on which SHAPE the value
// showed up in?", and the answer has to be no.
//
// [convertArrayLikeToJSON] recognises the same array three ways — the
// decoded []any the scan/pgoutput lanes hand it, the PG array TEXT
// LITERAL a `--type-override=col=jsonb` leaves as a string, and that
// same literal as []byte when the reader's bytes path produced it. The
// two literal arms used to go straight to a token marshal, so a
// `bytea[]` rendered `["3q0="]` down one lane and `["\\xdead"]` down the
// other: same declared family, same bytes, two encodings decided by
// arrival shape — on the family the policy exists to make deterministic.
//
// Both override spellings are exercised (Type=ir.Array, and
// Type=ir.JSON with the array parked in SourceColumnType) because
// [arrayElementType] consults them separately and the literal arms are
// reached mainly through the second.
func TestConvertArrayLikeToJSON_LiteralArmsTakeTheSameLeafPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		elem    ir.Type
		decoded []any // the []any lane's leaves
		literal string
		want    string
	}{
		{
			// The finding. PG renders a bytea inside an array literal as
			// the quoted `\\xdead`, which unescapes to the `\xdead` hex
			// text the trigger door also produces.
			"bytea",
			ir.Blob{},
			[]any{[]byte{0xde, 0xad}, []byte{0xbe, 0xef}},
			`{"\\xdead","\\xbeef"}`,
			`["3q0=","vu8="]`,
		},
		{
			// Already agreed before the fix — kept so a change that fixed
			// bytea by breaking json reports as itself.
			"json",
			ir.JSON{},
			[]any{[]byte(`{"a":1}`), []byte(`[1,2]`)},
			`{"{\"a\":1}","[1,2]"}`,
			`["{\"a\":1}","[1,2]"]`,
		},
		{
			// The string-leaf control: a family with no arm must not start
			// getting one because the tokens now walk the policy.
			"text",
			ir.Text{},
			[]any{"a", "b"},
			`{a,b}`,
			`["a","b"]`,
		},
		{
			// A NULL element has to survive the token walk as JSON null on
			// every arm, not become the four letters n-u-l-l.
			"bytea with a NULL element",
			ir.Blob{},
			[]any{[]byte{0xaa}, nil, []byte{0xbb}},
			`{"\\xaa",NULL,"\\xbb"}`,
			`["qg==",null,"uw=="]`,
		},
		{
			"empty",
			ir.Blob{},
			[]any{},
			`{}`,
			`[]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The two override spellings the writer actually sees.
			for _, col := range []*ir.Column{
				{Name: "c", Type: ir.Array{Element: tc.elem}},
				{Name: "c", Type: ir.JSON{}, SourceColumnType: ir.Array{Element: tc.elem}},
			} {
				fromDecoded, err := prepareValue(tc.decoded, col)
				if err != nil {
					t.Fatalf("[]any lane (%T column): %v", col.Type, err)
				}
				fromLiteral, err := prepareValue(tc.literal, col)
				if err != nil {
					t.Fatalf("literal-string lane (%T column): %v", col.Type, err)
				}
				fromLiteralBytes, err := prepareValue([]byte(tc.literal), col)
				if err != nil {
					t.Fatalf("literal-bytes lane (%T column): %v", col.Type, err)
				}
				for _, got := range []struct {
					lane string
					v    any
				}{
					{"[]any", fromDecoded},
					{"literal string", fromLiteral},
					{"literal []byte", fromLiteralBytes},
				} {
					if got.v != tc.want {
						t.Errorf("column type %T, %s lane: got %v; want %s — the leaf encoding must be "+
							"decided by the element FAMILY, never by which shape the value arrived in",
							col.Type, got.lane, got.v, tc.want)
					}
				}
			}
		})
	}
}

// TestConvertArrayLikeToJSON_LiteralArmRefusalIsLoud holds the other
// half of routing the literal arms through the leaf policy: a token that
// the declared family cannot carry is now a REFUSAL rather than a
// verbatim store, and a value that is not an array literal at all is
// still a shape verdict (fall through), not an error.
func TestConvertArrayLikeToJSON_LiteralArmRefusalIsLoud(t *testing.T) {
	// A bytea[] literal whose token is not the `\x`-hex form
	// `bytea_output = hex` produces. Pre-fix this stored the token's own
	// ASCII, disagreeing with the []any lane's base64.
	for _, v := range []any{`{"nothex"}`, []byte(`{"nothex"}`)} {
		got, err := prepareValue(v, byteaArrayCol)
		if err == nil {
			t.Errorf("a bytea[] literal with a non-hex token was accepted as %v", got)
			continue
		}
		if !strings.Contains(err.Error(), "even-hex") || !strings.Contains(err.Error(), `"y"`) {
			t.Errorf("refusal is not the named leaf one: %v", err)
		}
	}
	// And the disambiguation still works: a JSON OBJECT on a plain JSON
	// column is not an array literal, so the arm declines rather than
	// refusing, and prepareValue's next branch emits the bytes.
	got, err := prepareValue([]byte(`{"a":1}`), &ir.Column{Name: "j", Type: ir.JSON{}})
	if err != nil {
		t.Fatalf("a JSON object on a JSON column was refused: %v", err)
	}
	if got != `{"a":1}` {
		t.Errorf("JSON object on a JSON column = %v; want the document verbatim", got)
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

// TestArrayLeafForJSON_TriggerDoorApplierLaneIsUnguarded is a
// RESIDUAL pin, not a guard, and the name says so on purpose.
//
// The sibling above pins that the applier lane refuses a `[]byte` leaf.
// That refusal was documented as "the backstop that keeps the halves
// preflight does NOT reach (warm resume, the multi-database fan-out)
// loud rather than silent" — the sentence the whole preflight-scope
// argument rested on. It holds only for doors that PRODUCE a []byte.
// The pgtrigger door does not: its payload decode is a `UseNumber`
// json.Unmarshal with no column types, so its leaves are drawn from
// {string, json.Number, bool, nil, []any, map[string]any}. On that door
// the refusal cannot fire and a `bytea[]` diverges silently from what
// the cold copy stored.
//
// This test enumerates exactly what passes, with the cold copy's answer
// beside it, so the residual is a fact in the test suite rather than a
// sentence in a doc comment. It fails if a shape's rendering changes —
// including if someone closes the gap, which is the moment to re-read
// the false-positive argument below.
//
// # Why it is not closed by refusing
//
// On this lane the offending value is byte-identical to values that are
// correct today and common. `[]any{"\\xdead"}` is what a `bytea[]`
// produces AND what a jsonb column holding the DOCUMENT `["\\xdead"]`
// produces; `[]any{json.Number("1")}` is a `bytea[]`-free `int[]` CDC
// value that renders exactly as the cold copy does. A blanket refusal
// on "[]any with no resolvable element type" would break every jsonb
// array document over pgtrigger → MySQL and every string- and
// native-leaf array over CDC on both doors. Sniffing the CONTENT to
// tell them apart is what item 135 forbids one layer up. The cold-start
// preflight — which reads the SOURCE schema, whatever the source engine
// — is where this family is refused; see preflightArrayBytesLeafOnCDC.
func TestArrayLeafForJSON_TriggerDoorApplierLaneIsUnguarded(t *testing.T) {
	// Exactly the shape colTypesFor builds: the TARGET's own column, on
	// which MySQL renders every array family as one JSON column.
	colTypes := map[string]*ir.Column{"c": {Name: "c", Type: ir.JSON{Binary: true}}}
	num := func(s string) json.Number { return json.Number(s) }

	for _, tc := range []struct {
		name     string
		leaves   []any
		want     string
		coldCopy string // "" ⇒ the applier and the cold copy agree
	}{
		{
			// The divergence. PG's to_jsonb renders a bytea through
			// bytea_output, which the capture clause pins to hex.
			"bytea[] as the trigger door's hex string",
			[]any{`\xdead`},
			`["\\xdead"]`,
			`["3q0="]`,
		},
		{
			// Refused at pgtrigger.Setup, so this needs a stream set up
			// before that refusal existed — pinned because the value path
			// itself is silent about it.
			"json[] as the trigger door's nested document",
			[]any{map[string]any{"a": num("1")}},
			`[{"a":1}]`,
			`["{\"a\": 1}"]`,
		},
		{
			// The item-145 collision, arriving through this door: nesting
			// makes a `null` DOCUMENT and a SQL NULL element identical.
			"json[] null document beside a SQL NULL element",
			[]any{nil, nil, true},
			`[null,null,true]`,
			`["null",null,"true"]`,
		},
		// ---- and the families that AGREE, which is why a blanket
		// refusal here is not available. ----
		{"text[] agrees", []any{"a", nil, "b"}, `["a",null,"b"]`, ""},
		{"int[] agrees", []any{num("1"), num("2")}, `[1,2]`, ""},
		{"bool[] agrees", []any{true, false}, `[true,false]`, ""},
		{"2-D text[] agrees", []any{[]any{"a"}, []any{"b"}}, `[["a"],["b"]]`, ""},
		{
			// A jsonb column whose DOCUMENT is an array reaches this arm
			// byte-identically to a real array value. Refusing the arm
			// would break this, and it is correct today.
			"a jsonb array DOCUMENT, indistinguishable from a real array",
			[]any{num("1"), "x", nil},
			`[1,"x",null]`,
			"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := prepareApplierValue(tc.leaves, colTypes, "c")
			if err != nil {
				t.Fatalf("the applier lane refused a trigger-door leaf shape it accepts today "+
					"(if this is a deliberate new guard, re-read the false-positive argument in this "+
					"test's doc before keeping it): %v", err)
			}
			if got != tc.want {
				t.Errorf("applier lane = %v; want %s", got, tc.want)
			}
			if tc.coldCopy != "" && got == tc.coldCopy {
				t.Errorf("the applier lane now agrees with the cold copy (%s) for %q — the residual this "+
					"test records has been CLOSED. Update arrayLeafForJSON's SCOPE section and "+
					"preflightArrayBytesLeafOnCDC's, both of which currently say it is open",
					tc.coldCopy, tc.name)
			}
		})
	}

	// The other door, for contrast: a []byte leaf on the same lane IS
	// refused. Keeping both in one test is what makes the scope of the
	// backstop legible instead of implied.
	if _, err := prepareApplierValue([]any{[]byte{0xde, 0xad}}, colTypes, "c"); err == nil {
		t.Error("the pgoutput door's []byte leaf is no longer refused on the applier lane; the backstop " +
			"this test scopes has gone away entirely")
	}
}

// TestArrayLeafForJSON_CDCRefusalCarriesTheSyncRemedy holds the half of
// the CDC-lane refusal that an operator actually reads.
//
// `sync` cold-start and `schema add-table` run
// pipeline.preflightArrayBytesLeafOnCDC and refuse before any data
// moves, with remedies. A stream that resumes WARM reads no schema and
// so hits this refusal instead — mid-flight, per row. For `json[]` that
// is a corrupting family being stopped; for `bytea[]` it is a v0.115.0
// sync that worked, so the message has to say the family is now
// sync-unsupported and what to do, not just complain about a type.
func TestArrayLeafForJSON_CDCRefusalCarriesTheSyncRemedy(t *testing.T) {
	colTypes := map[string]*ir.Column{"c": {Name: "c", Type: ir.JSON{Binary: true}}}
	_, err := prepareApplierValue([]any{[]byte(`{"a":1}`)}, colTypes, "c")
	if err == nil {
		t.Fatal("no refusal on the applier lane")
	}
	for _, want := range []string{
		`column "c"`,          // names the row
		"--exclude-table",     // the scope remedy the preflight offers
		"sluice migrate",      // the copy-it-anyway remedy the preflight offers
		"NOT supported by co", // states the family is sync-unsupported
		"resumed WARM",        // says why the operator is seeing it late
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the warm-resume refusal is missing %q, so it does not read like the preflight's:\n%v",
				want, err)
		}
	}
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) || coded.Code != sluicecode.CodeValueUnrepresentable {
		t.Fatalf("refusal is not a %s error: %v", sluicecode.CodeValueUnrepresentable, err)
	}
	if !strings.Contains(coded.Hint, "--exclude-table") || !strings.Contains(coded.Hint, "sluice migrate") {
		t.Errorf("the coded HINT does not carry the two preflight remedies: %q", coded.Hint)
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
			"no resolvable element type",
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

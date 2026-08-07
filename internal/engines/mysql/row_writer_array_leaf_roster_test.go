// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every array element family the POSTGRES reader can produce must have a
// stated MySQL-target leaf verdict — the sibling of
// TestEveryDecodableArrayElementIsWritable, which its own header
// explicitly scopes out of MySQL ("It reaches the POSTGRES writer only.
// The MySQL target's array handling (convertArrayLikeToJSON) is a
// different mechanism").
//
// That scoping sentence was accurate and it was also the gap: nothing
// anywhere compared the decodable-family roster against the MySQL
// writer, so `json[]` could land on MySQL as a base64 token in every
// shape with every test green. This file is that comparison.
//
// # What it reaches, stated so the name cannot be read as broader
//
//   - The ROSTER is derived from the Postgres schema reader's
//     `builtinArrayElement` map, read out of its SOURCE (the map is
//     package-private to `postgres`, and importing that package from
//     this one to reach it is a coupling not worth buying). BOTH halves
//     of that map are derived — the UDT keys AND the `data_type` each
//     maps to — so a family added there fails this test until someone
//     states its MySQL verdict, and a family REPOINTED there (say
//     `"_bpchar": "character"` becoming `"text"`) fails it too. The
//     second half was the item-152 review finding: the keys were
//     derived and the ir.Type each key resolves to was hand-written and
//     bound to nothing, so the roster would have gone on probing
//     ir.Char while the reader produced ir.Text, green either way.
//     The pgoutput CDC door's `pgArrayElementOID` registry is
//     NOT read: `TestOIDToType_ArrayParity` in the postgres package
//     already holds the two doors to the same FAMILY set, and unlike
//     the Postgres-target roster this one dispatches on the family and
//     not on `ir.Time.WithTimeZone`, so the one per-door divergence in
//     the resolved ir.Type cannot change a verdict here. That is a
//     claim: TestArrayLeafForJSON_TimetzTakesTheSameVerdictAsTime holds
//     it.
//   - The CHECK is a real PROBE of [arrayLeafForJSON] with an
//     IR-canonical leaf for the family, compared against the declared
//     verdict AND the declared rendered value. It is a per-element
//     fidelity check on one leaf; it says nothing about dimensions,
//     NULL elements, empty arrays or whole-column NULLs — those are
//     TestConvertArrayLikeToJSON_ShapeMatrix here and
//     TestMigrate_PGToMySQL_JSONAndByteaArrays on a real MySQL server.
//     The two halves are complementary and neither substitutes for the
//     other.
//   - It reaches the MYSQL target's shared value door only. All three
//     MySQL write cores (batched INSERT via flattenArgs, LOAD DATA via
//     the writer's prepareValue call, and the CDC applier via
//     prepareApplierValue) funnel through [prepareValue], so one door
//     is the whole population — but that is a property of the current
//     code, and it is why TestPrepareValueCallSiteRoster exists next
//     door rather than being asserted here.
//   - It does NOT bind the last link: that PG `data_type` → this
//     `ir.Type`. That resolution is `postgres.translateType`, which is
//     package-private and unreachable from here by any means short of
//     moving this gate to a third package. So if `translateType`
//     started resolving `"character"` to something other than ir.Char,
//     this roster would keep probing ir.Char and stay green. The
//     Postgres-side sibling TestEveryDecodableArrayElementIsWritable
//     DOES call translateType and would see the change — as a
//     writability verdict, not as a divergence from what this file
//     declares. Stated rather than implied, because the two derived
//     halves above make it easy to read this roster as fully bound.

package mysql

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// mysqlArrayLeafVerdict is what the MySQL target does with one element
// family, and why. Three verdicts, and the distinction between them is
// the whole finding.
type mysqlArrayLeafVerdict struct {
	kind   string
	reason string
}

const (
	// leafPassthrough — Go's own JSON rendering of the IR-canonical leaf
	// is already faithful, so the leaf is handed to encoding/json
	// untouched.
	leafPassthrough = "passthrough"
	// leafDocumentText — the leaf is a JSON document; it is carried as
	// its exact TEXT inside a JSON string rather than nested as a JSON
	// value.
	leafDocumentText = "document-text"
	// leafBase64 — the leaf is raw bytes with no faithful JSON form;
	// base64, declared, and one-way.
	leafBase64 = "base64"
)

// mysqlArrayLeafRoster is the fail-by-default verdict map, keyed by the
// Postgres array UDT name (`builtinArrayElement`'s own keys). Each entry
// carries the IR element type the reader resolves that UDT to, an
// IR-canonical sample leaf, and the exact string the leaf must render to
// once inside the marshalled array.
var mysqlArrayLeafRoster = map[string]struct {
	elem    ir.Type
	verdict mysqlArrayLeafVerdict
	leaf    any
	wantRaw string
}{
	// ---- native families: JSON has a token for each. ----
	"_bool": {
		ir.Boolean{},
		mysqlArrayLeafVerdict{leafPassthrough, "JSON has a boolean token"},
		true, `true`,
	},
	"_int2": {
		ir.Integer{Width: 16},
		mysqlArrayLeafVerdict{leafPassthrough, "JSON numbers carry int64 exactly at these widths"},
		int64(42), `42`,
	},
	"_int4": {
		ir.Integer{Width: 32},
		mysqlArrayLeafVerdict{leafPassthrough, "JSON numbers carry int64 exactly at these widths"},
		int64(42), `42`,
	},
	"_int8": {
		ir.Integer{Width: 64},
		mysqlArrayLeafVerdict{
			leafPassthrough,
			"encoding/json renders an int64 as its exact decimal digits — it does NOT route through " +
				"float64 the way a json.Unmarshal into `any` would, so the 2^53 cliff that bit the " +
				"resume-cursor codec is absent here. Pinned by the 2^63-1 row of the family matrix.",
		},
		int64(9223372036854775807), `9223372036854775807`,
	},
	"_float4": {
		ir.Float{},
		mysqlArrayLeafVerdict{leafPassthrough, "JSON has a number token; NaN/±Inf refuse loudly at the marshal"},
		float64(1.5), `1.5`,
	},
	"_float8": {
		ir.Float{Precision: ir.FloatDouble},
		mysqlArrayLeafVerdict{leafPassthrough, "JSON has a number token; NaN/±Inf refuse loudly at the marshal"},
		float64(1.5), `1.5`,
	},

	// ---- string-shaped families: the IR leaf is already a string. ----
	"_numeric": {
		ir.Decimal{Precision: 10, Scale: 2},
		mysqlArrayLeafVerdict{
			leafPassthrough,
			"the IR leaf is the canonical numeric STRING (decodeDecimal), so it renders as a JSON " +
				"string — deliberately, since a JSON number would round-trip through float64 in most " +
				"consumers and lose the precision the column declared",
		},
		"12345678.99", `"12345678.99"`,
	},
	"_text":    {ir.Text{}, mysqlArrayLeafVerdict{leafPassthrough, "string leaf, JSON string"}, "abc", `"abc"`},
	"_varchar": {ir.Varchar{Length: 20}, mysqlArrayLeafVerdict{leafPassthrough, "string leaf, JSON string"}, "abc", `"abc"`},
	"_bpchar":  {ir.Char{Length: 5}, mysqlArrayLeafVerdict{leafPassthrough, "string leaf, JSON string (blank-padded by PG)"}, "ab   ", `"ab   "`},
	"_char":    {ir.Char{Length: 1}, mysqlArrayLeafVerdict{leafPassthrough, "string leaf, JSON string"}, "a", `"a"`},
	"_uuid":    {ir.UUID{}, mysqlArrayLeafVerdict{leafPassthrough, "canonical text leaf"}, "0d8f1c3e-0000-4000-8000-000000000001", `"0d8f1c3e-0000-4000-8000-000000000001"`},
	"_inet":    {ir.Inet{}, mysqlArrayLeafVerdict{leafPassthrough, "canonical text leaf"}, "192.168.0.1/32", `"192.168.0.1/32"`},
	"_cidr":    {ir.Cidr{}, mysqlArrayLeafVerdict{leafPassthrough, "canonical text leaf"}, "10.0.0.0/8", `"10.0.0.0/8"`},
	"_macaddr": {ir.Macaddr{}, mysqlArrayLeafVerdict{leafPassthrough, "canonical text leaf"}, "08:00:2b:01:02:03", `"08:00:2b:01:02:03"`},
	"_macaddr8": {
		ir.Macaddr{Width: ir.MacaddrEUI64},
		mysqlArrayLeafVerdict{leafPassthrough, "canonical text leaf"},
		"08:00:2b:01:02:03:04:05", `"08:00:2b:01:02:03:04:05"`,
	},

	// ---- temporal families. ----
	"_date": {
		ir.Date{},
		mysqlArrayLeafVerdict{
			leafPassthrough,
			"the IR leaf is a time.Time, which encoding/json renders as RFC 3339. That is a " +
				"REPRESENTATION choice, not a loss — but note it differs from the scalar path, which " +
				"binds a time.Time to a MySQL DATE/DATETIME column natively. Inside a JSON column " +
				"there is no temporal type to bind to.",
		},
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), `"2026-01-02T00:00:00Z"`,
	},
	"_timestamp": {
		ir.Timestamp{},
		mysqlArrayLeafVerdict{leafPassthrough, "time.Time leaf, RFC 3339 — see _date"},
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), `"2026-01-02T03:04:05Z"`,
	},
	"_timestamptz": {
		ir.Timestamp{WithTimeZone: true},
		mysqlArrayLeafVerdict{leafPassthrough, "time.Time leaf, RFC 3339 with the offset — see _date"},
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), `"2026-01-02T03:04:05Z"`,
	},
	"_time": {
		ir.Time{},
		mysqlArrayLeafVerdict{leafPassthrough, "the IR leaf is the time-of-day STRING (decodeTimeAsString)"},
		"12:34:56", `"12:34:56"`,
	},
	"_timetz": {
		ir.Time{WithTimeZone: true},
		mysqlArrayLeafVerdict{
			leafPassthrough,
			"string leaf, carried WITH its zone offset. This DIVERGES from the scalar timetz path, " +
				"which strips the offset because a MySQL TIME column rejects it (catalog Bug 71) — " +
				"inside a JSON column there is nothing to reject it, so the zone is kept and the value " +
				"is strictly more faithful than the scalar one. Deliberate; not a leak of the Bug 71 " +
				"policy. NOTE the Postgres TARGET refuses this family outright (no faithful binary " +
				"array leaf); MySQL can carry it precisely because its target is text.",
		},
		"12:34:56+02", `"12:34:56+02"`,
	},

	// ---- the two []byte-leaf families: the finding. ----
	"_bytea": {
		ir.Blob{},
		mysqlArrayLeafVerdict{
			leafBase64,
			"raw bytes have no faithful JSON form. base64 is declared, reversible, and one-way — " +
				"nothing in sluice reconstructs an ir.Array from a MySQL source. See the WART note on " +
				"arrayLeafForJSON.",
		},
		[]byte{0xde, 0xad},
		`"3q0="`,
	},
	"_json": {
		ir.JSON{},
		mysqlArrayLeafVerdict{
			leafDocumentText,
			"the document's exact text inside a JSON string. NESTING it as a JSON value would make a " +
				"`null` document indistinguishable from a SQL NULL element and an array-valued " +
				"document indistinguishable from a nested dimension — item 145's collision, on this " +
				"target. Pre-fix this was base64: silent loss.",
		},
		[]byte(`{"a":1}`), `"{\"a\":1}"`,
	},
	"_jsonb": {
		ir.JSON{Binary: true},
		mysqlArrayLeafVerdict{leafDocumentText, "identical to _json; the reader resolves jsonb to ir.JSON{Binary:true}"},
		[]byte(`{"a": 1}`), `"{\"a\": 1}"`,
	},
}

// mysqlArrayLeafSourceDataType anchors each roster entry's hand-written
// `elem` to the PG `data_type` it was written against — the second half
// of `builtinArrayElement`, which the key derivation alone does not see.
//
// It is deliberately a COPY of that map rather than a derivation of it,
// and the copy is the point: the test asserts the two are identical in
// both directions, so repointing a UDT at a different `data_type` in the
// reader fails here and makes whoever did it revisit the `elem` and the
// verdict beside it. Without this the reader could start resolving
// `_bpchar` through `text` and the roster would keep probing ir.Char,
// green either way. What it still does NOT bind is `data_type` →
// `ir.Type`; see the "does NOT bind the last link" bullet in the header.
var mysqlArrayLeafSourceDataType = map[string]string{
	"_bool":        "boolean",
	"_int2":        "smallint",
	"_int4":        "integer",
	"_int8":        "bigint",
	"_float4":      "real",
	"_float8":      "double precision",
	"_numeric":     "numeric",
	"_text":        "text",
	"_varchar":     "character varying",
	"_bpchar":      "character",
	"_char":        "character",
	"_bytea":       "bytea",
	"_date":        "date",
	"_time":        "time without time zone",
	"_timetz":      "time with time zone",
	"_timestamp":   "timestamp without time zone",
	"_timestamptz": "timestamp with time zone",
	"_json":        "json",
	"_jsonb":       "jsonb",
	"_uuid":        "uuid",
	"_inet":        "inet",
	"_cidr":        "cidr",
	"_macaddr":     "macaddr",
	"_macaddr8":    "macaddr8",
}

// TestEveryDecodableArrayElementHasAMySQLLeafVerdict is the divergence
// map: derived roster in, probed verdict out.
func TestEveryDecodableArrayElementHasAMySQLLeafVerdict(t *testing.T) {
	derivedTypes := postgresBuiltinArrayElement(t)
	derived := sortedKeysOf(derivedTypes)

	// Anti-vacuity on the INPUT: the roster is derived, so a derivation
	// that silently broke would compare an empty set against an empty set
	// and pass. The registry carries 24 entries today.
	if len(derived) < 20 {
		t.Fatalf("derived only %d array element UDTs from the postgres schema reader; the derivation is "+
			"broken and this roster would pass on a shrunken set", len(derived))
	}

	// The value half. Both directions, so a UDT repointed at a different
	// `data_type` — the change the key derivation cannot see — reports as
	// itself rather than as silence.
	for udt, dataType := range derivedTypes {
		declared, ok := mysqlArrayLeafSourceDataType[udt]
		if !ok {
			t.Errorf("the postgres reader maps array UDT %q to data_type %q, but "+
				"mysqlArrayLeafSourceDataType does not list it — add it, and check that the ir.Type in "+
				"mysqlArrayLeafRoster is what that data_type actually resolves to", udt, dataType)
			continue
		}
		if declared != dataType {
			t.Errorf("the postgres reader now maps array UDT %q to data_type %q, not %q. The MySQL roster's "+
				"element ir.Type for that family was written for %q and is bound to nothing else, so it "+
				"would keep probing the old family with this test green. Re-derive the entry: update "+
				"mysqlArrayLeafSourceDataType, and update mysqlArrayLeafRoster[%q].elem, .leaf and "+
				".wantRaw to whatever %q resolves to", udt, dataType, declared, declared, udt, dataType)
		}
	}
	for udt := range mysqlArrayLeafSourceDataType {
		if _, ok := derivedTypes[udt]; !ok {
			t.Errorf("mysqlArrayLeafSourceDataType declares %q, which builtinArrayElement no longer "+
				"lists — remove the stale entry", udt)
		}
	}

	for _, udt := range derived {
		entry, declared := mysqlArrayLeafRoster[udt]
		if !declared {
			t.Errorf("array element family %q DECODES on the postgres reader but has no MySQL-target leaf "+
				"verdict.\n\n"+
				"MySQL has no array type, so an ir.Array lands on a JSON column and every leaf is "+
				"rendered by encoding/json. That is faithful for scalar-shaped leaves and SILENT LOSS "+
				"for a []byte one (Go renders []byte as base64 — the json[] defect). Add the family to "+
				"mysqlArrayLeafRoster with its verdict, its IR-canonical sample leaf and the exact "+
				"rendering, and add an arm to arrayLeafForJSON if passthrough is not faithful.", udt)
			continue
		}
		if entry.verdict.reason == "" {
			t.Errorf("family %q has a verdict with no reason; an unexplained encoding choice is "+
				"indistinguishable from an oversight", udt)
		}

		// The probe: one IR-canonical leaf through the real leaf policy,
		// then through the real marshaller, compared against the declared
		// rendering. Marshalling a one-element array and stripping the
		// brackets is deliberate — it exercises the same encoder path the
		// writer uses rather than a hand-rolled equivalent.
		got, err := arrayLeafForJSON(entry.leaf, entry.elem, &ir.Column{Name: "c"})
		if err != nil {
			t.Errorf("family %q: arrayLeafForJSON refused an IR-canonical leaf: %v", udt, err)
			continue
		}
		raw, err := json.Marshal([]any{got})
		if err != nil {
			t.Errorf("family %q: marshal: %v", udt, err)
			continue
		}
		rendered := string(raw[1 : len(raw)-1])
		if rendered != entry.wantRaw {
			t.Errorf("family %q (%s) rendered %s; want %s", udt, entry.verdict.kind, rendered, entry.wantRaw)
		}

		// And the verdict itself has to match what the code DID, so a
		// family whose arm changed cannot keep a stale label.
		switch entry.verdict.kind {
		case leafPassthrough:
			if fmt.Sprintf("%T", got) != fmt.Sprintf("%T", entry.leaf) {
				t.Errorf("family %q is labelled %s but the leaf changed type %T → %T",
					udt, leafPassthrough, entry.leaf, got)
			}
		case leafDocumentText, leafBase64:
			if _, isString := got.(string); !isString {
				t.Errorf("family %q is labelled %s but the leaf came back as %T, not a string",
					udt, entry.verdict.kind, got)
			}
		default:
			t.Errorf("family %q carries the unknown verdict kind %q", udt, entry.verdict.kind)
		}
	}

	// The other direction: a roster entry for a family the reader no
	// longer produces overstates the surface and hides a removal.
	inDerived := map[string]bool{}
	for _, udt := range derived {
		inDerived[udt] = true
	}
	for udt := range mysqlArrayLeafRoster {
		if !inDerived[udt] {
			t.Errorf("mysqlArrayLeafRoster declares %q, which the postgres reader's builtinArrayElement no "+
				"longer lists — remove the stale entry", udt)
		}
	}
}

// TestArrayLeafForJSON_TimetzTakesTheSameVerdictAsTime holds the claim
// the roster's header makes about NOT reading the pgoutput door: that
// door resolves `timetz` to ir.Time with WithTimeZone UNSET while the
// schema-read door sets it, and this roster would be narrower than its
// name if that flag could change a verdict. It cannot, because
// arrayLeafForJSON does not dispatch on it — which is a property of the
// code today, so it gets a check rather than a sentence.
func TestArrayLeafForJSON_TimetzTakesTheSameVerdictAsTime(t *testing.T) {
	col := &ir.Column{Name: "c"}
	const leaf = "12:34:56+02"
	withZone, err := arrayLeafForJSON(leaf, ir.Time{WithTimeZone: true}, col)
	if err != nil {
		t.Fatalf("timetz leaf: %v", err)
	}
	withoutZone, err := arrayLeafForJSON(leaf, ir.Time{}, col)
	if err != nil {
		t.Fatalf("time leaf: %v", err)
	}
	if withZone != withoutZone {
		t.Errorf("arrayLeafForJSON now dispatches on ir.Time.WithTimeZone (%v vs %v) — the roster derives "+
			"from the schema-read door ONLY, so the pgoutput door's zone-unset resolution is no longer "+
			"covered; derive from both doors or add the CDC-door family explicitly",
			withZone, withoutZone)
	}
}

// postgresBuiltinArrayElement reads the postgres schema reader's
// `builtinArrayElement` map out of its source file — BOTH the UDT keys
// and the `data_type` each maps to. The map is package-private to
// `postgres`; parsing the source is what keeps this roster DERIVED (a
// family added or repointed there fails this test) without buying an
// import edge from mysql to postgres.
func postgresBuiltinArrayElement(t *testing.T) map[string]string {
	t.Helper()
	const src = "../postgres/schema_reader.go"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, src, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", src, err)
	}
	out := map[string]string{}
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || spec.Names[0].Name != "builtinArrayElement" {
			return true
		}
		if len(spec.Values) != 1 {
			return false
		}
		lit, isComposite := spec.Values[0].(*ast.CompositeLit)
		if !isComposite {
			return false
		}
		for _, elt := range lit.Elts {
			kv, isKV := elt.(*ast.KeyValueExpr)
			if !isKV {
				continue
			}
			key, isLit := kv.Key.(*ast.BasicLit)
			if !isLit || key.Kind != token.STRING {
				continue
			}
			udt, err := strconv.Unquote(key.Value)
			if err != nil {
				t.Fatalf("unquote %s: %v", key.Value, err)
			}
			val, isLit := kv.Value.(*ast.BasicLit)
			if !isLit || val.Kind != token.STRING {
				t.Fatalf("builtinArrayElement[%q] is no longer a string literal; the value derivation "+
					"below would silently read nothing", udt)
			}
			dataType, err := strconv.Unquote(val.Value)
			if err != nil {
				t.Fatalf("unquote %s: %v", val.Value, err)
			}
			out[udt] = dataType
		}
		return false
	})
	if len(out) == 0 {
		t.Fatalf("found no builtinArrayElement entries in %s — the map was renamed or restructured and "+
			"this roster is deriving from nothing", src)
	}
	return out
}

// sortedKeysOf returns a map's keys in lexicographic order, so the
// roster walk is deterministic and a failure names families in a stable
// order across runs.
func sortedKeysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

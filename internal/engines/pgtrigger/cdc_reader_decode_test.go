// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for decodeJSONBRow's payload-leaf normalization (RDS
// validation F3, 2026-07-16): array elements must follow the SAME
// loss-free-only rule as scalars, recursively through nested (multi-
// dim) arrays. Value-level fidelity through the writer's type-aware
// re-parse is pinned separately: per-family unit matrix in
// postgres/row_writer_unit_test.go, real-path integration matrix in
// cdc_apply_array_families_integration_test.go.

package pgtrigger

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestDecodeJSONBRow_ArrayLeavesFollowScalarRule pins the recursion:
// integer numerics inside arrays become int64 (the IR-canonical form
// the target writer's Integer arm requires), non-integer numerics stay
// json.Number (precision-deferred to the type-aware layer), strings and
// bools pass through, NULL elements stay nil — at every nesting depth.
func TestDecodeJSONBRow_ArrayLeavesFollowScalarRule(t *testing.T) {
	row, err := decodeJSONBRow(`{
		"ints":   [1, -9223372036854775808, null],
		"floats": [1.5, 2, "Infinity", "-Infinity", "NaN", null],
		"nested": [[1, 2.5], ["a", null]]
	}`)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	wantInts := []any{int64(1), int64(-9223372036854775808), nil}
	if got := row["ints"]; !reflect.DeepEqual(got, wantInts) {
		t.Errorf("ints = %#v; want %#v (integer leaves normalized to int64 at every element)", got, wantInts)
	}

	// Non-integer numbers stay json.Number; to_jsonb's non-finite float
	// spellings stay strings (the writer's Float arm owns mapping them —
	// mapping here would corrupt a text[] holding the literal word).
	wantFloats := []any{json.Number("1.5"), int64(2), "Infinity", "-Infinity", "NaN", nil}
	if got := row["floats"]; !reflect.DeepEqual(got, wantFloats) {
		t.Errorf("floats = %#v; want %#v", got, wantFloats)
	}

	wantNested := []any{[]any{int64(1), json.Number("2.5")}, []any{"a", nil}}
	if got := row["nested"]; !reflect.DeepEqual(got, wantNested) {
		t.Errorf("nested = %#v; want %#v (rule must recurse through multi-dim levels)", got, wantNested)
	}
}

// TestDecodeJSONBRow_NegativeZeroStaysNumber pins the "-0" boundary of
// the integer rule: int64(0) would silently drop a float sign bit, so
// the token stays json.Number, scalar and array element alike. This is
// a DEFENSIVE pin — PG's to_jsonb capture can never actually emit -0
// (it stores numbers as numeric; see the engine.go negative-zero wart
// and TestCDCApply_FloatNegativeZeroCaptureNormalization) — but the
// decode layer must not be the one destroying a sign if the capture
// format ever becomes sign-faithful.
func TestDecodeJSONBRow_NegativeZeroStaysNumber(t *testing.T) {
	row, err := decodeJSONBRow(`{"f": -0, "fs": [-0, 0]}`)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, want := row["f"], json.Number("-0"); !reflect.DeepEqual(got, want) {
		t.Errorf("scalar -0 = %#v (%T); want json.Number(\"-0\") — int64 would lose the sign bit", got, got)
	}
	wantArr := []any{json.Number("-0"), int64(0)}
	if got := row["fs"]; !reflect.DeepEqual(got, wantArr) {
		t.Errorf("array -0 = %#v; want %#v", got, wantArr)
	}
}

// TestDecodeJSONBRow_JSONBObjectsNotDescended pins the deliberate
// boundary: a jsonb column's OBJECT document is re-marshaled verbatim
// on apply (encoding/json emits json.Number byte-identically), so its
// leaves are left untouched — while a jsonb column whose top-level
// value is a JSON array is indistinguishable from an array column and
// IS normalized, which is harmless for the same marshal-identity
// reason.
func TestDecodeJSONBRow_JSONBObjectsNotDescended(t *testing.T) {
	row, err := decodeJSONBRow(`{"doc": {"k": 1, "tags": [1, 2.5]}}`)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	doc, ok := row["doc"].(map[string]any)
	if !ok {
		t.Fatalf("doc = %T; want map[string]any", row["doc"])
	}
	if got, want := doc["k"], json.Number("1"); !reflect.DeepEqual(got, want) {
		t.Errorf("doc.k = %#v (%T); want untouched json.Number(\"1\")", got, got)
	}
	wantTags := []any{json.Number("1"), json.Number("2.5")}
	if got := doc["tags"]; !reflect.DeepEqual(got, wantTags) {
		t.Errorf("doc.tags = %#v; want untouched %#v", got, wantTags)
	}
}

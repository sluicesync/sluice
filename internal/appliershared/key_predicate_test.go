// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"maps"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestKeyPredicate pins the narrowing every UPDATE/DELETE WHERE goes through
// (Bug 339): to the key (plus the routing columns) when the image names every
// non-generated key column, and the image untouched otherwise.
func TestKeyPredicate(t *testing.T) {
	full := ir.Row{"id": int64(1), "tenant": "t1", "v": "old", "added": "DEFAULT", "nul": nil}
	gen := map[string]*ir.Column{"g": {Name: "g", GeneratedExpr: "id + 1"}}
	for _, tc := range []struct {
		name     string
		before   ir.Row
		key      []string
		colTypes map[string]*ir.Column
		keep     []string
		want     ir.Row
	}{
		{
			name: "full image narrows to a single-column key", before: full, key: []string{"id"},
			want: ir.Row{"id": int64(1)},
		},
		{
			name: "composite key", before: full, key: []string{"tenant", "id"},
			want: ir.Row{"id": int64(1), "tenant": "t1"},
		},
		{
			name: "routing columns are kept", before: full, key: []string{"id"}, keep: []string{"tenant", "absent"},
			want: ir.Row{"id": int64(1), "tenant": "t1"},
		},
		{
			name: "a NULL routing value is kept as NULL", before: full, key: []string{"id"}, keep: []string{"nul"},
			want: ir.Row{"id": int64(1), "nul": nil},
		},
		{
			name: "already narrowed is unchanged", before: ir.Row{"id": int64(1)}, key: []string{"id"},
			want: ir.Row{"id": int64(1)},
		},
		{name: "keyless target keeps the whole row", before: full, key: nil, want: full},
		{name: "image without every key column is kept whole", before: full, key: []string{"id", "other"}, want: full},
		{
			name:   "a generated key column keeps the image whole",
			before: ir.Row{"id": int64(1), "g": int64(2), "v": "x"}, key: []string{"id", "g"}, colTypes: gen,
			want: ir.Row{"id": int64(1), "g": int64(2), "v": "x"},
		},
		{name: "empty image", before: ir.Row{}, key: []string{"id"}, want: ir.Row{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := KeyPredicate(tc.before, tc.key, tc.colTypes, tc.keep)
			if !maps.EqualFunc(got, tc.want, func(a, b any) bool { return a == b }) {
				t.Errorf("KeyPredicate = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestKeyPredicate_DoesNotMutateTheChange pins that the narrowing copies: the
// change's own before-image is read again after the WHERE is built (redaction,
// the shard-key change check, apply marks), so it must keep every column.
func TestKeyPredicate_DoesNotMutateTheChange(t *testing.T) {
	before := ir.Row{"id": int64(1), "v": "old"}
	_ = KeyPredicate(before, []string{"id"}, nil, nil)
	if len(before) != 2 {
		t.Fatalf("KeyPredicate mutated its input: %v", before)
	}
}

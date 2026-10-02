// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// The pipeline finds the probe by type assertion; a renamed method would
// drop the vtgate arm of the preflight silently.
var _ migcore.ShardKeyUpsertProber = (*RowWriter)(nil)

// TestVindexUpsertMismatch_EveryShape grades the GC-41 (e) (a) decision over
// every key/vindex shape: a vindex inside the key passes, one outside it (or
// partly outside, or on a keyless table) is named.
func TestVindexUpsertMismatch_EveryShape(t *testing.T) {
	cases := []struct {
		name        string
		pk, vindex  []string
		wantOutside string // "" = passes
	}{
		{name: "primary vindex is the primary key", pk: []string{"id"}, vindex: []string{"id"}},
		{name: "vindex is one member of a composite key", pk: []string{"tenant", "id"}, vindex: []string{"tenant"}},
		{name: "multi-column vindex inside the key", pk: []string{"a", "b", "c"}, vindex: []string{"a", "b"}},
		{name: "names fold", pk: []string{"ID"}, vindex: []string{"id"}},
		{name: "primary vindex on a non-key column", pk: []string{"id"}, vindex: []string{"cust"}, wantOutside: "cust"},
		{name: "secondary vindex on a non-key column", pk: []string{"id"}, vindex: []string{"id", "email"}, wantOutside: "email"},
		{name: "multi-column vindex partly outside", pk: []string{"a"}, vindex: []string{"a", "b"}, wantOutside: "b"},
		{name: "keyless table", vindex: []string{"id"}, wantOutside: "id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := vindexUpsertMismatch("ks", "t", c.pk, c.vindex)
			if c.wantOutside == "" {
				if got != "" {
					t.Fatalf("a safe shape was flagged: %s", got)
				}
				return
			}
			if got == "" {
				t.Fatal("a vindex column outside the key passed; every upsert would be refused, and dropping the column would duplicate a moved row")
			}
			for _, want := range []string{"ks.t", "(" + c.wantOutside + ")", "VT12001"} {
				if !strings.Contains(got, want) {
					t.Errorf("detail does not carry %q: %s", want, got)
				}
			}
		})
	}
}

// A writer that is not vtgate-fronted (nil vtgateCfg: every vanilla MySQL and
// MariaDB target, and a bare-struct writer) pays nothing and never refuses.
func TestShardKeyUpsertMismatch_NoOpOffVtgate(t *testing.T) {
	w := &RowWriter{schema: "s"}
	detail, err := w.ShardKeyUpsertMismatch(t.Context(), []*ir.Table{{Name: "t"}})
	if err != nil || detail != "" {
		t.Fatalf("got (%q, %v), want a no-op", detail, err)
	}
}

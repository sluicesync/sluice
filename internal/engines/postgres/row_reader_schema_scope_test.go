// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestEffectiveSchema_RefusesATableFromAnotherSchema is the point-of-harm half
// of ADR-0075's second premise (2026-08-08 invariant sweep).
//
// ADR-0075 argues a multi-schema cold start is consistent because one exported
// snapshot spans every schema. True of PostgreSQL — and insufficient, because
// only a reader with qualifyBySchema set actually READS across schemas. The
// snapshot-importer readers minted for the ADR-0079 parallel lane do not, so
// handing one a table from another schema used to return that table's NAME
// qualified by the READER's schema: `public."t"` instead of `tenant_b."t"`.
// With the canonical multi-schema shape — the same table name per tenant
// schema — that resolves, returns rows, and is wrong.
//
// The matrix is the full 2x3: {spanning, non-spanning} x {table names this
// reader's schema, table names another, table names none}. Pinning only the
// refusal would leave the over-refusal direction — the one that would break
// every single-schema copy in the tree — unmeasured.
func TestEffectiveSchema_RefusesATableFromAnotherSchema(t *testing.T) {
	cases := []struct {
		name        string
		qualify     bool
		readerSchma string
		tableSchema string
		want        string
		wantRefuse  bool
	}{
		{
			name: "non-spanning/same schema", readerSchma: "public", tableSchema: "public",
			want: "public",
		},
		{
			name: "non-spanning/empty table schema", readerSchma: "public", tableSchema: "",
			want: "public",
		},
		{
			// The defect cell. Pre-fix this returned "public" and the SELECT
			// silently read public."t".
			name: "non-spanning/foreign schema", readerSchma: "public", tableSchema: "tenant_b",
			wantRefuse: true,
		},
		{
			name: "spanning/same schema", qualify: true, readerSchma: "public", tableSchema: "public",
			want: "public",
		},
		{
			name: "spanning/foreign schema", qualify: true, readerSchma: "public", tableSchema: "tenant_b",
			want: "tenant_b",
		},
		{
			// A spanning reader handed an unstamped table falls back to its
			// own bound schema — the documented defensive arm, kept pinned so
			// the refusal above cannot creep into it.
			name: "spanning/empty table schema", qualify: true, readerSchma: "public", tableSchema: "",
			want: "public",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &RowReader{schema: c.readerSchma, qualifyBySchema: c.qualify}
			got, err := r.effectiveSchema(&ir.Table{Schema: c.tableSchema, Name: "t"})
			switch {
			case c.wantRefuse && err == nil:
				t.Fatalf("effectiveSchema returned %q with no error; want a refusal — a single-schema "+
					"reader that silently substitutes its own schema is the ADR-0075 divergence", got)
			case c.wantRefuse:
				if !errors.Is(err, errSchemaEscape) {
					t.Fatalf("error = %v; want it to wrap errSchemaEscape so callers can match structurally", err)
				}
			case err != nil:
				t.Fatalf("effectiveSchema: unexpected refusal %v (this cell is a working single-schema copy; "+
					"refusing it would break every non-spanning read in the tree)", err)
			case got != c.want:
				t.Fatalf("effectiveSchema = %q; want %q", got, c.want)
			}
		})
	}
}

// TestReadRows_RefusesATableFromAnotherSchema pins the refusal at the surface
// an orchestrator actually calls, not only at the helper. effectiveSchema is
// shared by ReadRows / CountRows / RangeBounds / EstimateRowCount /
// SampleKeysetBoundaries; ReadRows is the one that would have COPIED the wrong
// rows, so it is the one graded end to end. No querier is needed — the refusal
// resolves before any statement is issued, which is also what makes it safe on
// a pinned snapshot connection.
func TestReadRows_RefusesATableFromAnotherSchema(t *testing.T) {
	r := &RowReader{schema: "public"}
	table := &ir.Table{
		Schema:  "tenant_b",
		Name:    "t",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}
	rows, err := r.ReadRows(t.Context(), table)
	if err == nil {
		t.Fatal("ReadRows accepted a table from another schema; want a refusal before any query is issued")
	}
	if rows != nil {
		t.Fatal("ReadRows returned a channel alongside its error")
	}
	if !errors.Is(err, errSchemaEscape) {
		t.Fatalf("ReadRows error = %v; want errSchemaEscape", err)
	}
}

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestVerifyIndexes_MissingRaisesCode pins the loud-failure safety net: when
// the index-build path silently no-op'd (the target has none of the secondary
// indexes it was supposed to build), VerifyIndexes returns a
// SLUICE-E-INDEX-MISSING refusal — NOT a clean exit-0 — naming the missing
// table.index list. This is the net that makes the whole silent-index-loss
// CLASS un-shippable regardless of which build path ran.
func TestVerifyIndexes_MissingRaisesCode(t *testing.T) {
	rec := &indexRecorder{exists: false} // probe says every index is ABSENT
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}

	schema := &ir.Schema{Tables: []*ir.Table{indexedTable("orders"), indexedTable("users")}}
	err := w.VerifyIndexes(context.Background(), schema)
	if err == nil {
		t.Fatal("VerifyIndexes must FAIL when expected indexes are missing; got nil (silent loss)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("VerifyIndexes error must carry a sluicecode; got %v", err)
	}
	if ce.Code != sluicecode.CodeIndexMissing {
		t.Errorf("code = %q; want %q", ce.Code, sluicecode.CodeIndexMissing)
	}
	// The message must NAME the missing rows (loud-failure discipline).
	for _, want := range []string{"orders.orders_v_idx", "users.users_v_idx"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name the missing index %q; got %q", want, err.Error())
		}
	}
}

// TestVerifyIndexes_HealthyNoError pins the happy path: when every expected
// index exists on the target, VerifyIndexes returns nil (no false positive).
func TestVerifyIndexes_HealthyNoError(t *testing.T) {
	rec := &indexRecorder{exists: true} // probe says every index is PRESENT
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}

	schema := &ir.Schema{Tables: []*ir.Table{indexedTable("orders"), indexedTable("users")}}
	if err := w.VerifyIndexes(context.Background(), schema); err != nil {
		t.Fatalf("VerifyIndexes on a fully-indexed target must pass; got %v", err)
	}
}

// TestVerifyIndexes_SkipsInlineAndNoIndexTables pins that the verifier uses
// the SAME eligible set the build targeted: a table with only inline-emitted
// or zero secondary indexes contributes no expected index, so an all-absent
// probe still passes (no false flag on indexes the index phase never builds).
func TestVerifyIndexes_SkipsInlineAndNoIndexTables(t *testing.T) {
	rec := &indexRecorder{exists: false} // probe says ABSENT — must not matter here
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}

	// A table carrying only a PK and no secondary index → no expected index.
	plain := &ir.Table{
		Name:       "p0",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: pk(),
	}
	schema := &ir.Schema{Tables: []*ir.Table{plain}}
	if err := w.VerifyIndexes(context.Background(), schema); err != nil {
		t.Fatalf("VerifyIndexes must not flag a table with no buildable secondary index; got %v", err)
	}
}

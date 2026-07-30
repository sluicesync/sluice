// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fakeFKWriter is a SchemaWriter that also implements the item-109 orphan-scan
// surfaces, letting the driver run without a database.
type fakeFKWriter struct {
	unvalidated []ir.UnvalidatedForeignKey

	// scan is the canned answer ScanForeignKeyOrphan returns.
	scanViolating []any
	scanFound     bool
	scanErr       error
	scanCalls     int

	dropped []string // "table.fk" of each DropForeignKey call
}

func (f *fakeFKWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error { return nil }

func (f *fakeFKWriter) CreateIndexes(context.Context, *ir.Schema) error { return nil }

func (f *fakeFKWriter) CreateConstraints(context.Context, *ir.Schema) error { return nil }

func (f *fakeFKWriter) SyncIdentitySequences(context.Context, *ir.Schema) error { return nil }

func (f *fakeFKWriter) CreateViews(context.Context, *ir.Schema) error { return nil }

func (f *fakeFKWriter) UnvalidatedForeignKeys() []ir.UnvalidatedForeignKey { return f.unvalidated }

func (f *fakeFKWriter) ScanForeignKeyOrphan(_ context.Context, _ *ir.Table, _ *ir.ForeignKey, _, _ []any) (violatingKey []any, found bool, err error) {
	f.scanCalls++
	return f.scanViolating, f.scanFound, f.scanErr
}

func (f *fakeFKWriter) DropForeignKey(_ context.Context, childTable, fkName string) error {
	f.dropped = append(f.dropped, childTable+"."+fkName)
	return nil
}

func childSchema() (*ir.Schema, *ir.ForeignKey) {
	fk := &ir.ForeignKey{Name: "events_customer_fk", Columns: []string{"customer_id"}, ReferencedTable: "customers", ReferencedColumns: []string{"id"}}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:        "events",
		Columns:     []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "customer_id", Type: ir.Integer{Width: 64}}},
		PrimaryKey:  &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}},
		ForeignKeys: []*ir.ForeignKey{fk},
	}}}
	return schema, fk
}

// dummySampler implements nothing the boundary machinery needs, so the driver
// falls back to a single unbounded chunk — enough to exercise the scan/drop
// path without a database.
type dummySampler struct{}

// TestValidateUnvalidatedForeignKeys_NoOp: a writer with nothing unvalidated,
// and a writer without the reporter surface, both do nothing.
func TestValidateUnvalidatedForeignKeys_NoOp(t *testing.T) {
	schema, _ := childSchema()

	w := &fakeFKWriter{} // empty unvalidated set
	if err := ValidateUnvalidatedForeignKeys(context.Background(), w, schema, dummySampler{}); err != nil {
		t.Fatalf("empty unvalidated set must be a no-op; got %v", err)
	}
	if w.scanCalls != 0 {
		t.Fatalf("no scan should run for an empty set; got %d calls", w.scanCalls)
	}
}

// TestValidateUnvalidatedForeignKeys_CleanPasses: the scan finds no orphan, so
// the FK stands and the run continues.
func TestValidateUnvalidatedForeignKeys_CleanPasses(t *testing.T) {
	schema, fk := childSchema()
	w := &fakeFKWriter{
		unvalidated: []ir.UnvalidatedForeignKey{{Table: "events", FK: fk}},
		scanFound:   false,
	}
	if err := ValidateUnvalidatedForeignKeys(context.Background(), w, schema, dummySampler{}); err != nil {
		t.Fatalf("a clean scan must pass; got %v", err)
	}
	if w.scanCalls == 0 {
		t.Fatal("the scan must actually run for a reported unvalidated FK")
	}
	if len(w.dropped) != 0 {
		t.Fatalf("a clean scan must not drop the FK; dropped %v", w.dropped)
	}
}

// TestValidateUnvalidatedForeignKeys_OrphanRefuses: the scan finds an orphan, so
// the FK is DROPPED and a coded SLUICE-E-FK-SOURCE-ORPHAN refusal is returned
// naming the table, FK, and example key.
func TestValidateUnvalidatedForeignKeys_OrphanRefuses(t *testing.T) {
	schema, fk := childSchema()
	w := &fakeFKWriter{
		unvalidated:   []ir.UnvalidatedForeignKey{{Table: "events", FK: fk}},
		scanFound:     true,
		scanViolating: []any{int64(42)},
	}
	err := ValidateUnvalidatedForeignKeys(context.Background(), w, schema, dummySampler{})
	if err == nil {
		t.Fatal("an orphan must refuse the run")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeFKSourceOrphan {
		t.Fatalf("want SLUICE-E-FK-SOURCE-ORPHAN; got %v (coded=%v)", err, ok)
	}
	// The FK must be dropped BEFORE refusing — never left present-but-unproven.
	if len(w.dropped) != 1 || w.dropped[0] != "events.events_customer_fk" {
		t.Fatalf("the violated FK must be dropped; dropped=%v", w.dropped)
	}
	for _, want := range []string{"events", "events_customer_fk", "42", "customers"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal message must name %q; got: %v", want, err)
		}
	}
}

// TestValidateUnvalidatedForeignKeys_ReporterWithoutScannerRefuses: a writer
// that reports unvalidated FKs but cannot scan/drop them must refuse loudly
// rather than let them stand unproven.
func TestValidateUnvalidatedForeignKeys_ReporterWithoutScannerRefuses(t *testing.T) {
	schema, fk := childSchema()
	w := &reporterNoScannerDropper{unvalidated: []ir.UnvalidatedForeignKey{{Table: "events", FK: fk}}}
	err := ValidateUnvalidatedForeignKeys(context.Background(), w, schema, dummySampler{})
	if err == nil {
		t.Fatal("a writer that reports unvalidated FKs but cannot scan them must refuse")
	}
	if !strings.Contains(err.Error(), "cannot scan/drop") {
		t.Errorf("want a loud 'cannot scan/drop' refusal; got %v", err)
	}
}

// reporterNoScannerDropper implements the reporter but NOT the scanner/dropper.
type reporterNoScannerDropper struct {
	unvalidated []ir.UnvalidatedForeignKey
}

func (reporterNoScannerDropper) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}

func (reporterNoScannerDropper) CreateIndexes(context.Context, *ir.Schema) error { return nil }

func (reporterNoScannerDropper) CreateConstraints(context.Context, *ir.Schema) error { return nil }

func (reporterNoScannerDropper) SyncIdentitySequences(context.Context, *ir.Schema) error { return nil }

func (reporterNoScannerDropper) CreateViews(context.Context, *ir.Schema) error { return nil }

func (r reporterNoScannerDropper) UnvalidatedForeignKeys() []ir.UnvalidatedForeignKey {
	return r.unvalidated
}

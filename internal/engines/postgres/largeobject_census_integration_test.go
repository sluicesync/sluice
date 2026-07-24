//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for the large-object census (roadmap item 68c) on
// real PG: a lo_create'd blob shows up in the count, an oid-typed
// column shows up in the suspects (a plain integer column does not),
// and a fresh database censuses to zero. The pipeline-side WARN
// wording/scoping is unit-pinned in
// internal/pipeline/largeobject_preflight_test.go; this file
// ground-truths the catalog SQL the advisory rides on.

package postgres

import (
	"context"
	"testing"
	"time"
)

func TestLargeObjectCensus_RealPG(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, dsn, `
		CREATE TABLE docs (
			id       BIGINT PRIMARY KEY,
			blob_ref OID,
			note     TEXT
		);
		CREATE TABLE plain (
			id  BIGINT PRIMARY KEY,
			qty INTEGER
		);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srIface, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	sr, ok := srIface.(*SchemaReader)
	if !ok {
		t.Fatalf("schema reader is %T; want *SchemaReader", srIface)
	}
	defer func() { _ = sr.Close() }()

	// Before any lo exists: count 0, but the oid column is already a
	// suspect (the census reports types, the pipeline decides volume).
	count, suspects, err := sr.LargeObjectCensus(ctx)
	if err != nil {
		t.Fatalf("census (no los): %v", err)
	}
	if count != 0 {
		t.Errorf("lo count on a fresh db = %d; want 0", count)
	}
	if got := suspects["docs"]; len(got) != 1 || got[0] != "blob_ref" {
		t.Errorf("suspects[docs] = %v; want [blob_ref]", got)
	}
	if _, ok := suspects["plain"]; ok {
		t.Errorf("plain integer columns must not be suspects; got %v", suspects["plain"])
	}

	// Create a real large object and reference it.
	applyDDL(t, dsn, `
		INSERT INTO docs (id, blob_ref, note)
		VALUES (1, lo_from_bytea(0, '\xdeadbeef'), 'has a blob');
	`)

	count, suspects, err = sr.LargeObjectCensus(ctx)
	if err != nil {
		t.Fatalf("census (with lo): %v", err)
	}
	if count != 1 {
		t.Errorf("lo count after lo_from_bytea = %d; want 1", count)
	}
	if got := suspects["docs"]; len(got) != 1 || got[0] != "blob_ref" {
		t.Errorf("suspects[docs] = %v; want [blob_ref]", got)
	}
}

// Compile-time: the reader satisfies the pipeline's prober shape (the
// pipeline declares its own private interface; this mirrors it so an
// engine-side signature drift fails here, next to the SQL).
var _ interface {
	LargeObjectCensus(ctx context.Context) (int64, map[string][]string, error)
} = (*SchemaReader)(nil)

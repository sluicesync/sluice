//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for multi-schema Postgres `sync start` WITH a namespace
// rename map (ADR-0142 on the sync/CDC path). It is the streamer counterpart
// of TestMigrate_MultiSchema_RenameMap_PostgresToPostgres: two source schemas
// a, b are mapped a=x, b=y; cold-start + steady-state CDC must land every
// change in the RENAMED target schema (x, y) — never the source-named schema
// — with exactly-once convergence (counts + a content checksum). This pins
// the namespaceRenameFunc threading through SetMultiDatabaseRouting into the
// CDC change-router's routedSchema, and that the rename routes inserts,
// updates AND deletes (the three change families) without cross-schema bleed.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_MultiSchema_RenameMap_PostgresToPostgres drives a map-only
// (--map-schema a=x,b=y, no --include-schema) multi-schema PG→PG sync through
// cold-start and a steady-state insert/update/delete burst, asserting the data
// lands in the renamed target schemas x/y (never a/b) with exactly-once
// convergence.
func TestStreamer_MultiSchema_RenameMap_PostgresToPostgres(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgresLogicalMultiSchema(t)
	defer cleanup()

	// Two source schemas a, b, each with a same-named widgets table
	// (namespace isolation). `public` is left untouched and unmapped, so the
	// map-only selection must leave it out entirely.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA a;
		CREATE SCHEMA b;
		CREATE TABLE a.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE b.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO a.widgets (id, name) VALUES (1, 'a-one'), (2, 'a-two');
		INSERT INTO b.widgets (id, name) VALUES (1, 'b-one'), (2, 'b-two'), (3, 'b-three');
	`)

	pgEng, _ := engines.Get("postgres")
	nsMap, err := NewNamespaceRenameMap([]string{"a=x", "b=y"})
	if err != nil {
		t.Fatalf("construct rename map: %v", err)
	}
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:    pgEng,
			Target:    pgEng,
			SourceDSN: pgSource,
			TargetDSN: pgTarget,
			StreamID:  "multischema-rename-pg2pg",
			// Map-only: no DatabaseFilter; the map keys ARE the selection.
			NamespaceMap: nsMap,
		}
	}
	if !newStreamer().multiDatabaseMode() {
		t.Fatal("a rename map alone should engage multi-schema sync mode")
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- newStreamer().Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("rename streamer did not return after ctx cancel")
		}
	}()

	// ---- Cold-start lands the seed rows in the RENAMED target schemas. ----
	if !waitForPGSchemaCount(t, pgTarget, "x", "widgets", 2, 60*time.Second) {
		t.Fatalf("cold-start never delivered a.widgets to renamed schema x")
	}
	if !waitForPGSchemaCount(t, pgTarget, "y", "widgets", 3, 60*time.Second) {
		t.Fatalf("cold-start never delivered b.widgets to renamed schema y")
	}

	// ---- Steady-state DML in BOTH source schemas (insert/update/delete).
	// The CDC change-router must apply each change to its RENAMED target
	// schema, keying the route on the source schema. ----
	applyPGDDL(t, pgSource, `
		INSERT INTO a.widgets (id, name) VALUES (3, 'a-three');
		UPDATE a.widgets SET name='a-one-upd' WHERE id=1;
		INSERT INTO b.widgets (id, name) VALUES (4, 'b-four');
		DELETE FROM b.widgets WHERE id=2;
	`)

	// x: 2 + 1 insert = 3. y: 3 + 1 insert - 1 delete = 3.
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM x.widgets`, 3, 30*time.Second) {
		t.Fatalf("CDC never settled renamed schema x (want 3)")
	}
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM y.widgets`, 3, 30*time.Second) {
		t.Fatalf("CDC never settled renamed schema y (want 3)")
	}
	// The UPDATE routed to x only (count-neutral, so poll for its effect).
	if !waitForPGScalar(t, pgTarget, `SELECT COUNT(*) FROM x.widgets WHERE name='a-one-upd'`, 1, 15*time.Second) {
		t.Fatalf("CDC never routed the a→x UPDATE")
	}

	// Cross-schema bleed guards: the UPDATE landed in x only, the DELETE in y
	// only.
	if got := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM y.widgets WHERE name='a-one-upd'`); got != 0 {
		t.Errorf("cross-schema bleed: y has x's a-one-upd (%d); want 0", got)
	}
	if got := pgScalarCount(pgTarget, `SELECT COUNT(*) FROM y.widgets WHERE id=2`); got != 0 {
		t.Errorf("y DELETE not routed: id=2 still present (%d); want 0", got)
	}

	// The SOURCE-named schemas must NOT exist on the target — the rename
	// routed only to x/y, never a/b (proof of rename, not same-name copy), and
	// `public` (unmapped) was never selected.
	if n := pgScalarCount(pgTarget, `
		SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN ('a','b')`); n != 0 {
		t.Errorf("found %d source-named target schema(s); want 0 (rename must not create same-named schemas)", n)
	}

	// ---- Exactly-once convergence: a content checksum of each source schema
	// equals its renamed target schema (counts + ordered (id,name) digest).
	// A loss, dup or mis-route changes the digest. ----
	assertPGCrossSchemaChecksum(t, pgSource, "a", pgTarget, "x", "widgets")
	assertPGCrossSchemaChecksum(t, pgSource, "b", pgTarget, "y", "widgets")
}

// assertPGCrossSchemaChecksum asserts that srcSchema.table on the source and
// dstSchema.table on the target hold IDENTICAL content — same row count and
// the same ordered (id, name) digest — the exactly-once convergence gate when
// the two namespaces differ by name (a rename, so the same-name parity helper
// can't be reused).
func assertPGCrossSchemaChecksum(t *testing.T, srcDSN, srcSchema, dstDSN, dstSchema, table string) {
	t.Helper()
	digest := func(schema string) string {
		return "SELECT COALESCE(md5(string_agg(id::text || ':' || name, ',' ORDER BY id)), '') " +
			"FROM " + schema + "." + table
	}
	count := func(schema string) string {
		return "SELECT COUNT(*) FROM " + schema + "." + table
	}
	srcCount := pgScalarCount(srcDSN, count(srcSchema))
	dstCount := pgScalarCount(dstDSN, count(dstSchema))
	if srcCount != dstCount {
		t.Errorf("%s vs %s exactly-once FAIL: count src=%d dst=%d", srcSchema, dstSchema, srcCount, dstCount)
	}
	srcDigest := pgString(t, srcDSN, digest(srcSchema))
	dstDigest := pgString(t, dstDSN, digest(dstSchema))
	if srcDigest != dstDigest {
		t.Errorf("%s vs %s exactly-once FAIL: digest src=%q dst=%q", srcSchema, dstSchema, srcDigest, dstDigest)
	}
}

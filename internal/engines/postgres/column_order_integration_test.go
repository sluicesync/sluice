//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// columnOrderRecordFixtures is the codec matrix the store must round-trip
// on a real target (RENAME-SWAP-FORWARD): namespaces that differ only in
// case or by being absent, names the encoding must not touch (quotes,
// backticks, backslashes, multibyte, case), a wide table, and a
// recorded-but-empty list, which must come back empty, not absent.
func columnOrderRecordFixtures() []ir.ColumnOrderRecord {
	wide := make([]string, 1500)
	for i := range wide {
		wide[i] = fmt.Sprintf("column_%04d_with_a_long_name", i)
	}
	odd := []string{"id", "列名", "🦀", `a"b`, "a`b", `a\b`, "a'b", "Foo", "foo", "café"}
	return []ir.ColumnOrderRecord{
		{Schema: "public", Table: "kd", SourceColumns: []string{"id", "b", "c"}, TargetColumns: []string{"id", "c", "b"}},
		{Schema: "Public", Table: "kd", SourceColumns: []string{"id", "c"}, TargetColumns: []string{"id", "c"}},
		{Schema: "", Table: "kd", SourceColumns: []string{"id"}, TargetColumns: []string{"id"}},
		{Schema: "public", Table: "表_ü", SourceColumns: odd, TargetColumns: reversed(odd)},
		{Schema: "public", Table: "wide", SourceColumns: wide, TargetColumns: wide},
		{Schema: "public", Table: "empty", SourceColumns: []string{}, TargetColumns: []string{}},
	}
}

func assertColumnOrders(t *testing.T, got, want []ir.ColumnOrderRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d records; want %d", len(got), len(want))
	}
	byKey := map[[2]string]ir.ColumnOrderRecord{}
	for _, r := range got {
		byKey[[2]string{r.Schema, r.Table}] = r
	}
	for _, w := range want {
		g, ok := byKey[[2]string{w.Schema, w.Table}]
		if !ok {
			t.Errorf("record %q.%q did not come back", w.Schema, w.Table)
			continue
		}
		if g.SourceColumns == nil || g.TargetColumns == nil ||
			!slices.Equal(g.SourceColumns, w.SourceColumns) || !slices.Equal(g.TargetColumns, w.TargetColumns) {
			t.Errorf("record %q.%q round-tripped to %q / %q; want %q / %q",
				w.Schema, w.Table, g.SourceColumns, g.TargetColumns, w.SourceColumns, w.TargetColumns)
		}
	}
}

// TestColumnOrderStore_RoundTripsOnARealTarget is the MySQL test's twin on
// Postgres.
func TestColumnOrderStore_RoundTripsOnARealTarget(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	opened, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	applier := opened.(*ChangeApplier)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	if got, err := applier.ReadColumnOrders(ctx, "rt"); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("a stream with no records read %v, %v; want an empty list", got, err)
	}
	want := columnOrderRecordFixtures()
	if err := applier.WriteColumnOrders(ctx, "rt", want); err != nil {
		t.Fatalf("WriteColumnOrders: %v", err)
	}
	got, err := applier.ReadColumnOrders(ctx, "rt")
	if err != nil {
		t.Fatalf("ReadColumnOrders: %v", err)
	}
	assertColumnOrders(t, got, want)

	want[0].TargetColumns = []string{"id", "b", "c"}
	if err := applier.WriteColumnOrders(ctx, "rt", want[:1]); err != nil {
		t.Fatalf("WriteColumnOrders (upsert): %v", err)
	}
	got, err = applier.ReadColumnOrders(ctx, "rt")
	if err != nil {
		t.Fatalf("ReadColumnOrders: %v", err)
	}
	assertColumnOrders(t, got, want)
	if other, err := applier.ReadColumnOrders(ctx, "other"); err != nil || len(other) != 0 {
		t.Fatalf("another stream read %v, %v; want none of this stream's records", other, err)
	}

	applyPGApplier(t, dsn, "UPDATE "+columnOrderTableName+" SET target_columns = 'null' WHERE table_name = 'kd' AND schema_name = 'public'")
	if _, err := applier.ReadColumnOrders(ctx, "rt"); err == nil {
		t.Fatal("a damaged record was read; want the read refused")
	}
}

// TestColumnOrderStore_TheBoundaryRecordRidesItsTransaction is the MySQL
// test's twin on Postgres: the record commits and rolls back with its
// boundary, and a boundary carrying none leaves it alone.
func TestColumnOrderStore_TheBoundaryRecordRidesItsTransaction(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE TABLE kd (id BIGINT PRIMARY KEY, c INT, b INT);`)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	opened, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	applier := opened.(*ChangeApplier)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	snap := func(tok string, target []string) ir.SchemaSnapshot {
		return ir.SchemaSnapshot{
			Position: ir.Position{Engine: "postgres", Token: tok}, Schema: "public", Table: "kd",
			IR: &ir.Table{Schema: "public", Name: "kd", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "c", Type: ir.Integer{Width: 32}}, {Name: "b", Type: ir.Integer{Width: 32}},
			}},
			TargetColumns: target,
		}
	}
	if err := applier.applyOne(ctx, "atomic", snap(`{"lsn":"0/1"}`, []string{"id", "c", "b"})); err != nil {
		t.Fatalf("applyOne: %v", err)
	}
	committed := []ir.ColumnOrderRecord{{
		Schema: "public", Table: "kd",
		SourceColumns: []string{"id", "c", "b"}, TargetColumns: []string{"id", "c", "b"},
	}}
	got, err := applier.ReadColumnOrders(ctx, "atomic")
	if err != nil {
		t.Fatal(err)
	}
	assertColumnOrders(t, got, committed)
	if err := applier.applyOne(ctx, "atomic", snap(`{"lsn":"0/2"}`, nil)); err != nil {
		t.Fatalf("applyOne: %v", err)
	}
	got, _ = applier.ReadColumnOrders(ctx, "atomic")
	assertColumnOrders(t, got, committed)

	applyPGApplier(t, dsn, `DROP TABLE `+controlTableName)
	if err := applier.applyOne(ctx, "atomic", snap(`{"lsn":"0/3"}`, []string{"id", "b", "c"})); err == nil {
		t.Fatal("applyOne succeeded with the position write broken; the case needs it to fail")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var stored string
	if err := db.QueryRowContext(ctx, "SELECT target_columns FROM "+columnOrderTableName+" WHERE stream_id = 'atomic'").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != `["id","c","b"]` {
		t.Fatalf("after the failed boundary the record reads %s; want the committed one, untouched — the record ran ahead of its boundary", stored)
	}
}

// reversed returns a reversed copy of s.
func reversed(s []string) []string {
	out := slices.Clone(s)
	slices.Reverse(out)
	return out
}

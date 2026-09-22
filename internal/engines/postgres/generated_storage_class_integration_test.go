//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-6 (gap census 2026-09-22 S1/D5) on real servers: the generated
// column STORAGE-CLASS axis round-trips PG → PG, and the environmental
// premises the fix leans on are MEASURED rather than asserted.
//
// Two tests, two server populations:
//
//   - TestGeneratedColumns_StorageClass_PG18 boots a pinned postgres:18
//     (the first version with VIRTUAL) so the VIRTUAL cells run on every
//     per-PR shard, not only on the weekly pg-version-matrix. It also
//     measures the logical-replication premise the CDC lane's comment
//     cites: a publication never carries a VIRTUAL column.
//   - TestGeneratedColumns_StorageClass_DefaultImage runs on whatever
//     the harness booted (PG 16 per-PR, 17/18/latest on the matrix) and
//     pins BOTH directions of the version gate: below 18 the VIRTUAL
//     syntax is refused by the server (so that cell cannot be built,
//     and the test asserts the refusal instead of skipping) and a
//     VIRTUAL IR column lands STORED with the promotion WARN; at 18+
//     the same IR column lands VIRTUAL with no WARN.
//
// The independent expected value throughout is the TARGET's own
// pg_attribute.attgenerated, read with plain SQL — never sluice's reader
// re-reading what sluice's writer wrote.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// pgAttGeneratedByColumn reads attgenerated for every user column of
// schema.table on dsn, keyed by column name — the ground-truth catalog
// read every assertion below compares against.
func pgAttGeneratedByColumn(t *testing.T, dsn, schema, table string) map[string]string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname, a.attgenerated::text
		FROM   pg_attribute a
		JOIN   pg_class c ON c.oid = a.attrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE  n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped`, schema, table)
	if err != nil {
		t.Fatalf("query attgenerated: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var name, gen string
		if err := rows.Scan(&name, &gen); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[name] = gen
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// writeSchemaTo applies s to dsn through the real schema writer (the
// same OpenSchemaWriter → CreateTablesWithoutConstraints door migrate
// and sync cold start use), so the version probe and the emit gate are
// the production ones.
func writeSchemaTo(ctx context.Context, t *testing.T, dsn string, s *ir.Schema) *SchemaWriter {
	t.Helper()
	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open target writer: %v", err)
	}
	t.Cleanup(func() { _ = sw.(*SchemaWriter).Close() })
	if err := sw.CreateTablesWithoutConstraints(ctx, s); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	return sw.(*SchemaWriter)
}

// readSchemaFrom reads dsn through the real schema reader.
func readSchemaFrom(ctx context.Context, t *testing.T, dsn string) *ir.Schema {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open source reader: %v", err)
	}
	defer closeReader(t, sr)
	s, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	return s
}

// generatedColumn finds col on table in s.
func generatedColumn(t *testing.T, s *ir.Schema, table, col string) *ir.Column {
	t.Helper()
	for _, tb := range s.Tables {
		if tb.Name != table {
			continue
		}
		for _, c := range tb.Columns {
			if c.Name == col {
				return c
			}
		}
	}
	t.Fatalf("column %s.%s not in the read schema", table, col)
	return nil
}

// TestGeneratedColumns_StorageClass_PG18: on a pinned PostgreSQL 18 a
// source table declaring STORED, VIRTUAL and the bare form (VIRTUAL by
// default on 18 — measured, and the reason a PG 18 operator who never
// wrote the keyword is affected) reads into the IR with the right
// GeneratedStored, writes to a second database on the same server, and
// the target catalog carries 's'/'v'/'v'. Before GC-6 the reader set
// STORED for all three and the target catalog read 's'/'s'/'s'.
//
// The second half measures the logical-replication premise: under a
// plain publication attnames omits every generated column, and under
// `publish_generated_columns = stored` it admits the STORED one and
// still omits both VIRTUAL ones — so a VIRTUAL column is never on the
// wire and the CDC lane's existing generated-column exclusion
// (cdc_normalize.go) covers it without a change.
func TestGeneratedColumns_StorageClass_PG18(t *testing.T) {
	srcDSN, cleanup := startPostgresForCDCImage(t, "postgres:18")
	defer cleanup()
	if v := pgServerVersionNum(t, srcDSN); v < pgVersionVirtualGeneratedColumns {
		t.Fatalf("postgres:18 booted with server_version_num=%d; the image tag no longer means what this test pins", v)
	}
	applyPGSQL(t, srcDSN, `CREATE DATABASE target_db`)
	tgtDSN := strings.Replace(srcDSN, "/source_db?", "/target_db?", 1)
	if tgtDSN == srcDSN {
		t.Fatalf("could not derive a target DSN from %q", srcDSN)
	}

	applyPGSQL(t, srcDSN, `
		CREATE TABLE gen (
			id   INT PRIMARY KEY,
			c    INT NOT NULL,
			s    INT GENERATED ALWAYS AS (c * 2) STORED,
			v    INT GENERATED ALWAYS AS (c * 3) VIRTUAL,
			bare INT GENERATED ALWAYS AS (c * 4)
		);
		INSERT INTO gen (id, c) VALUES (1, 10);`)

	// The source premise: 18 spells the bare form 'v'.
	srcGen := pgAttGeneratedByColumn(t, srcDSN, "public", "gen")
	if srcGen["s"] != "s" || srcGen["v"] != "v" || srcGen["bare"] != "v" {
		t.Fatalf("source attgenerated = %v; want s/v/v — PostgreSQL 18's own spelling changed", srcGen)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	s := readSchemaFrom(ctx, t, srcDSN)
	for col, wantStored := range map[string]bool{"s": true, "v": false, "bare": false} {
		c := generatedColumn(t, s, "gen", col)
		if !c.IsGenerated() {
			t.Fatalf("reader lost the generated expression on gen.%s: %+v", col, c)
		}
		if c.GeneratedStored != wantStored {
			t.Errorf("reader: gen.%s GeneratedStored = %v; want %v", col, c.GeneratedStored, wantStored)
		}
	}

	sw := writeSchemaTo(ctx, t, tgtDSN, s)
	if sw.serverVersionNum < pgVersionVirtualGeneratedColumns {
		t.Fatalf("writer probed server_version_num=%d on a PG 18 target; the VIRTUAL gate would never open", sw.serverVersionNum)
	}
	tgtGen := pgAttGeneratedByColumn(t, tgtDSN, "public", "gen")
	if tgtGen["s"] != "s" || tgtGen["v"] != "v" || tgtGen["bare"] != "v" {
		t.Errorf("target attgenerated = %v; want s/v/v (the storage class must round-trip PG 18 → PG 18)", tgtGen)
	}

	// The computed values agree on both sides — a VIRTUAL column that
	// round-tripped as VIRTUAL still computes.
	applyPGSQL(t, tgtDSN, `INSERT INTO gen (id, c) VALUES (1, 10)`)
	for _, dsn := range []string{srcDSN, tgtDSN} {
		if got := pgQueryString(t, dsn, `SELECT s::text || ',' || v::text || ',' || bare::text FROM gen WHERE id = 1`); got != "20,30,40" {
			t.Errorf("%s: generated values = %q; want 20,30,40", dsn, got)
		}
	}

	// The logical-replication premise, measured on the source.
	applyPGSQL(t, srcDSN, `CREATE PUBLICATION plain FOR TABLE gen;
		CREATE PUBLICATION with_stored FOR TABLE gen WITH (publish_generated_columns = stored);`)
	attnames := func(pub string) string {
		return pgQueryString(t, srcDSN, `SELECT array_to_string(attnames, ',') FROM pg_publication_tables WHERE pubname = '`+pub+`'`)
	}
	if got := attnames("plain"); got != "id,c" {
		t.Errorf("plain publication attnames = %q; want id,c (no generated column is published by default)", got)
	}
	if got := attnames("with_stored"); got != "id,c,s" {
		t.Errorf("publish_generated_columns=stored attnames = %q; want id,c,s (a VIRTUAL column is never published)", got)
	}
}

// TestGeneratedColumns_StorageClass_DefaultImage pins the version gate in
// both directions on the harness's default server. Every cell asserts
// something on every version; nothing skips.
func TestGeneratedColumns_StorageClass_DefaultImage(t *testing.T) {
	srcDSN, srcCleanup := newSharedPGDB(t, "sluice_gen_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedPGDB(t, "sluice_gen_tgt")
	defer tgtCleanup()

	version := pgServerVersionNum(t, srcDSN)
	virtualCapable := version >= pgVersionVirtualGeneratedColumns

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("STORED round-trips STORED on every version", func(t *testing.T) {
		applyPGSQL(t, srcDSN, `CREATE TABLE st (id INT PRIMARY KEY, c INT NOT NULL, s INT GENERATED ALWAYS AS (c * 2) STORED)`)
		s := readSchemaFrom(ctx, t, srcDSN)
		if c := generatedColumn(t, s, "st", "s"); !c.GeneratedStored {
			t.Fatalf("reader: st.s GeneratedStored = false; want true (attgenerated 's')")
		}
		buf := captureGeneratedWarns(t)
		writeSchemaTo(ctx, t, tgtDSN, &ir.Schema{Tables: []*ir.Table{s.Tables[0]}})
		if got := pgAttGeneratedByColumn(t, tgtDSN, "public", "st")["s"]; got != "s" {
			t.Errorf("target st.s attgenerated = %q; want s", got)
		}
		if strings.Contains(buf.String(), generatedVirtualPromotedMarker) {
			t.Errorf("a STORED column must never trip the promotion WARN; log: %s", buf.String())
		}
	})

	t.Run("VIRTUAL source cell", func(t *testing.T) {
		db, err := sql.Open("pgx", srcDSN)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()
		_, err = db.ExecContext(ctx, `CREATE TABLE vt (id INT PRIMARY KEY, c INT NOT NULL, v INT GENERATED ALWAYS AS (c * 3) VIRTUAL)`)
		if !virtualCapable {
			// The measured reason the cell cannot be built here: the
			// server refuses the syntax. Asserted rather than skipped,
			// so a server that starts accepting VIRTUAL below 18 —
			// or a harness that quietly booted a newer image — fails
			// this test instead of silently changing which cells ran.
			if err == nil {
				t.Fatalf("server_version_num=%d accepted GENERATED … VIRTUAL; the gate constant pgVersionVirtualGeneratedColumns is wrong for this server", version)
			}
			t.Logf("server_version_num=%d refuses VIRTUAL as expected: %v", version, err)
			return
		}
		if err != nil {
			t.Fatalf("server_version_num=%d refused GENERATED … VIRTUAL: %v", version, err)
		}
		s := readSchemaFrom(ctx, t, srcDSN)
		if c := generatedColumn(t, s, "vt", "v"); c.GeneratedStored {
			t.Fatalf("reader: vt.v GeneratedStored = true; want false (attgenerated 'v')")
		}
		var vt *ir.Table
		for _, tb := range s.Tables {
			if tb.Name == "vt" {
				vt = tb
			}
		}
		writeSchemaTo(ctx, t, tgtDSN, &ir.Schema{Tables: []*ir.Table{vt}})
		if got := pgAttGeneratedByColumn(t, tgtDSN, "public", "vt")["v"]; got != "v" {
			t.Errorf("target vt.v attgenerated = %q; want v", got)
		}
	})

	// The writer half on its own: an IR column a MySQL VIRTUAL source (or
	// a PG 18 source) would hand this target. Below 18 it MUST land
	// STORED with the promotion WARN; at 18+ it lands VIRTUAL silently.
	t.Run("a VIRTUAL IR column lands per the target version", func(t *testing.T) {
		buf := captureGeneratedWarns(t)
		writeSchemaTo(ctx, t, tgtDSN, &ir.Schema{Tables: []*ir.Table{{
			Name: "ir_v",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "c", Type: ir.Integer{Width: 32}},
				{Name: "v", Type: ir.Integer{Width: 32}, Nullable: true, GeneratedExpr: "(c * 3)", GeneratedStored: false},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}}})
		got := pgAttGeneratedByColumn(t, tgtDSN, "public", "ir_v")["v"]
		warned := strings.Contains(buf.String(), generatedVirtualPromotedMarker)
		if virtualCapable {
			if got != "v" || warned {
				t.Errorf("PG %d target: attgenerated = %q, promotion WARN = %v; want v and no WARN (log: %s)", version, got, warned, buf.String())
			}
		} else if got != "s" || !warned {
			t.Errorf("PG %d target: attgenerated = %q, promotion WARN = %v; want s and a WARN carrying %s (log: %s)", version, got, warned, generatedVirtualPromotedMarker, buf.String())
		}
	})

	// The named wart: an INDEXED VIRTUAL column (a MySQL shape) lands
	// STORED on every version, with the WARN, because PG 18 refuses the
	// index on a VIRTUAL column — measured here on 18+ so the reason
	// stays a fact rather than a memory.
	t.Run("an indexed VIRTUAL IR column is promoted so its index can build", func(t *testing.T) {
		if virtualCapable {
			db, err := sql.Open("pgx", srcDSN)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			applyPGSQL(t, srcDSN, `CREATE TABLE ixv (id INT PRIMARY KEY, c INT NOT NULL, v INT GENERATED ALWAYS AS (c * 3) VIRTUAL)`)
			if _, err := db.ExecContext(ctx, `CREATE INDEX ixv_v ON ixv (v)`); err == nil {
				t.Fatalf("server_version_num=%d indexed a VIRTUAL column; the promotion's reason no longer holds — drop it and carry the index", version)
			}
		}
		buf := captureGeneratedWarns(t)
		tbl := &ir.Table{
			Name: "ir_ixv",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "c", Type: ir.Integer{Width: 32}},
				{Name: "v", Type: ir.Integer{Width: 32}, Nullable: true, GeneratedExpr: "(c * 3)", GeneratedStored: false},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes:    []*ir.Index{{Name: "ir_ixv_v", Columns: []ir.IndexColumn{{Column: "v"}}}},
		}
		sw := writeSchemaTo(ctx, t, tgtDSN, &ir.Schema{Tables: []*ir.Table{tbl}})
		if err := sw.CreateIndexes(ctx, &ir.Schema{Tables: []*ir.Table{tbl}}); err != nil {
			t.Fatalf("CreateIndexes on the promoted column: %v", err)
		}
		if got := pgAttGeneratedByColumn(t, tgtDSN, "public", "ir_ixv")["v"]; got != "s" {
			t.Errorf("target ir_ixv.v attgenerated = %q; want s (promoted so the index can build)", got)
		}
		if !strings.Contains(buf.String(), generatedVirtualPromotedMarker) {
			t.Errorf("promotion WARN missing; log: %s", buf.String())
		}
	})
}

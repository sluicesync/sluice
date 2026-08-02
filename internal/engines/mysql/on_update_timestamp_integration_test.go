//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ON UPDATE CURRENT_TIMESTAMP round-trip against a real MySQL
// (audit 2026-08-01 S7).
//
// The attribute rides information_schema.columns.extra, which the reader was
// consuming only for DEFAULT_GENERATED and the GENERATED storage class — so
// `ON UPDATE CURRENT_TIMESTAMP` fell through unread and was discarded on every
// path, including MySQL→MySQL, where both ends support it natively and the
// target simply stopped maintaining the column.
//
// A unit pin cannot establish this: the whole defect lives in the exact string
// the server puts in `extra`, and the emitted clause has to be one the server
// accepts back. So this reads a real table, re-emits it, applies the emitted
// DDL to the same server, and re-reads — the round trip has to survive the
// server at both ends.
//
// The precision cases are the load-bearing ones. MySQL requires the ON UPDATE
// fractional precision to equal the column's own and rejects a mismatch with
// errno 1294, which is why the IR carries a bool and the emitter renders the
// precision from the column type. If that derivation were wrong, the re-apply
// below fails on the server rather than passing quietly.

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestOnUpdateCurrentTimestamp_RoundTripsThroughRealMySQL(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE src (
			id     INT PRIMARY KEY,
			t0     TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			t3     DATETIME(3)  NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			t6     TIMESTAMP(6) NULL ON UPDATE CURRENT_TIMESTAMP(6),
			plain  TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP,
			bare   DATETIME     NULL
		);
	`)

	ctx := context.Background()
	eng := Engine{}
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	src := findTableByName(schema, "src")
	if src == nil {
		t.Fatal("table src not found in the read schema")
	}

	// (a) The reader must SEE it — this is the half that was missing.
	want := map[string]bool{
		"id": false, "t0": true, "t3": true, "t6": true,
		"plain": false, // DEFAULT only: extra is "DEFAULT_GENERATED", must NOT match
		"bare":  false,
	}
	got := map[string]bool{}
	for _, c := range src.Columns {
		got[c.Name] = c.OnUpdateCurrentTimestamp
	}
	for col, w := range want {
		if got[col] != w {
			t.Errorf("column %q: OnUpdateCurrentTimestamp = %v, want %v. "+
				"The attribute rides information_schema `extra`; `plain` is the discriminating case — it "+
				"carries DEFAULT_GENERATED with no ON UPDATE, so a substring match on the wrong token "+
				"would light it up too.", col, got[col], w)
		}
	}

	// (b) The emitter must render a clause the SERVER accepts back, at every
	//     precision. A wrong precision derivation fails here with errno 1294.
	ddl, err := emitTableDef(src)
	if err != nil {
		t.Fatalf("emit CREATE TABLE: %v", err)
	}
	if !strings.Contains(strings.ToUpper(ddl), "ON UPDATE CURRENT_TIMESTAMP") {
		t.Fatalf("emitted DDL carries no ON UPDATE clause:\n%s", ddl)
	}
	rebuilt := strings.Replace(ddl, "`src`", "`rebuilt`", 1)
	rebuilt = strings.Replace(rebuilt, `"src"`, `"rebuilt"`, 1)
	applyMySQL(t, dsn, rebuilt)

	// (c) Re-read the rebuilt table: the attribute must have survived the
	//     whole loop, per column and per precision.
	schema2, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema (rebuilt): %v", err)
	}
	rt := findTableByName(schema2, "rebuilt")
	if rt == nil {
		t.Fatalf("rebuilt table not found; emitted DDL was:\n%s", rebuilt)
	}
	for _, c := range rt.Columns {
		if want[c.Name] != c.OnUpdateCurrentTimestamp {
			t.Errorf("after round trip, column %q: OnUpdateCurrentTimestamp = %v, want %v (emitted DDL:\n%s)",
				c.Name, c.OnUpdateCurrentTimestamp, want[c.Name], rebuilt)
		}
	}
}

// findTableByName returns the named table from a read schema, or nil.
func findTableByName(s *ir.Schema, name string) *ir.Table {
	if s == nil {
		return nil
	}
	for _, t := range s.Tables {
		if t != nil && t.Name == name {
			return t
		}
	}
	return nil
}

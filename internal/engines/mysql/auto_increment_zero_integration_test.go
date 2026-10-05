// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSessionInvariants_SQLModeTierMatrix pins NO_AUTO_VALUE_ON_ZERO on the
// LIVE session, over every tier of the sql_mode override policy, on a real
// server — including the two tiers where sluice injects nothing and the
// post-connect ensureNoAutoValueOnZero is the only door: a DSN sql_mode
// (lower-case key, which the injector sees and yields to, and UPPER-case
// key, which it does not see and which the driver sends alongside the
// injected value in random map order) and the empty --mysql-sql-mode escape
// hatch. For each it reads the session's mode back AND writes a 0 into an
// AUTO_INCREMENT column through the pool and reads it back, so the pin
// grades the outcome, not the plumbing. The operator's strictness choice
// must survive: a DSN mode without STRICT_TRANS_TABLES stays without it.
func TestSessionInvariants_SQLModeTierMatrix(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "aiz_tiers")
	defer cleanup()
	strp := func(s string) *string { return &s }
	cases := []struct {
		name       string
		dsnParam   string
		mode       *string
		wantStrict bool
	}{
		{"nil override", "", nil, true},
		{"kong default literal", "", strp(defaultStrictSQLMode), true},
		{"explicit list", "", strp("ANSI_QUOTES"), false},
		{"empty escape hatch", "", strp(""), false},
		{"DSN sql_mode", "sql_mode=" + url.QueryEscape("'ANSI_QUOTES'"), nil, false},
		{"DSN SQL_MODE upper-case key", "SQL_MODE=" + url.QueryEscape("'ANSI_QUOTES'"), nil, false},
	}
	ctx := context.Background()
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			full := dsn
			if tc.dsnParam != "" {
				sep := "?"
				if strings.Contains(full, "?") {
					sep = "&"
				}
				full += sep + tc.dsnParam
			}
			cfg := mustParseDSN(t, full)
			db, err := openDB(ctx, cfg, tc.mode)
			if err != nil {
				t.Fatalf("openDB: %v", err)
			}
			defer func() { _ = db.Close() }()
			// Several connections: the upper-case DSN key races the
			// injected value per connection, so one sample proves little.
			db.SetMaxIdleConns(0)
			for n := 0; n < 8; n++ {
				var mode string
				if err := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
					t.Fatal(err)
				}
				if !sqlModeHas(mode, noAutoValueOnZero) {
					t.Fatalf("connection %d: session sql_mode %q lacks %s", n, mode, noAutoValueOnZero)
				}
				if tc.name == "DSN sql_mode" && sqlModeHas(mode, "STRICT_TRANS_TABLES") != tc.wantStrict {
					t.Fatalf("session sql_mode %q: the operator's strictness choice was not kept", mode)
				}
			}
			tbl := fmt.Sprintf("aiz_tier_%d", i)
			for _, q := range []string{
				"DROP TABLE IF EXISTS " + tbl,
				"CREATE TABLE " + tbl + " (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v VARCHAR(8))",
				"INSERT INTO " + tbl + " (id, v) VALUES (0, 'zero'), (9, 'nine')",
			} {
				if _, err := db.ExecContext(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			var ids string
			if err := db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(id ORDER BY id) FROM "+tbl).Scan(&ids); err != nil {
				t.Fatal(err)
			}
			if ids != "0,9" {
				t.Errorf("AUTO_INCREMENT ids = %s, want 0,9: an explicit 0 was rewritten", ids)
			}
		})
	}
}

// TestWriteCores_AutoIncrementZero_EveryCore drives each of the MySQL row
// writer's three bulk cores — LOAD DATA, the batched INSERT, and the
// idempotent ON DUPLICATE KEY UPDATE — with a row keyed 0 into an
// AUTO_INCREMENT key and into a non-key AUTO_INCREMENT column, through a
// writer opened by the engine (so through openDB), and reads the target
// back. Which core ran is asserted from the server's Com_load counter for
// the two non-ODKU cores.
func TestWriteCores_AutoIncrementZero_EveryCore(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "aiz_cores")
	defer cleanup()
	ctx := context.Background()
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()

	pk := &ir.Table{
		Name: "aiz_core_pk",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "v", Type: ir.Varchar{Length: 16}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	uk := &ir.Table{
		Name: "aiz_core_uk",
		Columns: []*ir.Column{
			{Name: "k", Type: ir.Varchar{Length: 8}},
			{Name: "n", Type: ir.Integer{Width: 32, Unsigned: true, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}},
	}
	ddl := map[string]string{
		"aiz_core_pk": "CREATE TABLE aiz_core_pk (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v VARCHAR(16) NOT NULL)",
		"aiz_core_uk": "CREATE TABLE aiz_core_uk (k VARCHAR(8) NOT NULL PRIMARY KEY, n INT UNSIGNED NOT NULL AUTO_INCREMENT, UNIQUE KEY (n))",
	}
	rowsFor := map[string][]ir.Row{
		"aiz_core_pk": {{"id": int64(0), "v": "zero"}, {"id": int64(5), "v": "five"}, {"id": int64(100), "v": "hundred"}},
		"aiz_core_uk": {{"k": "a", "n": int64(0)}, {"k": "b", "n": int64(7)}},
	}
	want := map[string]string{"aiz_core_pk": "0:zero,5:five,100:hundred", "aiz_core_uk": "a:0,b:7"}
	readQ := map[string]string{
		"aiz_core_pk": "SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id) FROM aiz_core_pk",
		"aiz_core_uk": "SELECT GROUP_CONCAT(CONCAT(k, ':', n) ORDER BY k) FROM aiz_core_uk",
	}

	cores := []struct {
		name     string
		infile   string
		loadData bool
		write    func(w *RowWriter, tbl *ir.Table, ch <-chan ir.Row) error
	}{
		{"load-data", "ON", true, func(w *RowWriter, tbl *ir.Table, ch <-chan ir.Row) error { return w.WriteRows(ctx, tbl, ch) }},
		{"batched-insert", "OFF", false, func(w *RowWriter, tbl *ir.Table, ch <-chan ir.Row) error { return w.WriteRows(ctx, tbl, ch) }},
		{"idempotent-odku", "OFF", false, func(w *RowWriter, tbl *ir.Table, ch <-chan ir.Row) error {
			return w.WriteRowsIdempotent(ctx, tbl, ch)
		}},
	}
	for _, core := range cores {
		t.Run(core.name, func(t *testing.T) {
			if _, err := admin.ExecContext(ctx, "SET GLOBAL local_infile = "+core.infile); err != nil {
				t.Skipf("SET GLOBAL local_infile: %v", err)
			}
			rw, err := Engine{}.OpenRowWriter(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenRowWriter: %v", err)
			}
			defer func() { _ = rw.(*RowWriter).Close() }()
			w := rw.(*RowWriter)
			before := comLoad(t, admin)
			for _, tbl := range []*ir.Table{pk, uk} {
				if _, err := admin.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl.Name); err != nil {
					t.Fatal(err)
				}
				if _, err := admin.ExecContext(ctx, ddl[tbl.Name]); err != nil {
					t.Fatal(err)
				}
				ch := make(chan ir.Row, len(rowsFor[tbl.Name]))
				for _, r := range rowsFor[tbl.Name] {
					ch <- r
				}
				close(ch)
				if err := core.write(w, tbl, ch); err != nil {
					t.Fatalf("write %s: %v", tbl.Name, err)
				}
				var got string
				if err := admin.QueryRowContext(ctx, readQ[tbl.Name]).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != want[tbl.Name] {
					t.Errorf("%s via %s = %s, want %s: an AUTO_INCREMENT value of 0 was rewritten",
						tbl.Name, core.name, got, want[tbl.Name])
				}
			}
			if moved := comLoad(t, admin) != before; moved != core.loadData {
				t.Fatalf("Com_load moved=%v, want %v: the %s core did not run", moved, core.loadData, core.name)
			}
		})
	}
}

// TestSessionInvariants_MariaDB_NoAutoValueOnZero measures the same fix on
// MariaDB (11.4), a registered flavor whose sql_mode vocabulary is its own:
// that MariaDB accepts NO_AUTO_VALUE_ON_ZERO in the injected list and in the
// CONCAT_WS repair, and honours it on a batched-INSERT write through the
// engine's own row writer, is a fact about MariaDB, so it is asserted.
func TestSessionInvariants_MariaDB_NoAutoValueOnZero(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	defer cleanup()
	ctx := context.Background()
	strp := func(s string) *string { return &s }
	for _, mode := range []*string{nil, strp("")} {
		db, err := openDB(ctx, mustParseDSN(t, dsn), mode)
		if err != nil {
			t.Fatalf("openDB(mode=%v): %v", mode, err)
		}
		var got string
		if err := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&got); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
		if !sqlModeHas(got, noAutoValueOnZero) {
			t.Fatalf("MariaDB session sql_mode %q (override %v) lacks %s", got, mode, noAutoValueOnZero)
		}
	}
	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, "CREATE TABLE aiz_maria (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v VARCHAR(16) NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	rw, err := Engine{Flavor: FlavorMariaDB}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() { _ = rw.(*RowWriter).Close() }()
	tbl := &ir.Table{
		Name: "aiz_maria",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "v", Type: ir.Varchar{Length: 16}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	ch := make(chan ir.Row, 2)
	ch <- ir.Row{"id": int64(0), "v": "zero"}
	ch <- ir.Row{"id": int64(9), "v": "nine"}
	close(ch)
	if err := rw.(*RowWriter).writeBatched(ctx, tbl, ch); err != nil {
		t.Fatalf("writeBatched: %v", err)
	}
	var ids string
	if err := admin.QueryRowContext(ctx, "SELECT GROUP_CONCAT(id ORDER BY id) FROM aiz_maria").Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if ids != "0,9" {
		t.Errorf("MariaDB AUTO_INCREMENT ids = %s, want 0,9: an explicit 0 was rewritten", ids)
	}
}

func comLoad(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var name string
	var v int64
	if err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Com_load'").Scan(&name, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

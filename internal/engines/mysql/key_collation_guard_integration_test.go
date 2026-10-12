//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-FIRST-BOUNDARY option 3, the MySQL-family applier backstop
// (key_collation_guard.go), on real servers: MySQL 8.0 (the shared
// container) and MariaDB 11.4, through both apply doors — the serial
// Apply (one statement per change) and the batched ApplyBatch (the
// coalesced multi-row upsert, which is also the lanes' path).
//
// The independent expected value is the target's own rows, read by
// HEX(key) after the run, never the applier's errors alone: a refusal must
// leave the target exactly as it was, and a pass must leave it as the
// source would.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// kcgServer is one target server under test.
type kcgServer struct {
	name  string
	eng   Engine
	newDB func(t *testing.T, name string) (string, func())
	// srcCI / tgtCI are two case-INSENSITIVE collations this server has,
	// with different names (a `_ci` source re-delivering onto a target
	// whose key is case-insensitive under a different collation).
	srcCI, tgtCI string
}

func kcgServers(t *testing.T) []kcgServer {
	t.Helper()
	mariaDSN, mariaCleanup := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	t.Cleanup(mariaCleanup)
	return []kcgServer{
		{
			name: "mysql-8.0", eng: Engine{},
			newDB: func(t *testing.T, name string) (string, func()) { return newSharedDB(t, name) },
			srcCI: "utf8mb4_0900_ai_ci", tgtCI: "utf8mb4_general_ci",
		},
		{
			name: "mariadb-11.4", eng: Engine{Flavor: FlavorMariaDB},
			newDB: func(t *testing.T, name string) (string, func()) {
				return kcgFreshDB(t, mariaDSN, name), func() {}
			},
			srcCI: "utf8mb4_unicode_ci", tgtCI: "utf8mb4_general_ci",
		},
	}
}

// kcgFreshDB creates database name on base's server and returns its DSN.
func kcgFreshDB(t *testing.T, base, name string) string {
	t.Helper()
	db, err := sql.Open("mysql", base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE DATABASE " + name + " DEFAULT CHARSET=utf8mb4"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	i := strings.LastIndex(base, "/")
	q := strings.Index(base[i:], "?")
	if q < 0 {
		return base[:i+1] + name
	}
	return base[:i+1] + name + base[i+q:]
}

// kcgRows reads table as "<id>:<HEX(k)>" sorted, the exact key bytes.
func kcgRows(t *testing.T, dsn, table, idCol string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(fmt.Sprintf("SELECT CAST(%s AS CHAR), HEX(k) FROM %s", idCol, table))
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id, k string
		if err := rows.Scan(&id, &k); err != nil {
			t.Fatal(err)
		}
		out = append(out, id+":"+k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// kcgSnapshot is the source's boundary for table: its key column k declared
// with charset/collation, as the active schema the applier arms from.
func kcgSnapshot(schema, table string, cols []*ir.Column) ir.SchemaSnapshot {
	return ir.SchemaSnapshot{
		Schema: schema, Table: table,
		Position: ir.Position{Engine: "mysql", Token: "kcg-snap-" + table},
		IR:       &ir.Table{Schema: schema, Name: table, Columns: cols},
	}
}

func kcgVarchar(coll string) ir.Type {
	return ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: coll}
}

// kcgApply applies changes through the serial door (Apply) or the batched
// door (ApplyBatch), returning the applier's error.
func kcgApply(t *testing.T, eng Engine, dsn string, batched bool, changes ...ir.Change) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer closeIf(applier)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	ch := make(chan ir.Change, len(changes))
	for _, c := range changes {
		ch <- c
	}
	close(ch)
	if batched {
		return applier.(ir.BatchedChangeApplier).ApplyBatch(ctx, "kcg-stream", ch, 100)
	}
	return applier.Apply(ctx, "kcg-stream", ch)
}

func kcgPos(n int) ir.Position { return ir.Position{Engine: "mysql", Token: fmt.Sprintf("kcg-%d", n)} }

// TestKeyCollationGuard_MySQLFamily is the backstop's matrix: every key
// shape the upsert can match through (PRIMARY KEY, a UNIQUE key beside a
// PRIMARY KEY, a table whose every column is its key, a UNIQUE-only table
// with no PRIMARY KEY — ADR-0072) × both apply doors × both servers. Each
// merge must refuse KEY-COLLATION-MERGE with the target untouched; each
// legitimate write — an identical re-apply, an UPDATE coalesced into an
// upsert that changes a UNIQUE column's case, and a `_ci` source's
// case-only key change re-delivered — must pass and converge.
func TestKeyCollationGuard_MySQLFamily(t *testing.T) {
	for _, srv := range kcgServers(t) {
		for _, batched := range []bool{false, true} {
			door := "serial"
			if batched {
				door = "batched"
			}
			srv, batched := srv, batched
			t.Run(srv.name+"/"+door, func(t *testing.T) {
				kcgRunServer(t, srv, batched)
			})
		}
	}
}

func kcgRunServer(t *testing.T, srv kcgServer, batched bool) {
	n := 0
	cell := func(name string) (string, string) {
		n++
		db := fmt.Sprintf("kcg_%s_%d_%t", strings.NewReplacer("-", "", ".", "").Replace(srv.name), n, batched)
		dsn, cleanup := srv.newDB(t, db)
		t.Cleanup(cleanup)
		return db, dsn
	}
	wantMerge := func(t *testing.T, err error, dsn, table, idCol, before string) {
		t.Helper()
		if !errors.Is(err, ir.ErrKeyCollationMerge) {
			t.Fatalf("err = %v; want %s", err, ir.ErrKeyCollationMerge)
		}
		if !strings.Contains(err.Error(), "KEY-COLLATION-MERGE") {
			t.Fatalf("the refusal text carries no marker: %v", err)
		}
		if got := kcgRows(t, dsn, table, idCol); got != before {
			t.Fatalf("the refusal changed the target: %s → %s", before, got)
		}
	}
	pkTable := func(schema string) []*ir.Column {
		return []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: kcgVarchar("utf8mb4_bin")},
			{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
		}
	}

	t.Run("pk merge", func(t *testing.T) {
		db, dsn := cell("pk")
		applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, d INT, PRIMARY KEY (k))")
		applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a',1)")
		before := kcgRows(t, dsn, "kd", "id")
		err := kcgApply(
			t, srv.eng, dsn, batched,
			kcgSnapshot(db, "kd", pkTable(db)),
			ir.Insert{Position: kcgPos(1), Schema: db, Table: "kd", Row: ir.Row{"id": int64(1), "k": "a", "d": int64(11)}},
			ir.Insert{Position: kcgPos(2), Schema: db, Table: "kd", Row: ir.Row{"id": int64(99), "k": "A", "d": int64(99)}},
		)
		if batched {
			// The batch holds both rows in one statement: the merge rolls
			// back the identical re-apply beside it too.
			wantMerge(t, err, dsn, "kd", "id", before)
			return
		}
		if !errors.Is(err, ir.ErrKeyCollationMerge) {
			t.Fatalf("err = %v; want %s", err, ir.ErrKeyCollationMerge)
		}
		if got, want := kcgRows(t, dsn, "kd", "id"), "1:61"; got != want {
			t.Fatalf("target %s; want %s (the identical re-apply lands, the merge does not)", got, want)
		}
	})

	t.Run("unique key beside a primary key", func(t *testing.T) {
		db, dsn := cell("uq")
		applyDDL(t, dsn, "CREATE TABLE ku (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, d INT, UNIQUE KEY uk (k))")
		applyDDL(t, dsn, "INSERT INTO ku VALUES (1,'a',1),(2,'b',2)")
		before := kcgRows(t, dsn, "ku", "id")
		snap := kcgSnapshot(db, "ku", pkTable(db))
		err := kcgApply(t, srv.eng, dsn, batched, snap,
			ir.Insert{Position: kcgPos(1), Schema: db, Table: "ku", Row: ir.Row{"id": int64(99), "k": "A", "d": int64(99)}})
		wantMerge(t, err, dsn, "ku", "id", before)
		// The same row changing its unique column's case — an UPDATE the
		// batch coalesces into an upsert — is one source row: it passes.
		err = kcgApply(t, srv.eng, dsn, batched, snap,
			ir.Update{
				Position: kcgPos(2), Schema: db, Table: "ku",
				Before: ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
				After:  ir.Row{"id": int64(1), "k": "A", "d": int64(1)},
			})
		if err != nil {
			t.Fatalf("a case-only change of a unique column on the same row refused: %v", err)
		}
		if got, want := kcgRows(t, dsn, "ku", "id"), "1:41,2:62"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})

	t.Run("every column is the key", func(t *testing.T) {
		db, dsn := cell("ak")
		applyDDL(t, dsn, "CREATE TABLE ak (k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY)")
		applyDDL(t, dsn, "INSERT INTO ak VALUES ('a')")
		before := kcgRows(t, dsn, "ak", "k")
		snap := kcgSnapshot(db, "ak", []*ir.Column{{Name: "k", Type: kcgVarchar("utf8mb4_bin")}})
		if err := kcgApply(t, srv.eng, dsn, batched, snap,
			ir.Insert{Position: kcgPos(1), Schema: db, Table: "ak", Row: ir.Row{"k": "a"}}); err != nil {
			t.Fatalf("an identical re-apply refused: %v", err)
		}
		err := kcgApply(t, srv.eng, dsn, batched, snap,
			ir.Insert{Position: kcgPos(2), Schema: db, Table: "ak", Row: ir.Row{"k": "A"}})
		wantMerge(t, err, dsn, "ak", "k", before)
	})

	t.Run("unique key and no primary key", func(t *testing.T) {
		db, dsn := cell("nk")
		applyDDL(t, dsn, "CREATE TABLE nk (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, d INT, UNIQUE KEY uk (k))")
		applyDDL(t, dsn, "INSERT INTO nk VALUES (1,'a',1)")
		before := kcgRows(t, dsn, "nk", "id")
		err := kcgApply(t, srv.eng, dsn, batched,
			kcgSnapshot(db, "nk", pkTable(db)),
			ir.Insert{Position: kcgPos(1), Schema: db, Table: "nk", Row: ir.Row{"id": int64(2), "k": "A", "d": int64(2)}})
		wantMerge(t, err, dsn, "nk", "id", before)
	})

	// Review LOW: with no primary key the guard cannot tell a re-delivered
	// row from a second source row. A `_bin` source's case-only UPDATE of a
	// UNIQUE column, re-delivered, therefore refuses — loud and permanent by
	// design — and the remedy must be the one that converges: re-copy the
	// table. Giving the target key the source's collation does NOT converge
	// here (measured below): the re-delivered INSERT no longer conflicts and
	// lands a second row.
	t.Run("no primary key: a re-delivered case-only update refuses and names a re-copy", func(t *testing.T) {
		db, dsn := cell("np")
		applyDDL(t, dsn, "CREATE TABLE np (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, d INT, UNIQUE KEY uk (k))")
		snap := kcgSnapshot(db, "np", pkTable(db))
		insert := ir.Insert{Position: kcgPos(1), Schema: db, Table: "np", Row: ir.Row{"id": int64(1), "k": "a", "d": int64(1)}}
		update := ir.Update{
			Position: kcgPos(2), Schema: db, Table: "np",
			Before: ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
			After:  ir.Row{"id": int64(1), "k": "A", "d": int64(1)},
		}
		if err := kcgApply(t, srv.eng, dsn, batched, snap, insert, update); err != nil {
			t.Fatalf("first delivery: %v", err)
		}
		before := kcgRows(t, dsn, "np", "id")
		if before != "1:41" {
			t.Fatalf("after the first delivery the target is %s; want 1:41", before)
		}
		err := kcgApply(t, srv.eng, dsn, batched, snap, insert, update)
		wantMerge(t, err, dsn, "np", "id", before)
		if !strings.Contains(err.Error(), "cannot tell a re-delivered change") || !strings.Contains(err.Error(), "Re-copy the table") {
			t.Fatalf("the refusal's remedy does not lead with the re-copy a keyless table needs: %v", err)
		}
		if strings.Contains(err.Error(), "MODIFY <column>") {
			t.Fatalf("the refusal recommends the collation change unconditionally, which does not converge here: %v", err)
		}
		// The collation remedy, measured 2026-10-12 on MySQL 8.0 and MariaDB
		// 11.4: the re-delivered INSERT no longer conflicts and lands a second
		// (1,'a'); the re-delivered UPDATE then hits the UNIQUE key (1062).
		// Loud, and not converged (serially the stray row stays) — which is
		// why the remedy leads with the re-copy. Pinned loud: a silent pass
		// here would mean the collation remedy started converging, and the
		// remedy text should be revisited.
		applyDDL(t, dsn, "ALTER TABLE np MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL")
		err = kcgApply(t, srv.eng, dsn, batched, snap, insert, update)
		t.Logf("after the collation change the re-delivery returned %v and the target is %s", err, kcgRows(t, dsn, "np", "id"))
		if err == nil {
			t.Fatalf("after the collation change the re-delivery passed (target %s): the collation remedy now converges "+
				"on a keyless table, so keyCollationMergeRemedy's re-copy-first text is stale", kcgRows(t, dsn, "np", "id"))
		}
	})

	// KEY-COLLATION-PG-DEFAULT-TO-MYSQL, operator's choice (b): a Postgres
	// source key on the database DEFAULT collation (no name, deterministic,
	// case-sensitive) lands on a `_ci` target key. Beside a primary key a
	// UNIQUE merge refuses; the primary key itself is not armed, so a
	// re-delivered case-only change of it still converges.
	pgDefault := ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}
	t.Run("postgres default collation: a unique key beside a primary key refuses a merge", func(t *testing.T) {
		db, dsn := cell("pgu")
		applyDDL(t, dsn, "CREATE TABLE pu (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE "+srv.tgtCI+" NOT NULL, d INT, UNIQUE KEY uk (k))")
		applyDDL(t, dsn, "INSERT INTO pu VALUES (1,'a',1)")
		snap := kcgSnapshot(db, "pu", []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: pgDefault},
			{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
		})
		before := kcgRows(t, dsn, "pu", "id")
		err := kcgApply(t, srv.eng, dsn, batched, snap,
			ir.Insert{Position: kcgPos(1), Schema: db, Table: "pu", Row: ir.Row{"id": int64(99), "k": "A", "d": int64(99)}})
		wantMerge(t, err, dsn, "pu", "id", before)
		// The same row changing its unique column's case passes: the primary
		// key recognises it.
		if err := kcgApply(t, srv.eng, dsn, batched, snap, ir.Update{
			Position: kcgPos(2), Schema: db, Table: "pu",
			Before: ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
			After:  ir.Row{"id": int64(1), "k": "A", "d": int64(1)},
		}); err != nil {
			t.Fatalf("a case-only change of the unique column on the same row refused: %v", err)
		}
		if got, want := kcgRows(t, dsn, "pu", "id"), "1:41"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})
	t.Run("postgres default collation: a re-delivered case-only primary-key change converges", func(t *testing.T) {
		db, dsn := cell("pgp")
		applyDDL(t, dsn, "CREATE TABLE pp (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE "+srv.tgtCI+" NOT NULL, d INT, PRIMARY KEY (k))")
		snap := kcgSnapshot(db, "pp", []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: pgDefault},
			{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
		})
		insert := ir.Insert{Position: kcgPos(1), Schema: db, Table: "pp", Row: ir.Row{"id": int64(1), "k": "a", "d": int64(1)}}
		update := ir.Update{
			Position: kcgPos(2), Schema: db, Table: "pp",
			Before: ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
			After:  ir.Row{"id": int64(1), "k": "A", "d": int64(1)},
		}
		if err := kcgApply(t, srv.eng, dsn, batched, snap, insert, update); err != nil {
			t.Fatalf("first delivery: %v", err)
		}
		if err := kcgApply(t, srv.eng, dsn, batched, snap, insert, update); err != nil {
			t.Fatalf("the re-delivery refused (a new wedge): %v", err)
		}
		if got, want := kcgRows(t, dsn, "pp", "id"), "1:41"; got != want {
			t.Fatalf("after the re-delivery the target is %s; want %s", got, want)
		}
	})

	t.Run("a case-insensitive source re-delivers a case-only key change", func(t *testing.T) {
		db, dsn := cell("rd")
		applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE "+srv.tgtCI+" NOT NULL, d INT, PRIMARY KEY (k))")
		snap := kcgSnapshot(db, "kd", []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: kcgVarchar(srv.srcCI)},
			{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
		})
		insert := ir.Insert{Position: kcgPos(1), Schema: db, Table: "kd", Row: ir.Row{"id": int64(1), "k": "a", "d": int64(1)}}
		update := ir.Update{
			Position: kcgPos(2), Schema: db, Table: "kd",
			Before: ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
			After:  ir.Row{"id": int64(1), "k": "A", "d": int64(1)},
		}
		if err := kcgApply(t, srv.eng, dsn, batched, snap, insert, update); err != nil {
			t.Fatalf("first delivery: %v", err)
		}
		// The window re-delivered: the INSERT of 'a' now meets the target's
		// 'A', which the source's own collation equates with it.
		if err := kcgApply(t, srv.eng, dsn, batched, snap, insert, update); err != nil {
			t.Fatalf("the re-delivery refused (a wedge): %v", err)
		}
		if got, want := kcgRows(t, dsn, "kd", "id"), "1:41"; got != want {
			t.Fatalf("after the re-delivery the target is %s; want %s", got, want)
		}
	})
}

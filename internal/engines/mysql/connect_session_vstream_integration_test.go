//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"
)

// bootVTTestSequenceCluster boots one vttestserver with a 2-shard keyspace
// "test" and an unsharded keyspace "lookup" (sequence tables must live in an
// unsharded keyspace), and returns the vtgate DSN for each.
func bootVTTestSequenceCluster(t *testing.T) (sharded, unsharded string, cleanup func()) {
	t.Helper()
	vt := bootVTTestServer(t, "test,lookup", "2,1")
	return vt.dsn("test"), vt.dsn("lookup"), vt.terminate
}

// waitVTTestInsert retries a first INSERT until vtgate's schema tracker has
// picked the table up ("table not found" for a moment after CREATE).
func waitVTTestExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		_, err := db.Exec(q, args...)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", q, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestVStream_SessionInvariants_ThroughVTGate measures the NO_AUTO_VALUE_ON_ZERO
// fix through vtgate, which sits between every Vitess/PlanetScale session and
// mysqld and has its own handling of `SET sql_mode` (a non-default value moves
// the session onto vttablet's settings pool). Three facts are asserted rather
// than assumed (the premise-naming rule):
//
//  1. vtgate accepts the injected mode list carrying the flag, serves the
//     post-connect `SELECT @@session.time_zone, @@session.sql_mode` probe,
//     and accepts the CONCAT_WS repair for the two tiers sluice injects
//     nothing on (a DSN sql_mode, the empty escape hatch);
//  2. on an UNSHARDED keyspace a 0 written into a MySQL AUTO_INCREMENT column
//     through a sluice-opened pool lands as 0 (vtgate passes the session mode
//     to mysqld);
//  3. on a SHARDED keyspace whose column is backed by a vtgate SEQUENCE, the
//     outcome is vtgate's, not mysqld's: vtgate's shouldGenerate (vitess
//     go/vt/vtgate/engine/insert_common.go) generates for an explicit 0
//     without consulting NO_AUTO_VALUE_ON_ZERO, so sluice's session mode
//     cannot carry it. Measured and recorded here, so a vtgate that starts
//     honouring the flag (or a sluice change that refuses the value) shows
//     up as this assertion changing.
func TestVStream_SessionInvariants_ThroughVTGate(t *testing.T) {
	shardedDSN, unshardedDSN, cleanup := bootVTTestSequenceCluster(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	admin, err := sql.Open("mysql", unshardedDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	for _, q := range []string{
		"CREATE TABLE aiz_u (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v VARCHAR(16))",
		"CREATE TABLE aiz_seq (id INT, next_id BIGINT, cache BIGINT, PRIMARY KEY (id)) COMMENT 'vitess_sequence'",
		"INSERT INTO aiz_seq (id, next_id, cache) VALUES (0, 1000, 10)",
	} {
		waitVTTestExec(t, admin, q)
	}
	applyVTTestSQL(t, unshardedDSN, "ALTER VSCHEMA ADD SEQUENCE lookup.aiz_seq")

	strp := func(s string) *string { return &s }
	tiers := []struct {
		name     string
		dsnParam string
		mode     *string
	}{
		{"nil override", "", nil},
		{"empty escape hatch", "", strp("")},
		{"DSN sql_mode", "sql_mode=" + url.QueryEscape("'STRICT_TRANS_TABLES'"), nil},
	}
	for i, tier := range tiers {
		t.Run("unsharded/"+tier.name, func(t *testing.T) {
			full := unshardedDSN
			if tier.dsnParam != "" {
				full += "&" + tier.dsnParam
			}
			db, err := openDB(ctx, mustParseDSN(t, full), tier.mode)
			if err != nil {
				t.Fatalf("openDB through vtgate: %v", err)
			}
			defer func() { _ = db.Close() }()
			var mode string
			if err := db.QueryRowContext(ctx, "SELECT @@session.sql_mode").Scan(&mode); err != nil {
				t.Fatal(err)
			}
			if !sqlModeHas(mode, noAutoValueOnZero) {
				t.Fatalf("vtgate session sql_mode %q lacks %s", mode, noAutoValueOnZero)
			}
			v := fmt.Sprintf("t%d", i)
			if _, err := db.ExecContext(ctx, "DELETE FROM aiz_u WHERE id = 0"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO aiz_u (id, v) VALUES (0, ?)", v); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM aiz_u WHERE id = 0 AND v = ?", v).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("an explicit 0 written through vtgate did not land as 0 (rows at id 0 with v=%s: %d)", v, n)
			}
		})
	}

	t.Run("sharded/sequence-backed column", func(t *testing.T) {
		sh, err := sql.Open("mysql", shardedDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sh.Close() }()
		waitVTTestExec(t, sh, "CREATE TABLE aiz_sh (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(16))")
		applyVTTestSQL(t, shardedDSN, "ALTER VSCHEMA ON test.aiz_sh ADD VINDEX hash(id) USING hash")
		applyVTTestSQL(t, shardedDSN, "ALTER VSCHEMA ON test.aiz_sh ADD AUTO_INCREMENT id USING lookup.aiz_seq")
		db, err := openDB(ctx, mustParseDSN(t, shardedDSN), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		waitVTTestExec(t, db, "INSERT INTO aiz_sh (id, v) VALUES (0, 'zero')")
		var id int64
		if err := db.QueryRowContext(ctx, "SELECT id FROM aiz_sh WHERE v = 'zero'").Scan(&id); err != nil {
			t.Fatal(err)
		}
		t.Logf("MEASURED: an explicit 0 into a vtgate-sequence-backed column on a sharded keyspace landed as id=%d", id)
		if id == 0 {
			t.Errorf("vtgate kept the explicit 0 (id=0): the recorded residual (vtgate generates for 0 regardless of " +
				"NO_AUTO_VALUE_ON_ZERO) no longer holds — update the residual note and the audit backlog")
		}
	})
}

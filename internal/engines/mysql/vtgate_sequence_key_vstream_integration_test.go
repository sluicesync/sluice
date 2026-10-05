//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_ProbeReplayKey_UnsuppliedPrimaryVindex is item B's measurement
// and pin. On a SHARDED keyspace a UNIQUE index is enforced per shard: two
// rows collide only if the primary vindex routes them to the same shard. A
// target table keyed on a sequence-backed `sid` (the primary vindex) and
// carrying a supplied NOT NULL UNIQUE(email) looked keyed to the replay
// probe — UNIQUE(email) is fully supplied — but a re-sent row carries no
// sid, vtgate draws a fresh sequence value, the row routes by THAT value,
// and on the other shard UNIQUE(email) collides with nothing.
//
// The measured re-send: N rows written, then the identical N rows written
// again one at a time through a sluice-opened pool, the way the cold-copy
// retry or a broker re-apply would re-send them. Every re-send that routed
// to the other shard lands a duplicate at no error; one that routed to the
// same shard fails 1062. The pin asserts the probe now says keyed=false for
// this table — and still keyed=true for the same shape with sid SUPPLIED.
func TestVStream_ProbeReplayKey_UnsuppliedPrimaryVindex(t *testing.T) {
	shardedDSN, unshardedDSN, cleanup := bootVTTestSequenceCluster(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	lk, err := sql.Open("mysql", unshardedDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lk.Close() }()
	waitVTTestExec(t, lk, "CREATE TABLE b_seq (id INT, next_id BIGINT, cache BIGINT, PRIMARY KEY (id)) COMMENT 'vitess_sequence'")
	waitVTTestExec(t, lk, "INSERT INTO b_seq (id, next_id, cache) VALUES (0, 1, 1)")
	applyVTTestSQL(t, unshardedDSN, "ALTER VSCHEMA ADD SEQUENCE lookup.b_seq")

	sh, err := sql.Open("mysql", shardedDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sh.Close() }()
	waitVTTestExec(t, sh, "CREATE TABLE b_t (sid BIGINT NOT NULL, email VARCHAR(64) NOT NULL, v VARCHAR(16), "+
		"PRIMARY KEY (sid), UNIQUE KEY uq_email (email))")
	applyVTTestSQL(t, shardedDSN, "ALTER VSCHEMA ON test.b_t ADD VINDEX hash(sid) USING hash")
	applyVTTestSQL(t, shardedDSN, "ALTER VSCHEMA ON test.b_t ADD AUTO_INCREMENT sid USING lookup.b_seq")

	db, err := openDB(ctx, mustParseDSN(t, shardedDSN), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	const n = 16
	for i := 0; i < n; i++ {
		waitVTTestExec(t, db, "INSERT INTO b_t (email, v) VALUES (?, 'first')", fmt.Sprintf("u%d@x", i))
	}
	dup, collided := 0, 0
	for i := 0; i < n; i++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO b_t (email, v) VALUES (?, 'resent')", fmt.Sprintf("u%d@x", i)); err != nil {
			if !isMySQLDupKey(err) {
				t.Fatalf("re-send %d: %v", i, err)
			}
			collided++
			continue
		}
		dup++
	}
	var total int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM b_t").Scan(&total); err != nil {
		t.Fatal(err)
	}
	t.Logf("MEASURED: %d rows written, re-sent once each: %d re-sends landed a DUPLICATE email at no error, "+
		"%d collided (1062); the table now holds %d rows for %d emails", n, dup, collided, total, n)
	if dup == 0 {
		t.Fatalf("no re-send landed a duplicate (%d collided): the measurement did not reach the cross-shard "+
			"shape, so the probe assertion below would grade nothing", collided)
	}

	rw, err := (Engine{Flavor: FlavorVitess}).OpenRowWriter(ctx, shardedDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rw.(*RowWriter).Close() }()
	prober := rw.(ir.ReplayKeyProber)
	col := func(name string) *ir.Column { return &ir.Column{Name: name, Type: ir.Varchar{Length: 64}} }

	unsupplied := &ir.Table{Name: "b_t", Columns: []*ir.Column{col("email"), col("v")}}
	exists, keyed, err := prober.ProbeReplayKey(ctx, unsupplied)
	if err != nil || !exists || keyed {
		t.Errorf("sid NOT supplied: ProbeReplayKey = (%v, %v, %v), want (true, false, nil): UNIQUE(email) is "+
			"per-shard and the re-sent row routes by a fresh sid", exists, keyed, err)
	}
	supplied := &ir.Table{Name: "b_t", Columns: []*ir.Column{{Name: "sid", Type: ir.Integer{Width: 64}}, col("email"), col("v")}}
	exists, keyed, err = prober.ProbeReplayKey(ctx, supplied)
	if err != nil || !exists || !keyed {
		t.Errorf("sid supplied: ProbeReplayKey = (%v, %v, %v), want (true, true, nil)", exists, keyed, err)
	}

	// The primary vindex is read as the FIRST row of SHOW VSCHEMA VINDEXES ON;
	// that order is a fact about vtgate, so it is asserted: a secondary
	// vindex added afterwards must not displace it.
	if _, err := sh.ExecContext(ctx, "ALTER VSCHEMA ON test.b_t ADD VINDEX unicode_loose_md5(email) USING unicode_loose_md5"); err != nil {
		t.Logf("could not add a secondary vindex (%v); primary-first order not graded here", err)
		return
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		cols, perr := readPrimaryVindexColumns(ctx, db, "test", "b_t")
		all, aerr := readVindexColumns(ctx, db, "test", "b_t")
		if perr == nil && aerr == nil && len(all) >= 2 {
			if strings.Join(cols, ",") != "sid" {
				t.Fatalf("primary vindex columns = %v (all: %v), want [sid]: SHOW VSCHEMA VINDEXES ON did not list the primary first", cols, all)
			}
			t.Logf("MEASURED: primary-first order holds with a secondary vindex present: primary=%v all=%v", cols, all)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("secondary vindex never appeared: primary=%v (%v) all=%v (%v)", cols, perr, all, aerr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

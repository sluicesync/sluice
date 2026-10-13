//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCDCReader_SameServerIDOnMySQL_BothStreamsKeepReading measures the
// MySQL arm of BINLOG-SERVER-ID-COLLISION. Two readers forced onto ONE
// server id stream from one MySQL 8 source; both must keep delivering every
// row, through a write made after both registered and another made later,
// AND both Binlog Dump threads must be the same connections at the end —
// so the pass cannot come from the source evicting one and go-mysql's
// syncer redialling it. MariaDB refuses the second registration loudly
// (ERROR 4052, observed in the pipeline's MariaDB cells).
//
// UNVERIFIED PREMISE, which this test is the only check of: that MySQL
// picks the zombie dump thread to evict by the replica's @replica_uuid /
// @slave_uuid rather than by server_id, and that go-mysql gives every syncer
// a distinct one. go-mysql sets it from uuid.NewUUID() (a version-1, time-
// and-MAC UUID, not a random one), so two syncers in one process differ by
// clock sequence; that is go-mysql's behaviour at the pinned version, not a
// contract. If either changes, this fails rather than a stream stalling.
func TestCDCReader_SameServerIDOnMySQL_BothStreamsKeepReading(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	applyMySQL(t, dsn, `CREATE TABLE sid (id BIGINT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB;`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const forced = 4242424
	open := func() (*CDCReader, <-chan ir.Change) {
		t.Helper()
		r, err := Engine{Flavor: FlavorVanilla}.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		rdr := r.(*CDCReader)
		rdr.serverID = forced
		t.Cleanup(func() { _ = rdr.Close() })
		ch, err := rdr.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
		return rdr, ch
	}
	before := sidDumpThreads(t, ctx, dsn)
	first, firstCh := open()
	second, secondCh := open()
	// The two dump threads these readers registered: an evict-and-reconnect
	// would replace one with a new connection id, so holding the same pair
	// to the end shows neither was killed and redialled (go-mysql's syncer
	// reconnects on its own, which a row count alone could not tell apart).
	ours := map[int64]bool{}
	for id := range sidDumpThreads(t, ctx, dsn) {
		if !before[id] {
			ours[id] = true
		}
	}
	if len(ours) != 2 {
		t.Fatalf("found %d new Binlog Dump threads after opening two readers on server id %d; want 2", len(ours), forced)
	}

	applyMySQL(t, dsn, `INSERT INTO sid VALUES (1, 1);`)
	time.Sleep(2 * time.Second)
	applyMySQL(t, dsn, `INSERT INTO sid VALUES (2, 2);`)

	for _, lane := range []struct {
		name string
		rdr  *CDCReader
		ch   <-chan ir.Change
	}{{"first", first, firstCh}, {"second", second, secondCh}} {
		got := drainChanges(t, ctx, lane.ch, 2, 20*time.Second)
		if len(got) != 2 {
			t.Fatalf("%s reader on shared server id %d received %d of 2 inserts (stream error: %v)",
				lane.name, forced, len(got), lane.rdr.Err())
		}
	}
	after := sidDumpThreads(t, ctx, dsn)
	for id := range ours {
		if !after[id] {
			t.Fatalf("Binlog Dump thread %d is gone after the writes: the source evicted a reader sharing server id %d (and it reconnected)", id, forced)
		}
	}
}

// sidDumpThreads is the set of the source's Binlog Dump connection ids.
func sidDumpThreads(t *testing.T, ctx context.Context, dsn string) map[int64]bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, "SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'")
	if err != nil {
		t.Fatalf("read PROCESSLIST: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestCDCReader_ServerIDEqualToTheSourcesIsReplaced pins
// serverIDDistinctFromSource: a generated replica id that equals the
// source's own @@server_id (possible on a managed service, which assigns
// large ids) is replaced before the stream registers, and the stream reads.
func TestCDCReader_ServerIDEqualToTheSourcesIsReplaced(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	applyMySQL(t, dsn, `CREATE TABLE sid2 (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var source uint32
	if err := db.QueryRowContext(ctx, "SELECT @@server_id").Scan(&source); err != nil {
		t.Fatal(err)
	}
	r, err := Engine{Flavor: FlavorVanilla}.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	rdr := r.(*CDCReader)
	defer func() { _ = rdr.Close() }()
	rdr.serverID = source
	ch, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	if rdr.serverID == source {
		t.Fatalf("the reader registered with the source's own server_id %d", source)
	}
	time.Sleep(300 * time.Millisecond)
	applyMySQL(t, dsn, `INSERT INTO sid2 VALUES (1);`)
	if got := drainChanges(t, ctx, ch, 1, 20*time.Second); len(got) != 1 {
		t.Fatalf("received %d of 1 insert (stream error: %v)", len(got), rdr.Err())
	}
}

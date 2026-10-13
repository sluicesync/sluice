//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCDCReader_SameServerIDOnMySQL_BothStreamsKeepReading measures the
// MySQL arm of BINLOG-SERVER-ID-COLLISION. Two readers forced onto ONE
// server id stream from one MySQL 8 source; both must keep delivering
// every row, through a write made after both registered and another made
// later. MariaDB refuses the second registration loudly (ERROR 4052,
// observed in the pipeline's MariaDB cells); this pins that MySQL neither
// refuses it nor quietly kills the older dump thread — it tells the two
// apart by the syncer's random @replica_uuid — so a collision there was
// never a silent stall. If a MySQL release starts keying zombie-dump
// eviction on server_id alone, the first reader stops receiving and this
// fails.
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
	first, firstCh := open()
	second, secondCh := open()

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
}

//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 G6 — the binlog-filter preflight against real mysqlds, one
// container per filter ARM (the two flags cannot be exercised on one
// server: a non-empty do-list makes the server — and, mirroring it, the
// preflight — ignore the ignore-list entirely). Each arm pins both
// directions: the filtered synced database refuses at both CDC-open
// chokepoint families, and a scope the filter does not touch passes —
// the Bug 246 no-false-refusal half. What only a real server can pin
// here is the evidence plumbing itself: that the running mysqld
// actually surfaces the startup flags in the Binlog_Do_DB /
// Binlog_Ignore_DB columns of the master-status row the preflight
// scans (the arm logic is unit-pinned in
// cdc_binlog_db_filter_preflight_test.go).

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// dsnForDatabase rebinds the testcontainer DSN's database component.
func dsnForDatabase(t *testing.T, dsn, from, to string) string {
	t.Helper()
	out := strings.Replace(dsn, "/"+from+"?", "/"+to+"?", 1)
	if out == dsn {
		t.Fatalf("could not rebind DSN database %q→%q in %q", from, to, dsn)
	}
	return out
}

func streamOpenErr(t *testing.T, ctx context.Context, dsn string) error {
	t.Helper()
	eng := Engine{Flavor: FlavorVanilla}
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	t.Cleanup(func() { _ = rdr.(*CDCReader).Close() })
	_, err = rdr.(*CDCReader).StreamChanges(ctx, ir.Position{})
	return err
}

// TestCDCReader_BinlogDBFilterPreflight_IgnoreDB: --binlog-ignore-db on
// the synced database refuses; an untouched database passes.
func TestCDCReader_BinlogDBFilterPreflight_IgnoreDB(t *testing.T) {
	dsn, cleanup := startMySQLM2Preflight(t, "--binlog-ignore-db=source_db")
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		CREATE DATABASE unfiltered_db;
		CREATE TABLE unfiltered_db.t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("filtered_scope_refuses", func(t *testing.T) {
		err := streamOpenErr(t, ctx, dsn)
		wantCodedRefusal(t, err, sluicecode.CodeCDCBinlogDBFiltered, "StreamChanges")
		if !strings.Contains(err.Error(), `"source_db"`) || !strings.Contains(err.Error(), "--binlog-ignore-db") {
			t.Errorf("refusal must name the filtered database and the flag; got: %v", err)
		}

		if snap, err := (Engine{Flavor: FlavorVanilla}).OpenSnapshotStream(ctx, dsn); err == nil {
			_ = snap.Close()
			t.Fatal("OpenSnapshotStream: accepted a binlog-ignore-db'd source; want the coded refusal before any copy")
		} else {
			wantCodedRefusal(t, err, sluicecode.CodeCDCBinlogDBFiltered, "OpenSnapshotStream")
		}
	})

	t.Run("unfiltered_scope_passes", func(t *testing.T) {
		if err := streamOpenErr(t, ctx, dsnForDatabase(t, dsn, "source_db", "unfiltered_db")); err != nil {
			t.Fatalf("StreamChanges scoped to an unfiltered database = %v; want nil (a filter on an "+
				"unrelated database must not refuse — Bug 246 discipline)", err)
		}
	})
}

// TestCDCReader_BinlogDBFilterPreflight_DoDB: --binlog-do-db omitting
// the synced database refuses; a scope inside the do-list passes.
func TestCDCReader_BinlogDBFilterPreflight_DoDB(t *testing.T) {
	dsn, cleanup := startMySQLM2Preflight(t, "--binlog-do-db=whitelisted_db")
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		CREATE DATABASE whitelisted_db;
		CREATE TABLE whitelisted_db.t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("omitted_scope_refuses", func(t *testing.T) {
		err := streamOpenErr(t, ctx, dsn)
		wantCodedRefusal(t, err, sluicecode.CodeCDCBinlogDBFiltered, "StreamChanges")
		if !strings.Contains(err.Error(), `"source_db"`) || !strings.Contains(err.Error(), "--binlog-do-db") {
			t.Errorf("refusal must name the omitted database and the flag; got: %v", err)
		}
	})

	t.Run("whitelisted_scope_passes", func(t *testing.T) {
		if err := streamOpenErr(t, ctx, dsnForDatabase(t, dsn, "source_db", "whitelisted_db")); err != nil {
			t.Fatalf("StreamChanges scoped to a do-listed database = %v; want nil", err)
		}
	})
}

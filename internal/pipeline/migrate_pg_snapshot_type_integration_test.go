//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for sluice's PG `pg_snapshot` type stance (ADR-0051
// Stage 2 candidate, queued via the broader-mining review).
//
// `pg_snapshot` (PG 13+) is the modern replacement for txid_snapshot.
// Same wire-format `xmin:xmax:xip_list`, but the underlying XIDs are
// xid8 (64-bit). Used in audit logs that capture snapshot state.
//
// Same-shape pin as the money + xml + pg_lsn + txid_snapshot pins.
// Three outcomes; only silent flatten fails:
//
//	(a) Migrator refuses-loudly with pg_snapshot named.
//	(b) Target type is text/varchar — fail loudly with SILENT-TYPE-LOSS.
//	(c) Target preserves pg_snapshot AND a sample text-round-trips
//	    byte-equal.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_PgSnapshotTypeStance pins gap-list
// Stage-2 pg_snapshot. Either preserve (c) or refuse-loudly with the
// type named (a); silent flatten to text is the silent-type-loss class.
func TestMigrate_PostgresToPostgres_PgSnapshotTypeStance(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE snapshot_audit (
			id     BIGINT PRIMARY KEY,
			label  VARCHAR(64) NOT NULL,
			snap   PG_SNAPSHOT NOT NULL
		);

		INSERT INTO snapshot_audit (id, label, snap) VALUES
			(1, 'simple', '10:20:'::pg_snapshot),
			(2, 'with-xip', '100:200:100,150'::pg_snapshot);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if err := mig.Run(ctx); err != nil {
		errStr := err.Error()
		t.Logf("Migrator.Run returned: %v", err)
		hasContext := false
		for _, want := range []string{"pg_snapshot", "PG_SNAPSHOT", "snapshot", "type", "unsupported"} {
			if strings.Contains(errStr, want) {
				hasContext = true
				break
			}
		}
		if !hasContext {
			t.Errorf("Migrator.Run failed but the error doesn't name the pg_snapshot type / "+
				"unsupported-type shape; operators reading CI output need a hint.\n"+
				"got: %v", err)
		}
		return
	}

	target, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open target: %v", err)
	}
	defer func() { _ = target.Close() }()

	var typname string
	const colQ = `
		SELECT t.typname FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_type  t ON a.atttypid = t.oid
		WHERE c.relname = 'snapshot_audit' AND a.attname = 'snap' AND a.attnum > 0
	`
	if err := target.QueryRowContext(ctx, colQ).Scan(&typname); err != nil {
		t.Fatalf("query target snapshot_audit.snap type: %v", err)
	}

	var rowCount int
	if err := target.QueryRowContext(ctx, `SELECT count(*) FROM snapshot_audit`).Scan(&rowCount); err != nil {
		t.Fatalf("count target snapshot_audit rows: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("target snapshot_audit rows = %d; want 2 (the seed)", rowCount)
	}

	switch typname {
	case "pg_snapshot":
		var got string
		if err := target.QueryRowContext(
			ctx,
			`SELECT snap::text FROM snapshot_audit WHERE label = 'with-xip'`,
		).Scan(&got); err != nil {
			t.Fatalf("read target pg_snapshot value: %v", err)
		}
		if got != "100:200:100,150" {
			t.Errorf("pg_snapshot round-trip lost the value: got %q; want %q",
				got, "100:200:100,150")
		}
		t.Logf("path (c) — pg_snapshot preserved on target as typname=pg_snapshot (correctness baseline)")

	case "text", "varchar", "char", "bpchar":
		t.Errorf("SILENT-TYPE-LOSS: target snapshot_audit.snap has typname=%q "+
			"(want 'pg_snapshot' or a clean refuse-loudly). Silent map to %q is the "+
			"loud-failure-tenet regression this pin catches.",
			typname, typname)

	default:
		t.Errorf("unexpected target snapshot_audit.snap type: %q (want 'pg_snapshot' / refuse / a documented mapping)", typname)
	}
}

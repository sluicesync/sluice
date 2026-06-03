//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for sluice's PG `pg_lsn` type stance (ADR-0051
// Stage 2 candidate, queued via the broader-mining review).
//
// PG `pg_lsn` is the Log Sequence Number type — a 64-bit XLOG-byte
// position rendered in text as `XXX/YYY` (e.g. `0/16D0F90`). Rarely
// used as a user-data type, but PG operators sometimes store WAL
// positions in audit / replication-bookkeeping tables. Concerns:
//
//   - **Text I/O parity.** PG's text rendering is `0/16D0F90` (no
//     leading zeros in either half, slash separator). Any
//     intermediate normalisation to `'0/016D0F90'` or a `int8`
//     under-the-hood representation would change byte-equality on
//     round-trip.
//   - **Cross-engine.** No good MySQL mapping — closest is `BIGINT`
//     but loses the slash-rendered semantics.
//
// Documented outcomes mirror the money + xml pins shape:
//
//	(a) Migrator refuses-loudly with the pg_lsn type named.
//	(b) Migrator silently maps pg_lsn → BIGINT / TEXT on the target
//	    (typname='int8'/'text'). Silent type-loss → fail loudly.
//	(c) Migrator preserves pg_lsn on the target (typname='pg_lsn')
//	    AND the value text-round-trips through `::text`. Baseline.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_PgLsnTypeStance pins gap-list Stage-2
// pg_lsn. Either preserve pg_lsn (c) or refuse-loudly with the type
// named (a); silent flatten to int8/text is the silent-type-loss
// class the tenet refuses.
func TestMigrate_PostgresToPostgres_PgLsnTypeStance(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE wal_audit (
			id        BIGINT PRIMARY KEY,
			label     VARCHAR(64) NOT NULL,
			recorded  PG_LSN     NOT NULL
		);

		INSERT INTO wal_audit (id, label, recorded) VALUES
			(1, 'small',  '0/16D0F90'::pg_lsn),
			(2, 'med',    '1/A0B0C0D'::pg_lsn),
			(3, 'large',  'FFFFFFFF/FFFFFFFF'::pg_lsn);
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
		// Path (a) — refuse-loudly.
		errStr := err.Error()
		t.Logf("Migrator.Run returned: %v", err)
		hasContext := false
		for _, want := range []string{"pg_lsn", "PG_LSN", "type", "unsupported", "lsn"} {
			if strings.Contains(errStr, want) {
				hasContext = true
				break
			}
		}
		if !hasContext {
			t.Errorf("Migrator.Run failed but the error doesn't name the pg_lsn type / "+
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
		WHERE c.relname = 'wal_audit' AND a.attname = 'recorded' AND a.attnum > 0
	`
	if err := target.QueryRowContext(ctx, colQ).Scan(&typname); err != nil {
		t.Fatalf("query target wal_audit.recorded type: %v", err)
	}

	var rowCount int
	if err := target.QueryRowContext(ctx, `SELECT count(*) FROM wal_audit`).Scan(&rowCount); err != nil {
		t.Fatalf("count target wal_audit rows: %v", err)
	}
	if rowCount != 3 {
		t.Errorf("target wal_audit rows = %d; want 3 (the seed)", rowCount)
	}

	switch typname {
	case "pg_lsn":
		var got string
		if err := target.QueryRowContext(
			ctx,
			`SELECT recorded::text FROM wal_audit WHERE label = 'small'`,
		).Scan(&got); err != nil {
			t.Fatalf("read target pg_lsn value: %v", err)
		}
		// PG's pg_lsn text rendering omits leading zeros on both halves.
		// `0/16D0F90` should come back byte-equal.
		if got != "0/16D0F90" {
			t.Errorf("pg_lsn round-trip lost the value: got %q; want %q", got, "0/16D0F90")
		}
		t.Logf("path (c) — pg_lsn preserved on target as typname=pg_lsn (correctness baseline)")

	case "int8", "bigint", "text", "varchar":
		t.Errorf("SILENT-TYPE-LOSS: target wal_audit.recorded has typname=%q "+
			"(want 'pg_lsn' or a clean refuse-loudly). Sluice's Stage 2 deferral list "+
			"says: 'each has a known text-IO / locale / dialect concern worth a per-type "+
			"round-trip integration test before adding to the allowlist.' Silent map to %q is the "+
			"loud-failure-tenet regression this pin catches.",
			typname, typname)

	default:
		t.Errorf("unexpected target wal_audit.recorded type: %q (want 'pg_lsn' / refuse / a documented mapping)", typname)
	}
}

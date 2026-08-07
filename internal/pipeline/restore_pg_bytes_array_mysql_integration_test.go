//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 232 — the RESTORE lane's half of the `json[]` / `jsonb[]` /
// `bytea[]` array-element story.
//
// The migrate sibling next door (migrate_pg_bytes_array_mysql_integration_test.go)
// pinned the family × shape matrix through `sluice migrate`. `restore`
// carries the same values on the same writer, but reaches it through a
// different schema: the manifest's source-native IR put through
// translate.RetargetForEngine, which rewrites `ir.Array{Element: X}` to
// the MySQL-shaped `ir.JSON{}` — and, before v0.116.1, dropped X. The
// element family was then unresolvable at the leaf, so v0.116.0 refused
// EVERY json[]/jsonb[]/bytea[] column with
// SLUICE-E-VALUE-UNREPRESENTABLE and restored 0 rows, on chains taken by
// any release. The three string- and native-leaf families in the same
// fixture restored fine, which is why nothing caught it: only the two
// []byte-leaf families dispatch on the element type at all.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// This file asserts through [assertBytesArrayMatrix], the migrate
// sibling's helper, unchanged: every comparison is against a rendering
// PostgreSQL itself produced from the SOURCE row — `j[i]::text`,
// `encode(b[i],'base64')`, `array_dims` — never against sluice's own
// reporting, its row counts, or the archive's recorded count. Sharing
// the helper is deliberate and is what makes the gate a PARITY gate: the
// same source column must land the same stored value through `migrate`
// and through `backup` + `restore`, and the assertion cannot drift
// between the two lanes because there is only one of it.
//
// # What this reaches, stated so the name cannot be read as broader
//
// The `restore` bulk-load door, PG source → MySQL target, single full
// manifest. NOT the chain-restore INCREMENTAL replay leg, which goes
// through ir.ChangeApplier.ApplyBatch with descriptors read from the
// target and therefore genuinely cannot resolve the element family —
// that lane still refuses, by design, and now says so without claiming
// to be a continuous sync (see refuseUnresolvedArrayLeafOnApplier in
// internal/engines/mysql).
//
// # Older chains
//
// The manifest wire has never carried ir.Column.SourceColumnType and the
// archive format is unchanged, so a chain written by an older binary is
// shape-identical here to one this test writes; the restore lane
// re-derives the provenance locally from the manifest's source-native
// IR. Verified out-of-band against a real v0.115.0-written chain restored
// by the fixed binary (Bug 232's reported population).
package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestRestore_PGToMySQL_ArrayElementFamiliesMatchMigrate backs up the
// migrate sibling's fixture from PostgreSQL and restores it into MySQL,
// then runs that sibling's full family × shape matrix against the
// result.
func TestRestore_PGToMySQL_ArrayElementFamiliesMatchMigrate(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bytesArrayMySQLSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: pgSource, Store: store, SluiceVersion: "test",
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run (PG): %v", err)
	}

	if err := (&backup.Restore{
		Target: myEng, TargetDSN: mysqlTarget, Store: store,
	}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run (PG chain → MySQL): %v — this is Bug 232's shape if it names "+
			"SLUICE-E-VALUE-UNREPRESENTABLE: the manifest records the array element type and the "+
			"cross-engine retarget dropped it before the writer asked", err)
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The restore must have loaded EVERY row, not merely exited 0 — a
	// refusal that stopped at the first offending column left the table
	// created and empty, which several of the assertions below would read
	// as a query error rather than as the defect.
	if got := mysqlString(t, ctx, db, "SELECT CAST(COUNT(*) AS CHAR) FROM bytesarr"); got != "8" {
		t.Fatalf("restored row count = %s; want the fixture's 8 (0 is Bug 232's signature: the table is "+
			"created and the rows are refused)", got)
	}

	assertBytesArrayMatrix(t, ctx, pgSource, db)
}

// TestRestore_PGToMySQL_ArrayRefusalNeverPrescribesTheSyncLane is the
// message half of Bug 232, pinned where the operator actually meets it.
//
// v0.116.0's restore answered with "…so this is the continuous-sync
// lane… Remedies: … or use `sluice migrate` for a one-shot copy", during
// a restore, which has no CDC lane and cannot be replaced by `migrate`
// (that command cannot read an archive). The fix makes the value land,
// so the assertion here is the strong one: nothing on this lane emits
// that text at all.
//
// It is a REGRESSION pin rather than a live-error pin — there is no
// input that still reaches the refusal through this door — so it grades
// the run's own output. The narrower unit-level pins on which refusal
// fires for which column shape live in internal/engines/mysql
// (TestArrayLeafForJSON_ArrayColumnWithNoElementDoesNotClaimTheApplierLane).
func TestRestore_PGToMySQL_ArrayRefusalNeverPrescribesTheSyncLane(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE arrfam (id INT PRIMARY KEY, j json[], b jsonb[], y bytea[]);
		INSERT INTO arrfam VALUES (1, ARRAY[$${"a":1}$$::json], ARRAY[$${"a":1}$$::jsonb], ARRAY['\xdead'::bytea]);
	`)

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: pgSource, Store: store, SluiceVersion: "test",
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	err = (&backup.Restore{Target: myEng, TargetDSN: mysqlTarget, Store: store}).Run(ctx)
	if err == nil {
		return // the fixed behaviour: the values land and there is no message to grade
	}
	for _, forbidden := range []string{
		"this is the continuous-sync lane",
		"NOT supported by continuous sync",
		"resumed WARM",
	} {
		if strings.Contains(err.Error(), forbidden) {
			t.Errorf("a `restore` failed with continuous-sync advice (%q), which is the Bug 232 message "+
				"defect — a restore has no CDC lane:\n%v", forbidden, err)
		}
	}
	t.Fatalf("restore of a json[]/jsonb[]/bytea[] chain into MySQL failed: %v", err)
}

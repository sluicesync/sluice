//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cursor-store fidelity pins for the migrate copy-resume path (audit
// 2026-07-15 CRITICAL-2 / HIGH-1).
//
// The migrate copy-resume LastPK rides the same ir.TableProgress JSON
// store the backfill cursor does, via migcore.PKTracker — so it shared
// both silent-loss classes: []byte PK cursors mangled by plain JSON
// (base64 encoding / U+FFFD replacement) and int64 cursors above 2^53
// drifting through float64. These pins drive Migrator end-to-end on a
// real PG against both classes, plus the legacy-poisoned-cursor
// quarantine (degrade to truncate-and-redo, never resume from a lie).

package pipeline

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// byteaIDHex returns row g's 6-byte bytea PK as hex: a first byte in
// the invalid-UTF-8 range (0x80+g) followed by the audit's observed
// mangled tail 0x9F8041FE10.
func byteaIDHex(g int) string {
	return fmt.Sprintf("%02x9f8041fe10", 0x80+g)
}

// md5OfByteaIDs returns an order-stable digest of a table's bytea ids.
func md5OfByteaIDs(t *testing.T, dsn, table string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var digest string
	if err := db.QueryRow(
		"SELECT md5(string_agg(encode(id, 'hex'), ',' ORDER BY id)) FROM " + table,
	).Scan(&digest); err != nil {
		t.Fatalf("digest %s: %v", table, err)
	}
	return digest
}

// seedByteaTable creates table on dsn with n bytea-PK rows (row 1..n).
func seedByteaTable(t *testing.T, dsn, table string, n int) {
	t.Helper()
	ddl := "CREATE TABLE " + table + " (id BYTEA PRIMARY KEY, name VARCHAR(64) NOT NULL);"
	values := make([]string, 0, n)
	for g := 1; g <= n; g++ {
		values = append(values, fmt.Sprintf("('\\x%s'::bytea, 'b_%d')", byteaIDHex(g), g))
	}
	ddl += "INSERT INTO " + table + " (id, name) VALUES " + strings.Join(values, ", ") + ";"
	applyPGDDL(t, dsn, ddl)
}

// TestMigrate_ResumeBatched_ByteaCursorRoundTrip plants a synthesized
// in-progress state whose cursor is the 50th row's RAW bytes — every
// id an invalid-UTF-8 sequence containing the audit's observed
// 0x9F8041FE10 — and resumes. Before the cursor envelope, the stored
// cursor came back U+FFFD-mangled (bytewise GREATER than the true
// cursor) and the resumed read silently skipped the tail range; the
// target must converge to the exact source id set.
func TestMigrate_ResumeBatched_ByteaCursorRoundTrip(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	seedByteaTable(t, sourceDSN, "binitems", 100)
	// The "previous attempt": table + first 50 rows already on target.
	seedByteaTable(t, targetDSN, "binitems", 50)

	cursor := []byte{0x80 + 50, 0x9F, 0x80, 0x41, 0xFE, 0x10}
	seedStateRow(t, targetDSN, "bytea-cursor", ir.MigrationPhaseBulkCopy,
		map[string]ir.TableProgress{
			"binitems": {State: ir.TableProgressInProgress, LastPK: []any{cursor}, RowsCopied: 50},
		})

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mig := &Migrator{
		Source:        pgEng,
		Target:        pgEng,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		MigrationID:   "bytea-cursor",
		Resume:        true,
		BulkBatchSize: 10,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	if got := countRows(t, targetDSN, "binitems"); got != 100 {
		t.Errorf("binitems row count = %d; want 100 (a mangled cursor skips the tail)", got)
	}
	if src, dst := md5OfByteaIDs(t, sourceDSN, "binitems"), md5OfByteaIDs(t, targetDSN, "binitems"); src != dst {
		t.Errorf("id-set digest mismatch: source %s, target %s", src, dst)
	}
	if state := readState(t, targetDSN, "bytea-cursor"); state.Phase != ir.MigrationPhaseComplete {
		t.Errorf("final phase = %q; want complete", state.Phase)
	}
}

// TestMigrate_ResumeBatched_LargeIntCursorExact is the HIGH-1 pin on
// the migrate path: BIGINT PKs above 2^53 spaced 2 apart (odd values a
// float64 pass collapses onto their even neighbours), with the
// synthesized cursor at the 50th row. A drifting cursor either skips
// the row float64 rounds past or replays far behind; the exact final
// id range pins both away.
func TestMigrate_ResumeBatched_LargeIntCursorExact(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE bigids (
			id   BIGINT PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO bigids (id, name)
			SELECT 9007199254740993 + 2*g, 'p_' || g::text FROM generate_series(1, 100) g;
	`
	applyPGDDL(t, sourceDSN, seedDDL)
	const targetSeedDDL = `
		CREATE TABLE bigids (
			id   BIGINT PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO bigids (id, name)
			SELECT 9007199254740993 + 2*g, 'p_' || g::text FROM generate_series(1, 50) g;
	`
	applyPGDDL(t, targetDSN, targetSeedDDL)

	seedStateRow(t, targetDSN, "bigint-cursor", ir.MigrationPhaseBulkCopy,
		map[string]ir.TableProgress{
			"bigids": {State: ir.TableProgressInProgress, LastPK: []any{int64(9007199254740993 + 2*50)}, RowsCopied: 50},
		})

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mig := &Migrator{
		Source:        pgEng,
		Target:        pgEng,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		MigrationID:   "bigint-cursor",
		Resume:        true,
		BulkBatchSize: 10,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	if got := countRows(t, targetDSN, "bigids"); got != 100 {
		t.Errorf("bigids row count = %d; want 100 (a drifted cursor skips or replays)", got)
	}
	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var minID, maxID int64
	if err := db.QueryRow("SELECT MIN(id), MAX(id) FROM bigids").Scan(&minID, &maxID); err != nil {
		t.Fatalf("min/max: %v", err)
	}
	if minID != 9007199254740995 || maxID != 9007199254740993+200 {
		t.Errorf("id range = [%d, %d]; want [9007199254740995, %d]", minID, maxID, 9007199254740993+200)
	}
}

// TestMigrate_ResumeBatched_LegacyPoisonedCursorQuarantines pins the
// legacy healing path: a progress row hand-rewritten to the
// pre-envelope wire shape — the bytea cursor stored as its base64
// string, exactly what plain json.Marshal([]byte) produced — must NOT
// be resumed from (the string is garbage bytes); the quarantine
// degrades the table to truncate-and-redo and the copy converges to
// the exact source id set anyway.
func TestMigrate_ResumeBatched_LegacyPoisonedCursorQuarantines(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	seedByteaTable(t, sourceDSN, "binlegacy", 100)
	seedByteaTable(t, targetDSN, "binlegacy", 50)

	// Plant a normal state row, then rewrite its progress JSON to the
	// legacy shape through the control table directly.
	seedStateRow(t, targetDSN, "bytea-legacy", ir.MigrationPhaseBulkCopy,
		map[string]ir.TableProgress{
			"binlegacy": {State: ir.TableProgressInProgress, LastPK: []any{[]byte{0x01}}, RowsCopied: 50},
		})
	cursorBytes := []byte{0x80 + 50, 0x9F, 0x80, 0x41, 0xFE, 0x10}
	legacyJSON := fmt.Sprintf(`{"state":"in_progress","last_pk":[%q],"rows_copied":50}`,
		base64.StdEncoding.EncodeToString(cursorBytes))
	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		"UPDATE sluice_migrate_table_progress SET progress = $1 WHERE migration_id = $2 AND table_name = $3",
		legacyJSON, "bytea-legacy", "binlegacy",
	); err != nil {
		t.Fatalf("rewrite progress row to legacy shape: %v", err)
	}

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mig := &Migrator{
		Source:        pgEng,
		Target:        pgEng,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		MigrationID:   "bytea-legacy",
		Resume:        true,
		BulkBatchSize: 10,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	if got := countRows(t, targetDSN, "binlegacy"); got != 100 {
		t.Errorf("binlegacy row count = %d; want 100 (quarantine must re-copy, not resume from base64 garbage)", got)
	}
	if src, dst := md5OfByteaIDs(t, sourceDSN, "binlegacy"), md5OfByteaIDs(t, targetDSN, "binlegacy"); src != dst {
		t.Errorf("id-set digest mismatch: source %s, target %s", src, dst)
	}
}

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
//
// The M0.2 fast-follow (roadmap item 72) extends the matrix along the
// axes the first cut left unexercised:
//
//   - the REAL MySQL target (go-sql-driver binds and scans PK cursor
//     values through a different wire path than pgx — the Bug-74
//     lesson pins the target driver, not just the shared codec),
//     including the legacy bare-uint64 ParseUint recovery over a
//     BIGINT UNSIGNED PK;
//   - the temporal-PK "time" envelope, the one wire kind only migrate
//     persists (the backfill executors normalise temporals to text
//     before the store, so no backfill pin can reach it);
//   - the CHUNKED resume path, whose envelope-bearing chunk BOUNDS
//     (LowerPK/UpperPK) were covered only at unit level.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
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

// ============================================================
// M0.2 fast-follow pins (roadmap item 72): MySQL target, temporal
// cursors, chunked bounds, legacy bare uint64.
// ============================================================

// seedStateRowMySQL is [seedStateRow] for a MySQL target: it writes
// the synthesized "previous attempt" through the real mysql
// MigrationStateStore, so the planted cursors ride the exact envelope
// encode/decode the production checkpoint path uses.
func seedStateRowMySQL(t *testing.T, dsn, migrationID string, phase ir.MigrationPhase, progress map[string]ir.TableProgress) {
	t.Helper()

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	opener, ok := myEng.(ir.MigrationStateStoreOpener)
	if !ok {
		t.Fatal("mysql engine doesn't implement MigrationStateStoreOpener")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := opener.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := store.Write(ctx, ir.MigrationState{
		MigrationID:   migrationID,
		Phase:         phase,
		TableProgress: progress,
	}); err != nil {
		t.Fatalf("write state: %v", err)
	}
}

// bfBinIDBytes is [bfBinIDHex] decoded to its raw 16 bytes — the
// planted-cursor twin of the hex literal the seeds embed.
func bfBinIDBytes(t *testing.T, i int) []byte {
	t.Helper()
	b, err := hex.DecodeString(bfBinIDHex(i))
	if err != nil {
		t.Fatalf("decode bin id %d: %v", i, err)
	}
	return b
}

// byteaIDBytes is [byteaIDHex] as raw bytes: row g's 6-byte invalid-
// UTF-8 PK (0x80+g then the audit's observed tail 0x9F8041FE10).
func byteaIDBytes(g int) []byte {
	return []byte{byte(0x80 + g), 0x9F, 0x80, 0x41, 0xFE, 0x10}
}

// pkTextsOrdered returns the table's `id` PK rendered to text via
// expr, ordered by id — the order-stable ground truth for exact id-set
// comparison between source and target (a mangled or drifted cursor
// skips PK ranges, which surfaces as a shorter or divergent list).
func pkTextsOrdered(t *testing.T, driver, dsn, table, expr string) []string {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query("SELECT " + expr + " FROM " + table + " ORDER BY id")
	if err != nil {
		t.Fatalf("read %s ids: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan %s id: %v", table, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s ids: %v", table, err)
	}
	return out
}

// assertSamePKTexts fails when the two ordered id lists differ, naming
// the first divergent row (loud failure names the row, per the tenet).
func assertSamePKTexts(t *testing.T, table string, src, dst []string) {
	t.Helper()
	if len(src) != len(dst) {
		t.Errorf("%s: source has %d ids, target has %d (a corrupted cursor skips PK ranges)", table, len(src), len(dst))
		return
	}
	for i := range src {
		if src[i] != dst[i] {
			t.Errorf("%s: id mismatch at ordered row %d: source %q, target %q", table, i, src[i], dst[i])
			return
		}
	}
}

// TestMigrate_ResumeBatched_MySQLTargetCursorFidelity re-runs the
// resume-cursor fidelity matrix against a REAL MySQL target. The PG
// pins above prove the shared envelope codec; this pins the OTHER
// shipping driver (Bug-74: go-sql-driver scans and binds PK values
// through its own wire path, so a green pgx pin does not cover it).
// One container, one migration, four tables — one per envelope wire
// kind whose driver round-trip differs:
//
//   - VARBINARY(16) PK whose bytes are invalid UTF-8 (bytes envelope;
//     the CRITICAL-2 mangling class);
//   - BIGINT PK straddling 2^53, odd-spaced so any float64 pass
//     collapses neighbours (i64 envelope; the HIGH-1 drift);
//   - DATETIME(6) PK with sub-second precision (time envelope — the
//     one wire kind only migrate persists);
//   - BIGINT UNSIGNED PK above MaxInt64 whose progress row is
//     REWRITTEN to the pre-envelope bare-number wire shape — exactly
//     what an old binary's json.Marshal(uint64) wrote — pinning the
//     v0.99.257 ParseUint legacy recovery end-to-end: the cursor must
//     decode exactly (not drift through ParseFloat) and must NOT trip
//     the float-over-integer quarantine.
//
// Each table mirrors an interrupt at row 50 of 100. The resumed run
// must pick up every cursor (log-asserted: resumed from cursor, never
// truncate-and-redo — a quarantine would ALSO converge, masking a
// broken decode) and converge to the exact source id set.
func TestMigrate_ResumeBatched_MySQLTargetCursorFidelity(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const ubigBase uint64 = 18446744073709551515 // +100 == MaxUint64
	timeBase := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tables := []struct {
		name   string
		pkDDL  string
		rowLit func(g int) string
		pkText string
		cursor []any // planted row-50 cursor; nil ⇒ legacy rewrite below
	}{
		{
			name:   "binitems16",
			pkDDL:  "id VARBINARY(16) PRIMARY KEY",
			rowLit: func(g int) string { return fmt.Sprintf("(X'%s', 'b_%d')", bfBinIDHex(g), g) },
			pkText: "HEX(id)",
			cursor: []any{bfBinIDBytes(t, 50)},
		},
		{
			name:   "bigids",
			pkDDL:  "id BIGINT PRIMARY KEY",
			rowLit: func(g int) string { return fmt.Sprintf("(%d, 'p_%d')", 9007199254740993+2*int64(g), g) },
			pkText: "CAST(id AS CHAR)",
			cursor: []any{int64(9007199254740993 + 2*50)},
		},
		{
			name:  "timeids",
			pkDDL: "id DATETIME(6) PRIMARY KEY",
			rowLit: func(g int) string {
				ts := timeBase.Add(time.Duration(g)*time.Second + time.Duration(g)*time.Microsecond)
				return fmt.Sprintf("('%s', 't_%d')", ts.Format("2006-01-02 15:04:05.000000"), g)
			},
			pkText: "DATE_FORMAT(id, '%Y-%m-%d %H:%i:%s.%f')",
			cursor: []any{timeBase.Add(50*time.Second + 50*time.Microsecond)},
		},
		{
			name:   "ubigids",
			pkDDL:  "id BIGINT UNSIGNED PRIMARY KEY",
			rowLit: func(g int) string { return fmt.Sprintf("(%d, 'u_%d')", ubigBase+uint64(g), g) },
			pkText: "CAST(id AS CHAR)",
			cursor: nil, // placeholder written, then rewritten to the legacy shape
		},
	}

	progress := make(map[string]ir.TableProgress, len(tables))
	for _, tb := range tables {
		var src, dst strings.Builder
		fmt.Fprintf(&src, "CREATE TABLE %s (%s, name VARCHAR(64) NOT NULL);", tb.name, tb.pkDDL)
		fmt.Fprintf(&dst, "CREATE TABLE %s (%s, name VARCHAR(64) NOT NULL);", tb.name, tb.pkDDL)
		srcVals := make([]string, 0, 100)
		dstVals := make([]string, 0, 50)
		for g := 1; g <= 100; g++ {
			srcVals = append(srcVals, tb.rowLit(g))
			if g <= 50 {
				// The "previous attempt": first 50 rows already on target.
				dstVals = append(dstVals, tb.rowLit(g))
			}
		}
		fmt.Fprintf(&src, "INSERT INTO %s VALUES %s;", tb.name, strings.Join(srcVals, ", "))
		fmt.Fprintf(&dst, "INSERT INTO %s VALUES %s;", tb.name, strings.Join(dstVals, ", "))
		applyMySQLDDL(t, sourceDSN, src.String())
		applyMySQLDDL(t, targetDSN, dst.String())

		cursor := tb.cursor
		if cursor == nil {
			cursor = []any{uint64(1)}
		}
		progress[tb.name] = ir.TableProgress{State: ir.TableProgressInProgress, LastPK: cursor, RowsCopied: 50}
	}
	seedStateRowMySQL(t, targetDSN, "mysql-cursor-fidelity", ir.MigrationPhaseBulkCopy, progress)

	// Rewrite ubigids' progress row to the pre-envelope wire shape: the
	// cursor as a plain JSON number above MaxInt64.
	db, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	legacyJSON := fmt.Sprintf(`{"state":"in_progress","last_pk":[%d],"rows_copied":50}`, ubigBase+50)
	res, err := db.Exec(
		"UPDATE sluice_migrate_table_progress SET progress = ? WHERE migration_id = ? AND table_name = ?",
		legacyJSON, "mysql-cursor-fidelity", "ubigids",
	)
	if err != nil {
		t.Fatalf("rewrite ubigids progress row to legacy shape: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("rewrite ubigids progress row: %d rows affected; want 1", n)
	}

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	logs := captureSlog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	mig := &Migrator{
		Source:        myEng,
		Target:        myEng,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		MigrationID:   "mysql-cursor-fidelity",
		Resume:        true,
		BulkBatchSize: 10,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// Every table must take the cursor path. Truncate-and-redo would
	// also converge to the right data, so the data assertions alone
	// cannot distinguish a working cursor decode from a quarantined one.
	out := logs.String()
	if got := strings.Count(out, "resuming table from cursor"); got != len(tables) {
		t.Errorf("resumed-from-cursor count = %d; want %d (every table must resume from its cursor)", got, len(tables))
	}
	if strings.Contains(out, "truncating in-progress table for resume") {
		t.Error("a table degraded to truncate-and-redo; the pin requires the cursor path")
	}
	if strings.Contains(out, "not trustworthy") {
		t.Error("a planted cursor tripped the legacy quarantine; it must decode clean")
	}

	for _, tb := range tables {
		t.Run(tb.name, func(t *testing.T) {
			src := pkTextsOrdered(t, "mysql", sourceDSN, tb.name, tb.pkText)
			dst := pkTextsOrdered(t, "mysql", targetDSN, tb.name, tb.pkText)
			if len(src) != 100 {
				t.Fatalf("source has %d rows; want 100 (seed broke)", len(src))
			}
			assertSamePKTexts(t, tb.name, src, dst)
		})
	}

	var phase string
	if err := db.QueryRow("SELECT phase FROM sluice_migrate_state WHERE migration_id = ?", "mysql-cursor-fidelity").Scan(&phase); err != nil {
		t.Fatalf("read final phase: %v", err)
	}
	if phase != string(ir.MigrationPhaseComplete) {
		t.Errorf("final phase = %q; want complete", phase)
	}
}

// TestMigrate_ResumeBatched_TemporalCursorRoundTrip pins the "time"
// envelope on the migrate resume path against real PG. Migrate is the
// only writer that persists time.Time cursor values (the backfill
// executors normalise temporals to text before the store), so this is
// the one wire kind no backfill pin can reach. The planted cursor is
// row 50's TIMESTAMP with a sub-second component; a lossy round-trip
// (dropped microseconds, a timezone shift, a string re-type) misplaces
// the cursor and the resumed walk skips or replays rows.
func TestMigrate_ResumeBatched_TemporalCursorRoundTrip(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE timeitems (
			id   TIMESTAMP PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO timeitems (id, name)
			SELECT timestamp '2024-01-15 10:30:00'
			       + g * interval '1 second' + g * interval '1 microsecond',
			       't_' || g::text
			FROM generate_series(1, 100) g;
	`
	applyPGDDL(t, sourceDSN, seedDDL)
	const targetSeedDDL = `
		CREATE TABLE timeitems (
			id   TIMESTAMP PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO timeitems (id, name)
			SELECT timestamp '2024-01-15 10:30:00'
			       + g * interval '1 second' + g * interval '1 microsecond',
			       't_' || g::text
			FROM generate_series(1, 50) g;
	`
	applyPGDDL(t, targetDSN, targetSeedDDL)

	// Row 50: base + 50s + 50µs. UTC, matching what pgx scans for a
	// TIMESTAMP column — the same shape a real interrupted run persists.
	cursor := time.Date(2024, 1, 15, 10, 30, 50, 50_000, time.UTC)
	seedStateRow(t, targetDSN, "temporal-cursor", ir.MigrationPhaseBulkCopy,
		map[string]ir.TableProgress{
			"timeitems": {State: ir.TableProgressInProgress, LastPK: []any{cursor}, RowsCopied: 50},
		})

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mig := &Migrator{
		Source:        pgEng,
		Target:        pgEng,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		MigrationID:   "temporal-cursor",
		Resume:        true,
		BulkBatchSize: 10,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	out := logs.String()
	if !strings.Contains(out, "resuming table from cursor") {
		t.Error("expected 'resuming table from cursor'; the temporal cursor was not resumed from")
	}
	if strings.Contains(out, "truncating in-progress table for resume") {
		t.Error("table degraded to truncate-and-redo; the pin requires the cursor path")
	}

	const usText = "to_char(id, 'YYYY-MM-DD HH24:MI:SS.US')"
	src := pkTextsOrdered(t, "pgx", sourceDSN, "timeitems", usText)
	dst := pkTextsOrdered(t, "pgx", targetDSN, "timeitems", usText)
	if len(src) != 100 {
		t.Fatalf("source has %d rows; want 100 (seed broke)", len(src))
	}
	assertSamePKTexts(t, "timeitems", src, dst)
	if state := readState(t, targetDSN, "temporal-cursor"); state.Phase != ir.MigrationPhaseComplete {
		t.Errorf("final phase = %q; want complete", state.Phase)
	}
}

// TestMigrate_ResumeChunked_EnvelopeChunkBounds pins the parallel
// (chunked) resume path end-to-end on real PG — the path
// classifyTableForResume routes to resumeActionResumeChunked, covered
// only at unit level before this pin. Unlike the single-cursor pins
// above, the persisted state here carries envelope-bearing chunk
// BOUNDS (LowerPK/UpperPK), not just cursors, and resolveChunks reuses
// them verbatim on resume — so a bound that fails to round-trip
// mis-ranges every batch of the chunk, silently skipping the PK range
// between the true and the mangled bound. Two tables cover the two
// envelope families production chunk planners emit:
//
//   - chunkbin: BYTEA keyset bounds (ADR-0096 — binary PKs are
//     keyset-chunkable) whose bytes are invalid UTF-8;
//   - chunkbig: BIGINT MIN/MAX bounds above 2^53, odd-spaced.
//
// The planted layout mirrors a crash mid-parallel-copy: 2 chunks per
// table, chunk 0 checkpointed at row 30 of its 60-row range, chunk 1
// at row 80 of its 40-row range; the target holds exactly those
// prefixes. The resume must honour the recorded bounds and converge
// both tables to the exact source id set.
func TestMigrate_ResumeChunked_EnvelopeChunkBounds(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	bigID := func(g int) int64 { return 9007199254740993 + 2*int64(g) }

	// chunkbin — source: 100 bytea rows; target: the two resumed
	// prefixes (rows 1..30 for chunk 0, rows 61..80 for chunk 1).
	seedByteaTable(t, sourceDSN, "chunkbin", 100)
	{
		ddl := "CREATE TABLE chunkbin (id BYTEA PRIMARY KEY, name VARCHAR(64) NOT NULL);"
		var values []string
		for g := 1; g <= 30; g++ {
			values = append(values, fmt.Sprintf("('\\x%s'::bytea, 'b_%d')", byteaIDHex(g), g))
		}
		for g := 61; g <= 80; g++ {
			values = append(values, fmt.Sprintf("('\\x%s'::bytea, 'b_%d')", byteaIDHex(g), g))
		}
		ddl += "INSERT INTO chunkbin (id, name) VALUES " + strings.Join(values, ", ") + ";"
		applyPGDDL(t, targetDSN, ddl)
	}

	// chunkbig — same layout over BIGINT ids above 2^53.
	applyPGDDL(t, sourceDSN, `
		CREATE TABLE chunkbig (
			id   BIGINT PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO chunkbig (id, name)
			SELECT 9007199254740993 + 2*g, 'p_' || g::text FROM generate_series(1, 100) g;
	`)
	applyPGDDL(t, targetDSN, `
		CREATE TABLE chunkbig (
			id   BIGINT PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		);
		INSERT INTO chunkbig (id, name)
			SELECT 9007199254740993 + 2*g, 'p_' || g::text FROM generate_series(1, 30) g;
		INSERT INTO chunkbig (id, name)
			SELECT 9007199254740993 + 2*g, 'p_' || g::text FROM generate_series(61, 80) g;
	`)

	// Chunk layout: (nil, id60] and (id60, nil) — the shape
	// ComputeChunkBoundaries / ComputeKeysetChunkBoundaries persist
	// (chunk 0's LowerPK nil, last chunk's UpperPK nil, LowerPK
	// exclusive / UpperPK inclusive).
	seedStateRow(t, targetDSN, "chunk-bounds", ir.MigrationPhaseBulkCopy,
		map[string]ir.TableProgress{
			"chunkbin": {
				State: ir.TableProgressInProgress,
				Chunks: []ir.TableChunkProgress{
					{ChunkIndex: 0, UpperPK: []any{byteaIDBytes(60)}, LastPK: []any{byteaIDBytes(30)}, RowsCopied: 30, State: ir.TableProgressInProgress},
					{ChunkIndex: 1, LowerPK: []any{byteaIDBytes(60)}, LastPK: []any{byteaIDBytes(80)}, RowsCopied: 20, State: ir.TableProgressInProgress},
				},
			},
			"chunkbig": {
				State: ir.TableProgressInProgress,
				Chunks: []ir.TableChunkProgress{
					{ChunkIndex: 0, UpperPK: []any{bigID(60)}, LastPK: []any{bigID(30)}, RowsCopied: 30, State: ir.TableProgressInProgress},
					{ChunkIndex: 1, LowerPK: []any{bigID(60)}, LastPK: []any{bigID(80)}, RowsCopied: 20, State: ir.TableProgressInProgress},
				},
			},
		})

	pgEng, _ := engines.Get("postgres")
	logs := captureSlog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	mig := &Migrator{
		Source:          pgEng,
		Target:          pgEng,
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		MigrationID:     "chunk-bounds",
		Resume:          true,
		BulkBatchSize:   10,
		BulkParallelism: 2,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// Both tables must take the CHUNKED path from the recorded bounds —
	// a fallback to single-reader or truncate-and-redo would also
	// converge, masking a broken bound decode.
	out := logs.String()
	if got := strings.Count(out, "resuming parallel copy from per-chunk cursors"); got != 2 {
		t.Errorf("chunked-resume count = %d; want 2 (both tables must resume from recorded chunks)", got)
	}
	if strings.Contains(out, "truncating in-progress table for resume") {
		t.Error("a table degraded to truncate-and-redo; the pin requires the chunked path")
	}
	if strings.Contains(out, "not trustworthy") {
		t.Error("a planted chunk bound tripped the legacy quarantine; it must decode clean")
	}

	t.Run("chunkbin", func(t *testing.T) {
		src := pkTextsOrdered(t, "pgx", sourceDSN, "chunkbin", "encode(id, 'hex')")
		dst := pkTextsOrdered(t, "pgx", targetDSN, "chunkbin", "encode(id, 'hex')")
		if len(src) != 100 {
			t.Fatalf("source has %d rows; want 100 (seed broke)", len(src))
		}
		assertSamePKTexts(t, "chunkbin", src, dst)
	})
	t.Run("chunkbig", func(t *testing.T) {
		src := pkTextsOrdered(t, "pgx", sourceDSN, "chunkbig", "id::text")
		dst := pkTextsOrdered(t, "pgx", targetDSN, "chunkbig", "id::text")
		if len(src) != 100 {
			t.Fatalf("source has %d rows; want 100 (seed broke)", len(src))
		}
		assertSamePKTexts(t, "chunkbig", src, dst)
	})

	if state := readState(t, targetDSN, "chunk-bounds"); state.Phase != ir.MigrationPhaseComplete {
		t.Errorf("final phase = %q; want complete", state.Phase)
	}
}

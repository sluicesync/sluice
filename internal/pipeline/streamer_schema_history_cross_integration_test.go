//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0049 Chunk E — cross-engine end-to-end pin (the v0.70.0
// release-note headline).
//
// THE invariant under pin: a mid-stream ALTER TABLE on the MySQL
// source emits an ir.SchemaSnapshot via the binlog QUERY-event path
// (Chunk B1), persists the IR table into the PG target's
// sluice_cdc_schema_history in the SAME tx as the ADR-0007 position
// write (locked decision #4a), and on warm-resume the applier's
// active-version cache is primed from that history via
// PrimeSchemaHistoryCache (Chunk C) — so a resumed cross-engine
// stream NEVER falls through to the loud ADR-0022 cold-start re-
// snapshot just because a DDL happened.
//
// This is the cross-engine live-CDC counterpart to Chunk D's
// backup/restore pin (TestIncrementalBackup_PostgresChainRestore_
// SchemaHistoryReplay in incremental_pg_integration_test.go). The
// two together cover the two operator-visible recovery surfaces the
// ADR-0049 schema-history is load-bearing for:
//
//   - **Chunk D pin**: take a backup across a DDL, restore the chain,
//     resume from the restored target. (Backup/restore loop.)
//   - **Chunk E pin (this file)**: keep a live stream running across
//     a DDL, kill it, warm-resume from the persisted position.
//     (Live-CDC restart loop — what an operator does after every
//     deploy.)
//
// They reuse no test code — the Chunk D path goes through Backup/
// IncrementalBackup/Restore; this path goes through Streamer.Run
// twice with a kill in between. Both must stay green; the existence
// of one does NOT subsume the other.
//
// Pin discipline (CLAUDE.md / Bug-74 lesson): assert the four
// claims a–d in the docstring of TestStreamer_MySQLToPostgres_
// SchemaHistoryWarmResumeAcrossDDL, do not collapse them to a
// single coarse check. Each maps to a specific ADR-0049 invariant
// (DP-1 boundary detection / DP-2 floor / locked-decision #4a
// same-tx / #4c anchor-at-detection / Chunk C prime).

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_MySQLToPostgres_SchemaHistoryWarmResumeAcrossDDL is
// the ADR-0049 Chunk E headline cross-engine pin. Phases:
//
//  1. Boot MySQL (binlog) source + PG target.
//  2. CREATE TABLE users(id, email) on MySQL; seed R1.
//  3. Start Streamer{MySQL→PG} (call #1) — bulk-copy lands R1.
//  4. INSERT R2 on MySQL → flows via CDC to PG (steady-state CDC
//     proves the stream is healthy before we provoke the DDL).
//  5. ALTER TABLE users ADD COLUMN nickname VARCHAR(64) on MySQL —
//     emits an ir.SchemaSnapshot via Chunk B1 (binlog QUERY-event
//     + clear(schemaCache), anchored at the DDL's own GTID).
//  6. INSERT R3 with a nickname value on MySQL → flows via CDC.
//  7. Wait for R3 to land on PG with nickname populated.
//  8. Cancel ctx → streamer #1 exits cleanly. The persisted
//     position in sluice_cdc_state is past the ALTER and past R3.
//  9. Pre-resume assertion (a): the target's
//     sluice_cdc_schema_history has a row for (stream, public,
//     users) — Chunk B1 wrote it in the same tx as the ADR-0007
//     position write (#4a). Without #4a the position would have
//     been persisted but the schema-history row absent.
//  10. Start streamer #2 with the SAME StreamID, capturing slog
//     output to assert (c). On apply-loop entry, Chunk C's
//     PrimeSchemaHistoryCache primes the applier's active-version
//     cache from the schema-history table.
//  11. Pre-CDC assertion (c-log): the warm-resume INFO log fires
//     ("warm resume from persisted position") AND the cold-start
//     INFO log does NOT ("cold start; snapshot captured") AND the
//     fall-through WARN does NOT ("falling through to cold start").
//     The two log-absence checks are the load-bearing proxies for
//     "no full re-snapshot was triggered" — without ADR-0049, a
//     DDL-induced ErrPositionInvalid would route through the
//     runOnce fall-through into coldStart, both negative markers
//     would fire, AND target row count would jump (the bulk-copy
//     upsert would re-overwrite R1+R2+R3).
//  12. INSERT R4 with another nickname value on MySQL → flows via
//     CDC through the warm-resumed stream.
//  13. Wait for R4 to land on PG with nickname populated. This is
//     assertion (d) (post-resume CDC keeps working with the
//     post-DDL schema) AND assertion (b) (the applier resolved
//     the post-ALTER schema for users — if the prime had handed
//     back the pre-ALTER schema or if the active-version cache
//     were unprimed and ActiveSchema returned (nil, false), the
//     nickname column write would either fail loud (column
//     unknown on the target writer cache) or land with the wrong
//     value).
//  14. Post-resume assertion (c-rows): row count on PG users
//     advances exactly by 1 from the kill-time count (R4 only) —
//     a re-bulk-copy would re-INSERT R1+R2+R3 again. The PG side
//     is UPSERT-on-PK during chain restore but this code path is
//     a live Streamer warm-resume, which goes through the CDC
//     INSERT path and does NOT re-emit R1/R2/R3 from the source
//     snapshot. The count test is therefore a clean proxy for
//     "no re-bulk-copy".
//  15. Cancel streamer #2; verify clean shutdown.
//
// Cross-references:
//   - ADR-0049 §"Implementation checkpoint sign-off" locked
//     decisions #4a (same-tx), #4b (loud fatal), #4c (anchor at
//     detection); DP-1 (binlog boundary = QUERY-event GTID),
//     DP-2 (compaction floor — not exercised here but composes
//     with the loud refuse below it).
//   - Chunk B1 cdc_reader.go maybeSnapshotSchemaB1; pendingDDLAnchor.
//   - Chunk B3 PG applier change_applier.go applyOne ir.SchemaSnapshot
//     case (same-tx write).
//   - Chunk C change_applier_schema_cache.go PrimeSchemaHistoryCache;
//     ActiveSchema (the active-version cache the warm resume primes
//     via the schemaHistoryCachePrimer interface in streamer.go).
//   - Chunk D's backup-side counterpart pin:
//     TestIncrementalBackup_PostgresChainRestore_SchemaHistoryReplay
//     in incremental_pg_integration_test.go (DO NOT collapse — they
//     pin different recovery surfaces; see file docstring).
//
// like a runbook; splitting into helpers obscures the ADR-invariant
// → phase mapping the docstring cross-refs.
//
//nolint:gocognit // Sequential phase-by-phase integration test reads
func TestStreamer_MySQLToPostgres_SchemaHistoryWarmResumeAcrossDDL(t *testing.T) {
	// HISTORICAL NOTE (task #28, closed 2026-05-20): this pin was
	// deferred from v0.70.0 with a t.Skip because the Phase 5 ALTER's
	// separate-DSN connection reliably died around the 50-60s mark
	// under streamer + -race resource pressure on CI, with an
	// `unexpected EOF` from the MySQL driver. Phase A instrumentation
	// (CI run 26173191325) confirmed the smoking gun:
	//   SHOW SESSION VARIABLES LIKE 'net_write_timeout' = 60
	// The mysql:8.0 default server-side socket-write timeout is 60s.
	// While ALTER executes, the *server* sees the conn as idle (no
	// active write); when the timeout fires it closes the conn →
	// client sees EOF. ALGORITHM=INSTANT didn't help because the
	// elapsed time the ALTER actually needs is irrelevant when the
	// server kills the conn for not writing to it. The fix bumps
	// --net-write-timeout to 600 in startMySQLBinlog's container Cmd
	// (one place, all binlog-flavoured pipeline tests benefit). See
	// streamer_resume_mysql_integration_test.go for the bump itself.

	mysqlSourceDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('r1@example.com');
	`
	applyMySQLDDL(t, mysqlSourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "test-cross-mysql-pg-schemahistory"

	// ---- Phase 3: streamer #1 — cold start, bulk-copy lands R1 ----
	streamer1 := &Streamer{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSourceDSN,
		TargetDSN: pgTargetDSN,
		StreamID:  streamID,
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- streamer1.Run(ctx1) }()

	if !waitForRowCount(t, pgTargetDSN, "users", 1, 60*time.Second) {
		t.Fatalf("phase 3: bulk copy never delivered R1 to PG target")
	}

	// ---- Phase 4: INSERT R2 — steady-state CDC works ----
	applyMySQLDDL(t, mysqlSourceDSN,
		"INSERT INTO users (email) VALUES ('r2@example.com');")
	if !waitForRowCount(t, pgTargetDSN, "users", 2, 30*time.Second) {
		t.Fatalf("phase 4: CDC never delivered R2 (steady-state pre-DDL must work)")
	}

	// ---- Phase 5: ALTER TABLE ADD COLUMN nickname ----
	// Emits an ir.SchemaSnapshot via Chunk B1 once the next ROW event
	// for users hits maybeSnapshotSchemaB1 (deferred-emit pattern;
	// the anchor is the ALTER's GTID frozen at clear time per #4c).
	// ALGORITHM=INSTANT is kept (sub-second DDL, no MDL wait) even
	// though MDL was ruled out as the root cause of the prior failure
	// — the bumped container-side --net-write-timeout (see file
	// docstring above) is the actual fix.
	applyMySQLDDL(t, mysqlSourceDSN,
		"ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INSTANT;")

	// ---- Phase 6: INSERT R3 carrying the new column ----
	// The first post-DDL ROW triggers maybeSnapshotSchemaB1 → writes
	// the post-ALTER ir.Table into sluice_cdc_schema_history on the
	// same tx as the ADR-0007 position write (#4a).
	applyMySQLDDL(t, mysqlSourceDSN,
		"INSERT INTO users (email, nickname) VALUES ('r3@example.com', 'Robbie');")

	// ---- Phase 7: wait for R3 + nickname on PG ----
	if !waitForRowCount(t, pgTargetDSN, "users", 3, 30*time.Second) {
		t.Fatalf("phase 7: CDC never delivered R3 (post-DDL INSERT must reach target)")
	}
	if !waitForNicknameByEmail(t, pgTargetDSN, "r3@example.com", "Robbie", 15*time.Second) {
		got := readNicknameByEmail(t, pgTargetDSN, "r3@example.com")
		t.Fatalf("phase 7: r3.nickname on PG = %q; want %q (Chunk B1 post-DDL schema must reach the writer)",
			got, "Robbie")
	}

	// ---- Phase 8: cancel streamer #1; verify clean exit ----
	cancel1()
	select {
	case <-runErr1:
	case <-time.After(15 * time.Second):
		t.Fatal("phase 8: streamer1 did not return after ctx cancel")
	}

	rowCountAtKill := countRows(t, pgTargetDSN, "users")
	if rowCountAtKill != 3 {
		t.Fatalf("phase 8: PG row count at kill-time = %d; want 3 (R1+R2+R3)", rowCountAtKill)
	}

	// ---- Phase 9 — assertion (a): schema-history row exists ----
	// Chunk B1 + B3 + locked decision #4a contract: the
	// SchemaSnapshot for users.ALTER landed in the target's
	// sluice_cdc_schema_history under streamID, schema=public,
	// table=users, in the same tx as the position write.
	persistedToken := readPersistedPositionXE(t, pgTargetDSN, streamID)
	if persistedToken == "" {
		t.Fatal("phase 9: sluice_cdc_state has no persisted position for streamID — warm resume can't run (ADR-0007 atomicity prereq)")
	}
	t.Logf("phase 9: persisted position token = %q", persistedToken)

	historyCount := schemaHistoryCountXE(t, pgTargetDSN, streamID, "public", "users")
	if historyCount == 0 {
		t.Fatal("phase 9 / assertion (a): sluice_cdc_schema_history has 0 rows for " +
			"(streamID=" + streamID + ", schema=public, table=users) — ADR-0049 Chunk B1+B3 #4c " +
			"anchor-at-detection MUST have written the post-ALTER IR table here")
	}
	t.Logf("phase 9: sluice_cdc_schema_history rows for users = %d", historyCount)

	// ---- Phase 10/11 — assertion (b)+(c): warm-resume; cache prime + no cold-start ----
	// captureSlog swaps slog.Default so we can inspect the streamer's
	// log output for the warm/cold start markers. Streamer logs
	// "warm resume from persisted position" (INFO) on warmResume and
	// "cold start; snapshot captured" (INFO) on coldStart — they are
	// mutually exclusive on any given runOnce iteration.
	logBuf := captureSlog(t)

	streamer2 := &Streamer{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSourceDSN,
		TargetDSN: pgTargetDSN,
		StreamID:  streamID,
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- streamer2.Run(ctx2) }()

	// Wait long enough for runOnce to reach the warm-resume log line
	// and for PrimeSchemaHistoryCache to run, but shorter than the
	// re-bulk-copy windows we want to assert against.
	time.Sleep(5 * time.Second)

	logs := logBuf.String()
	if !strings.Contains(logs, "warm resume from persisted position") {
		t.Errorf("phase 11 / assertion (c-log+): warm-resume INFO log not observed in %ds; "+
			"got logs:\n%s", 5, logs)
	}
	if strings.Contains(logs, "cold start; snapshot captured") {
		t.Errorf("phase 11 / assertion (c-log-): cold-start INFO log fired during warm-resume — "+
			"ADR-0049 invariant violated: a DDL must NOT force re-snapshot; logs:\n%s", logs)
	}
	if strings.Contains(logs, "falling through to cold start") {
		t.Errorf("phase 11 / assertion (c-log-): warm-resume fall-through WARN fired — "+
			"persisted position was rejected; ADR-0049 must keep resume valid across DDL; logs:\n%s", logs)
	}

	// Row-count proxy for (c): row count must still be exactly 3
	// (R1+R2+R3). A re-bulk-copy would have re-INSERTed all three
	// (the PG bulk-copy path uses COPY which on a fresh target via
	// the cold-start preflight refuses; but if cold-start ran the
	// preflight, the run would have errored out — which we'd also
	// see as warmResumed=false in the prime path). The single best
	// signal here is the log-absence above; the count is a backup
	// that also catches the case where cold-start somehow ran past
	// preflight.
	if got := countRows(t, pgTargetDSN, "users"); got != 3 {
		t.Errorf("phase 11 / assertion (c-count): row count after warm-resume = %d; want 3 "+
			"(re-bulk-copy or duplicate snapshot would have shifted it)", got)
	}

	// ---- Phase 12/13: INSERT R4 — post-resume CDC + assertion (b)+(d) ----
	applyMySQLDDL(t, mysqlSourceDSN,
		"INSERT INTO users (email, nickname) VALUES ('r4@example.com', 'Riley');")
	if !waitForRowCount(t, pgTargetDSN, "users", 4, 30*time.Second) {
		t.Fatalf("phase 13 / assertion (d): post-resume CDC never delivered R4")
	}
	if !waitForNicknameByEmail(t, pgTargetDSN, "r4@example.com", "Riley", 15*time.Second) {
		got := readNicknameByEmail(t, pgTargetDSN, "r4@example.com")
		t.Fatalf("phase 13 / assertion (b): r4.nickname on PG = %q; want %q "+
			"(applier's active-version cache for users must hold the post-ALTER IR with nickname)",
			got, "Riley")
	}

	// ---- Phase 14: post-resume count is exactly 4 — no re-snapshot ----
	if got := countRows(t, pgTargetDSN, "users"); got != 4 {
		t.Errorf("phase 14 / assertion (c-count): final row count = %d; want 4 "+
			"(R1+R2+R3+R4, with NO replay of R1..R3 from a re-snapshot)", got)
	}

	// ---- Phase 15: clean shutdown ----
	cancel2()
	select {
	case <-runErr2:
	case <-time.After(15 * time.Second):
		t.Fatal("phase 15: streamer2 did not return after ctx cancel")
	}
}

// readPersistedPositionXE is a duplicate-named (XE-suffixed) variant
// of readPersistedPosition that exists only because the existing
// helper in streamer_resume_integration_test.go is fine for the
// resume tests but this Chunk E test file is parallel to the
// streamer_cross_integration_test.go pattern — we keep the helper
// scope-local to make the Chunk E assertions self-contained when
// reading this file in isolation.
func readPersistedPositionXE(t *testing.T, dsn, streamID string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var token string
	err = db.QueryRowContext(
		ctx,
		`SELECT source_position FROM "public"."sluice_cdc_state" WHERE stream_id = $1`,
		streamID,
	).Scan(&token)
	if err != nil {
		return ""
	}
	return token
}

// schemaHistoryCountXE returns the row count in the target's
// sluice_cdc_schema_history table for the given (streamID, schema,
// table) triple. ADR-0049 Chunk A pin: the table lives under the
// applier's controlSchema (public for the default PG applier).
func schemaHistoryCountXE(t *testing.T, dsn, streamID, schemaName, tableName string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int
	err = db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM "public"."sluice_cdc_schema_history"
		 WHERE stream_id = $1 AND schema_name = $2 AND table_name = $3`,
		streamID, schemaName, tableName,
	).Scan(&n)
	if err != nil {
		t.Fatalf("schema-history count: %v", err)
	}
	return n
}

// readNicknameByEmail returns the nickname for a user with the
// given email on the PG target, or "" if absent / NULL. Used to
// assert (b) — the Chunk C active-version cache holds the
// post-ALTER schema, so the nickname column is written into the
// new column on the target.
func readNicknameByEmail(t *testing.T, dsn, email string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var nickname sql.NullString
	err = db.QueryRowContext(
		ctx,
		`SELECT nickname FROM users WHERE email = $1`, email,
	).Scan(&nickname)
	if err != nil {
		return ""
	}
	if !nickname.Valid {
		return ""
	}
	return nickname.String
}

// waitForNicknameByEmail polls until the nickname for (email)
// equals want or the timeout fires. CDC is async — the post-DDL
// row arrives a moment after the source INSERT.
func waitForNicknameByEmail(t *testing.T, dsn, email, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if readNicknameByEmail(t, dsn, email) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// Compile-time guard: the load-bearing ADR-0049 types referenced
// in the docstring above must remain in the public/package
// surface. If any is renamed without updating this pin, this guard
// catches the rename at build time. Cheap insurance against
// ADR-drift; pin-the-class per CLAUDE.md.
var (
	_ ir.Change = ir.SchemaSnapshot{}
	_ error     = ir.ErrPositionInvalid
)

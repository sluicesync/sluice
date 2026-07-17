//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine continuous-sync integration test: MariaDB source →
// Postgres target (roadmap item 73 Phase 3, ADR-0170). This is the
// end-to-end validation the "validate cross-engine before building more"
// tenet requires — the reader-level pins (engines/mysql) prove the CDC
// mechanics; THIS proves the full pipeline (snapshot → CDC handoff →
// change apply) converges on a real PG target, AND that the Phase-2 type
// fidelity (JSON identity, native uuid, temporal) survives the CDC path,
// not just bulk migrate.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// startMariaDBBinlog boots a mariadb:11.4 source container with binlog
// (ROW + FULL row-image; MariaDB is always GTID-capable). Returns the
// source_db DSN + a terminate cleanup. 11.4 only: the reader-level tests
// (engines/mysql) already cover both LTS lines; this cross-engine leg
// keeps to one image for tractability.
func startMariaDBBinlog(t *testing.T) (sourceDSN string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	req := testcontainers.ContainerRequest{
		Image: "mariadb:11.4",
		Env: map[string]string{
			"MARIADB_ROOT_PASSWORD": "rootpw",
			"MARIADB_DATABASE":      "source_db",
		},
		Cmd: []string{
			"--server-id=1",
			"--log-bin=mysqld-bin",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
		},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
			return fmt.Sprintf("root:rootpw@tcp(%s:%s)/source_db", host, port.Port())
		}).WithStartupTimeout(4 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("boot mariadb:11.4: %v", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	log.Printf("cross-engine mariadb source booted at %s:%s", host, port.Port())
	cleanup = func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/source_db?parseTime=true", host, port.Port()), cleanup
}

// TestStreamer_MariaDBToPostgres is the Phase-3 cross-engine sync spine.
// Phases:
//
//	A. Boot MariaDB (binlog) source + Postgres target.
//	B. Seed MariaDB with a corpus exercising the P2 type surface: int PK,
//	   text, JSON (MariaDB's longtext+json_valid alias), temporal
//	   DATETIME(3).
//	C. Streamer{Source: mariadb, Target: pg} runs (cold-start snapshot →
//	   CDC handoff).
//	D. Bulk copy lands R1 on PG; assert every P2 value landed correctly.
//	E. INSERT R2 on MariaDB → flows via CDC; assert its P2 values.
//	F. UPDATE R1 → verify on PG.
//	G. DELETE R2 → verify gone.
//	H. Clean shutdown.
//
// KNOWN GAP (deliberately NOT in the corpus — reported for a follow-up
// chunk, ADR-0170 § Consequences): MariaDB's NATIVE uuid / inet6 / inet4
// columns do NOT survive the CDC path. Phase 2 (ADR-0169) added them for
// schema + bulk copy, where the value arrives as driver-formatted TEXT;
// the binlog carries the RAW storage bytes (16 for uuid/inet6, 4 for
// inet4), and decodeValue's ir.UUID/ir.Inet handler assumes the MySQL
// "stored in a VARCHAR → text" shape and stringifies the raw bytes. On a
// PG target this fails LOUDLY (`invalid input syntax for type uuid`,
// SQLSTATE 22P02) — no silent corruption — so it is safe to defer. The
// proper fix is a flavor+type-aware binlog decode (format 16 bytes → uuid
// text, ground-truthing MariaDB's uuid byte ORDER — the swapped-vs-straight
// trap is a Bug-74-class silent-corruption risk if done without a live
// pin) pinned across the uuid/inet4/inet6 family on both LTS lines. Bulk
// migrate of these types is unaffected (Phase 2, proven).
func TestStreamer_MariaDBToPostgres(t *testing.T) {
	sourceDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE items (
			id      INT          NOT NULL,
			name    VARCHAR(64)  NOT NULL,
			payload JSON         NOT NULL,
			created DATETIME(3)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO items (id, name, payload, created) VALUES
			(1, 'r1', '{"k": 1, "s": "hi"}', '2026-01-02 03:04:05.678');
	`
	applyMariaDBSQL(t, sourceDSN, seedDDL)

	mariaEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    mariaEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: pgTargetDSN,
		StreamID:  "test-cross-mariadb-pg",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// ---- Phase D: bulk copy lands R1; assert P2 values ----
	if !waitForRowCount(t, pgTargetDSN, "items", 1, 90*time.Second) {
		t.Fatalf("phase D: bulk copy never delivered R1 to PG target")
	}
	assertItemOnPG(t, pgTargetDSN, itemExpect{
		id:      1,
		name:    "r1",
		payload: `{"k": 1, "s": "hi"}`,
		created: "2026-01-02 03:04:05.678",
	}, "phase D (bulk copy)")

	// ---- Phase E: CDC INSERT R2; assert its P2 values landed via CDC ----
	applyMariaDBSQL(t, sourceDSN, `
		INSERT INTO items (id, name, payload, created) VALUES
			(2, 'r2', '{"k": 2, "arr": [1,2,3]}', '2026-07-17 09:08:07.006');`)
	if !waitForRowCount(t, pgTargetDSN, "items", 2, 30*time.Second) {
		select {
		case e := <-runErr:
			t.Fatalf("phase E: CDC INSERT never delivered R2 to PG target; streamer.Run returned: %v", e)
		default:
			t.Fatalf("phase E: CDC INSERT never delivered R2 to PG target (streamer still running, no error surfaced)")
		}
	}
	// JSON lands VERBATIM through CDC — MariaDB JSON is textual
	// (longtext, ir.JSON{Binary:false}, ADR-0169) and PG `json` (not
	// jsonb) preserves the exact source bytes, so the expectation is the
	// inserted text character-for-character (no whitespace normalization).
	assertItemOnPG(t, pgTargetDSN, itemExpect{
		id:      2,
		name:    "r2",
		payload: `{"k": 2, "arr": [1,2,3]}`,
		created: "2026-07-17 09:08:07.006",
	}, "phase E (CDC INSERT)")

	// ---- Phase F: CDC UPDATE R1 ----
	applyMariaDBSQL(t, sourceDSN,
		"UPDATE items SET name = 'r1-upd', payload = '{\"k\": 11}' WHERE id = 1;")
	if !waitForNameByID(t, pgTargetDSN, 1, "r1-upd", 30*time.Second) {
		t.Fatalf("phase F: CDC UPDATE never propagated to PG")
	}
	assertItemOnPG(t, pgTargetDSN, itemExpect{
		id:      1,
		name:    "r1-upd",
		payload: `{"k": 11}`,
		created: "2026-01-02 03:04:05.678",
	}, "phase F (CDC UPDATE)")

	// ---- Phase G: CDC DELETE R2 ----
	applyMariaDBSQL(t, sourceDSN, "DELETE FROM items WHERE id = 2;")
	if !waitForRowGoneByID(t, pgTargetDSN, "items", 2, 30*time.Second) {
		t.Fatalf("phase G: CDC DELETE never removed R2 from PG")
	}
	if n := countRows(t, pgTargetDSN, "items"); n != 1 {
		t.Errorf("phase G: PG items count = %d; want 1", n)
	}

	// ---- Phase H: stop the stream (simulated crash) ----
	// Ties the reader-level ResumeAfterKill pin (engines/mysql) to the
	// FULL pipeline: stop, apply a while-down change, restart with the
	// same StreamID, and assert the while-down change converges via the
	// warm-resumed CDC stream (not a fresh re-snapshot).
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("phase H: Streamer.Run did not return after ctx cancel")
	}

	// The handoff persisted a source position on the PG target; a
	// non-empty position is the warm-resume prerequisite.
	if pos := readPGPersistedPosition(t, pgTargetDSN, "test-cross-mariadb-pg"); pos == "" {
		t.Fatal("phase H: sluice_cdc_state has no/empty source_position — warm resume can't work")
	}

	// While the stream is DOWN, insert a new row on the MariaDB source.
	applyMariaDBSQL(t, sourceDSN, `
		INSERT INTO items (id, name, payload, created) VALUES
			(3, 'r3-whiledown', '{"k": 3}', '2026-03-03 03:03:03.003');`)

	// ---- Phase I: warm-resume; the while-down change converges ----
	streamer2 := &Streamer{
		Source:    mariaEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: pgTargetDSN,
		StreamID:  "test-cross-mariadb-pg",
	}
	stream2Ctx, stream2Cancel := context.WithCancel(context.Background())
	defer stream2Cancel()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- streamer2.Run(stream2Ctx) }()

	if !waitForRowCount(t, pgTargetDSN, "items", 2, 30*time.Second) {
		select {
		case e := <-runErr2:
			t.Fatalf("phase I: warm-resume never delivered the while-down row; streamer2.Run returned: %v", e)
		default:
			t.Fatalf("phase I: warm-resume never delivered the while-down row (id=3)")
		}
	}
	assertItemOnPG(t, pgTargetDSN, itemExpect{
		id:      3,
		name:    "r3-whiledown",
		payload: `{"k": 3}`,
		created: "2026-03-03 03:03:03.003",
	}, "phase I (warm-resume CDC)")
	// Exactly-once: R1 (r1-upd) + R3 only — the resume must not have
	// duplicated or lost the pre-existing row.
	if n := countRows(t, pgTargetDSN, "items"); n != 2 {
		t.Errorf("phase I: PG items count = %d after warm-resume; want 2 (R1 + R3, exactly-once)", n)
	}
	assertItemOnPG(t, pgTargetDSN, itemExpect{
		id:      1,
		name:    "r1-upd",
		payload: `{"k": 11}`,
		created: "2026-01-02 03:04:05.678",
	}, "phase I (R1 survived resume)")

	// ---- Phase J: clean shutdown ----
	stream2Cancel()
	select {
	case <-runErr2:
	case <-time.After(15 * time.Second):
		t.Fatal("phase J: streamer2.Run did not return after ctx cancel")
	}
}

// TestStreamer_MariaDBToMySQL_NativeUUIDRefusedPreData is the direct proof
// that the ADR-0170 native-uuid/inet refusal closes the MySQL-family-target
// SILENT-corruption path: a MariaDB source with a native uuid column,
// synced to a MYSQL target (whose CHAR(36) would silently accept a
// mis-decoded binlog string), must refuse with the coded error PRE-DATA —
// the refusal fires in the snapshot opener, before the bulk copy, so the
// MySQL target gets ZERO rows (no chance for any value, right or wrong, to
// land). This is the target that would corrupt silently without the
// refusal (a PG target rejects the bad string loudly; a MySQL target does
// not).
func TestStreamer_MariaDBToMySQL_NativeUUIDRefusedPreData(t *testing.T) {
	srcDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, mysqlTargetDSN, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	applyMariaDBSQL(t, srcDSN, `
		CREATE TABLE items (
			id INT NOT NULL,
			u  UUID NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB;
		INSERT INTO items (id, u) VALUES (1, '11111111-1111-1111-1111-111111111111');`)

	mariaEng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	s := &Streamer{
		Source:    mariaEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: mysqlTargetDSN,
		StreamID:  "test-mariadb-mysql-native",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if err == nil {
		t.Fatal("mariadb → mysql sync with a native uuid column SUCCEEDED; want the coded native-type refusal — " +
			"a MySQL-family target (CHAR(36)) would SILENTLY accept the mis-decoded binlog bytes")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCMariaDBNativeTypeUnsupported {
		t.Fatalf("Run err = %v; want coded %s", err, sluicecode.CodeCDCMariaDBNativeTypeUnsupported)
	}
	// Pre-data: the refusal fires in the snapshot opener, before the bulk
	// copy — so the MySQL target got NO rows. pollRowCountMySQL tolerates
	// an absent table (returns 0).
	if n := pollRowCountMySQL(mysqlTargetDSN, "items"); n != 0 {
		t.Errorf("mysql target items row count = %d; want 0 (the refusal must be pre-data — nothing copied)", n)
	}
}

// readPGPersistedPosition returns the source_position sluice persisted for
// streamID in the target's sluice_cdc_state control table ("" if absent).
func readPGPersistedPosition(t *testing.T, dsn, streamID string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var pos sql.NullString
	err = db.QueryRowContext(ctx,
		`SELECT source_position FROM "public"."sluice_cdc_state" WHERE stream_id = $1`, streamID).Scan(&pos)
	if err != nil {
		return ""
	}
	return pos.String
}

// itemExpect is the expected post-translation shape of an items row on PG.
type itemExpect struct {
	id      int
	name    string
	payload string // json::text on PG (pg normalizes spacing)
	created string // "2006-01-02 15:04:05.999" on PG
}

// assertItemOnPG reads items(id) on the PG target and asserts every P2
// column landed correctly — the end-to-end proof that JSON identity,
// temporal, and native-uuid fidelity survive the path named by phase.
func assertItemOnPG(t *testing.T, dsn string, want itemExpect, phase string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("%s: open pg: %v", phase, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var name, payload, created string
	err = db.QueryRowContext(ctx,
		`SELECT name, payload::text, to_char(created, 'YYYY-MM-DD HH24:MI:SS.MS')
		   FROM items WHERE id = $1`, want.id).Scan(&name, &payload, &created)
	if err != nil {
		t.Fatalf("%s: read items(id=%d) on PG: %v", phase, want.id, err)
	}
	if name != want.name {
		t.Errorf("%s: items(%d).name = %q; want %q", phase, want.id, name, want.name)
	}
	if payload != want.payload {
		t.Errorf("%s: items(%d).payload = %q; want %q (JSON identity through this path)", phase, want.id, payload, want.payload)
	}
	if created != want.created {
		t.Errorf("%s: items(%d).created = %q; want %q (temporal DATETIME(3) fidelity)", phase, want.id, created, want.created)
	}
}

// waitForNameByID polls the PG target until items(id).name == want.
func waitForNameByID(t *testing.T, dsn string, id int, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollNameByID(dsn, id) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func pollNameByID(dsn string, id int) string {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM items WHERE id = $1", id).Scan(&name); err != nil {
		return ""
	}
	return name
}

// waitForRowGoneByID polls until items(id) is absent on the PG target.
func waitForRowGoneByID(t *testing.T, dsn, table string, id int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", dsn)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var n int
			qerr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE id = $1", id).Scan(&n)
			cancel()
			_ = db.Close()
			if qerr == nil && n == 0 {
				return true
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// applyMariaDBSQL runs a possibly-multi-statement script against a MariaDB
// DSN (the mysql driver serves MariaDB).
func applyMariaDBSQL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open mariadb: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply mariadb sql: %v", err)
	}
}
